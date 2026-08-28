#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Carry events over oslo.messaging, on the bus the service already has.

The AMQP transport talks to a broker directly.  This one goes through
the abstraction an OpenStack service is already configured for, which
buys three things a raw connection does not: the operator's existing
``[oslo_messaging_notifications]`` and ``transport_url`` settings apply
unchanged, whichever driver they chose is the one used, and events
land on the notification bus beside everything else the deployment
already collects.

**Notifications, not RPC.**  RPC is a call with a reply and a server
expected to be listening; a flow reporting its progress wants neither.
Notifications are fire-and-forget fanout with durable queues, which is
exactly the shape of this traffic, and they cost the flow nothing when
no collector is running.

Delivery uses one notification per batch, carrying the same envelope
the AMQP transport sends, so a collector is parsing the same thing
either way.

**One thing this gives up.**  The AMQP transport declares its durable
queue on every publish, so a flow that runs before the collector ever
has is not a lost run.  A notifier cannot do that: the queue belongs to
the listener, and a broker discards what it has nothing to route to.
So the first collector must be started before the first flow, once;
after that the queue is durable and restarts cost nothing.  If flows
genuinely run before any collector exists, use the AMQP transport.

### Why the subscriber parks a delivery thread

oslo.messaging pushes: it hands each notification to an endpoint on its
own thread and takes that endpoint's return value as the decision to
acknowledge or requeue.  :class:`~taskflow_meter.transports.base.Subscriber`
pulls: the caller owns the loop, so it can stop between batches instead
of being trapped inside one.

Bridging the two by buffering and acknowledging on arrival would mean
a handler that fails has already lost the batch -- the one outcome this
package treats as worse than redelivering.  So the endpoint hands the
batch over and *waits*: the consumer runs the handler on the caller's
thread, reports back, and the endpoint turns that into an ack or a
requeue.  A delivery nobody claims within ``park`` seconds goes back to
the broker rather than pinning a thread forever.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable
from collections.abc import Sequence
from typing import Any

import oslo_messaging
from oslo_config import cfg

from taskflow_meter.events import Event
from taskflow_meter.transports.base import Publisher
from taskflow_meter.transports.base import Subscriber
from taskflow_meter.transports.base import from_payload
from taskflow_meter.transports.base import to_payload

LOG = logging.getLogger(__name__)

#: Notification topic the events are published on.
DEFAULT_TOPIC = "taskflow-meter"

#: ``event_type`` every notification carries, so a listener that also
#: hears other traffic can pick these out.
DEFAULT_EVENT_TYPE = "taskflow_meter.events"

DEFAULT_PUBLISHER_ID = "taskflow-meter"

#: Listener pool.  Named, so several collectors share one queue and
#: each notification is handled once rather than once per collector.
DEFAULT_POOL = "taskflow-meter"

#: The notification driver.  ``messagingv2`` is the one that puts a
#: readable payload on the bus; ``noop`` silently discards.
DEFAULT_DRIVER = "messagingv2"

#: oslo.messaging's two verdicts, which are plain strings.  Bound here
#: because oslo.messaging ships no type information, so reading them off
#: the module at each use site gives back ``Any``.
HANDLED: str = oslo_messaging.NotificationResult.HANDLED
REQUEUE: str = oslo_messaging.NotificationResult.REQUEUE

DEFAULT_TIMEOUT = 1.0

#: How long a delivery waits to be claimed before going back to the
#: broker.  Long enough to cover a slow batch, short enough that a
#: collector which stopped consuming does not strand the bus.
DEFAULT_PARK = 30.0


class _Delivery:
    """One batch, handed from a delivery thread to a consumer.

    Settled exactly once, by whichever side gets there first: the
    consumer claiming it, or the delivery thread giving up on it.
    """

    __slots__ = ("_lock", "_settled", "done", "events", "handled")

    def __init__(self, events: list[Event]) -> None:
        self.events = events
        self.done = threading.Event()
        self.handled = False
        self._lock = threading.Lock()
        self._settled = False

    def settle(self) -> bool:
        """Take ownership.  True for the first caller only."""
        with self._lock:
            if self._settled:
                return False
            self._settled = True
            return True

    def finish(self, *, handled: bool) -> None:
        self.handled = handled
        self.done.set()


class _Endpoint:
    """What oslo.messaging calls when a notification arrives."""

    def __init__(self, pending: queue.Queue[_Delivery], park: float) -> None:
        self._pending = pending
        self._park = park

    def info(
        self,
        _ctxt: Any,
        _publisher_id: str,
        _event_type: str,
        payload: Any,
        _metadata: Any,
    ) -> str:
        """Handle one notification.

        Five positional arguments, because that is how oslo.messaging
        calls an endpoint; only the payload is ours to care about. The
        return value is a ``NotificationResult``, which is a plain
        string.
        """
        try:
            events = from_payload(payload)
        except Exception:
            # A notification we cannot read will never become readable,
            # so requeueing it would only replay the failure forever.
            LOG.exception("discarding an unreadable notification")
            return HANDLED

        delivery = _Delivery(events)
        self._pending.put(delivery)

        if not delivery.done.wait(self._park):
            if delivery.settle():
                # Nobody was consuming.  Back to the broker, where it
                # waits for a collector rather than for this thread.
                LOG.debug("no consumer claimed a batch; requeueing")
                return REQUEUE
            # Claimed just as we gave up: it is being handled, so the
            # only correct thing left is to wait for the verdict.
            delivery.done.wait()

        if delivery.handled:
            return HANDLED
        return REQUEUE


class OsloMessagingTransport(Publisher):
    """Publishes batches as notifications."""

    name = "oslo_messaging"

    def __init__(
        self,
        url: str | None = None,
        *,
        topic: str = DEFAULT_TOPIC,
        event_type: str = DEFAULT_EVENT_TYPE,
        publisher_id: str = DEFAULT_PUBLISHER_ID,
        driver: str | None = DEFAULT_DRIVER,
        conf: cfg.ConfigOpts | None = None,
        transport: Any = None,
    ) -> None:
        """Configure a publisher.

        With no *url* the service's own configuration decides, which is
        the point of going through oslo.messaging at all: a deployment
        that already set ``transport_url`` does not restate it here.
        """
        self.url = url
        self.topic = topic
        self.event_type = event_type
        self.publisher_id = publisher_id
        self.driver = driver
        self._conf = conf if conf is not None else cfg.CONF
        self._transport = transport
        self._owns_transport = transport is None
        self._notifier: Any = None

    @property
    def notifier(self) -> Any:
        if self._notifier is None:
            self._notifier = oslo_messaging.Notifier(
                self._get_transport(),
                driver=self.driver,
                publisher_id=self.publisher_id,
                topics=[self.topic],
            )
        return self._notifier

    def _get_transport(self) -> Any:
        if self._transport is None:
            self._transport = oslo_messaging.get_notification_transport(
                self._conf, url=self.url
            )
        return self._transport

    def start(self) -> None:
        self.notifier  # noqa: B018 - build it before the first batch

    def stop(self) -> None:
        self._notifier = None
        if self._owns_transport and self._transport is not None:
            self._transport.cleanup()
            self._transport = None

    def publish(self, events: Sequence[Event]) -> None:
        # An empty context: these are not on behalf of a request, and
        # a monitoring payload is no place to copy one into.
        self.notifier.info({}, self.event_type, to_payload(events))


class OsloMessagingSubscriber(Subscriber):
    """Consumes those notifications."""

    name = "oslo_messaging"

    def __init__(
        self,
        url: str | None = None,
        *,
        topic: str = DEFAULT_TOPIC,
        pool: str | None = DEFAULT_POOL,
        executor: str = "threading",
        park: float = DEFAULT_PARK,
        conf: cfg.ConfigOpts | None = None,
        transport: Any = None,
    ) -> None:
        self.url = url
        self.topic = topic
        self.pool = pool
        self.executor = executor
        self.park = park
        self._conf = conf if conf is not None else cfg.CONF
        self._transport = transport
        self._owns_transport = transport is None
        self._listener: Any = None
        self._pending: queue.Queue[_Delivery] = queue.Queue()

    def _get_transport(self) -> Any:
        if self._transport is None:
            self._transport = oslo_messaging.get_notification_transport(
                self._conf, url=self.url
            )
        return self._transport

    def start(self) -> None:
        if self._listener is not None:
            return
        listener = oslo_messaging.get_notification_listener(
            self._get_transport(),
            [oslo_messaging.Target(topic=self.topic)],
            [_Endpoint(self._pending, self.park)],
            executor=self.executor,
            # Without this the driver has no way to hand a batch back,
            # and a handler that failed would lose it.
            allow_requeue=True,
            pool=self.pool,
        )
        listener.start()
        self._listener = listener

    def stop(self) -> None:
        listener = self._listener
        if listener is None:
            return
        self._listener = None
        # Release anything parked first, or the delivery threads sit on
        # `park` seconds each and stop() waits for every one of them.
        self._release_pending()
        listener.stop()
        listener.wait()
        if self._owns_transport and self._transport is not None:
            self._transport.cleanup()
            self._transport = None

    def _release_pending(self) -> None:
        while True:
            try:
                delivery = self._pending.get_nowait()
            except queue.Empty:
                return
            if delivery.settle():
                # Unhandled, so the delivery thread requeues it.
                delivery.finish(handled=False)

    def consume(
        self,
        handler: Callable[[Sequence[Event]], None],
        *,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> int:
        """Deliver what has arrived, and return how many events."""
        self.start()
        received = 0
        while True:
            try:
                delivery = self._pending.get(timeout=timeout)
            except queue.Empty:
                break
            if not delivery.settle():
                # It went back to the broker while we were idle; it
                # will be delivered again rather than lost.
                continue
            try:
                handler(delivery.events)
            except Exception:
                # Same bargain as the AMQP transport: leave it on the
                # bus and stop draining, because a requeued batch comes
                # straight back and carrying on would spin on it while
                # the handler cannot recover.
                LOG.exception("handler failed; leaving the notification")
                delivery.finish(handled=False)
                break
            delivery.finish(handled=True)
            received += len(delivery.events)
        return received

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

"""Carry events over a message broker, using kombu.

kombu rather than a raw AMQP client because taskflow already depends on
it for its worker-based engine, so a deployment running those flows has
it already.

This is the transport for the collector deployment: the processes
running flows publish, one collector consumes and writes to a shared
datasource, and the API workers read that with ``poll = false``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from collections.abc import Sequence
from typing import Any

import kombu

from taskflow_meter.events import Event
from taskflow_meter.transports.base import Publisher
from taskflow_meter.transports.base import Subscriber
from taskflow_meter.transports.base import from_payload
from taskflow_meter.transports.base import to_payload

LOG = logging.getLogger(__name__)

DEFAULT_EXCHANGE = "taskflow-meter"
DEFAULT_QUEUE = "taskflow-meter-events"
DEFAULT_ROUTING_KEY = "events"
DEFAULT_TIMEOUT = 1.0


def build_exchange(name: str = DEFAULT_EXCHANGE) -> kombu.Exchange:
    return kombu.Exchange(name, type="direct", durable=True)


def build_queue(
    name: str = DEFAULT_QUEUE,
    *,
    exchange: str = DEFAULT_EXCHANGE,
    routing_key: str = DEFAULT_ROUTING_KEY,
) -> kombu.Queue:
    """Durable, so events survive a collector restart.

    A monitoring queue that discards what it could not deliver is a
    monitoring queue that lies about the gap.
    """
    return kombu.Queue(
        name,
        exchange=build_exchange(exchange),
        routing_key=routing_key,
        durable=True,
    )


class AMQPTransport(Publisher):
    """Publishes batches to an exchange."""

    name = "amqp"

    def __init__(
        self,
        url: str,
        *,
        exchange: str = DEFAULT_EXCHANGE,
        queue: str = DEFAULT_QUEUE,
        routing_key: str = DEFAULT_ROUTING_KEY,
        connection: kombu.Connection | None = None,
    ) -> None:
        self.url = url
        self.routing_key = routing_key
        self._exchange = build_exchange(exchange)
        self._queue = build_queue(
            queue, exchange=exchange, routing_key=routing_key
        )
        self._connection = connection
        self._owns_connection = connection is None

    @property
    def connection(self) -> kombu.Connection:
        if self._connection is None:
            self._connection = kombu.Connection(self.url)
        return self._connection

    def stop(self) -> None:
        if self._owns_connection and self._connection is not None:
            self._connection.release()
            self._connection = None

    def publish(self, events: Sequence[Event]) -> None:
        payload = to_payload(events)
        producer = self.connection.Producer(serializer="json")
        producer.publish(
            payload,
            exchange=self._exchange,
            routing_key=self.routing_key,
            # Declared on every publish: the exchange and queue may not
            # exist yet if a flow starts before the collector does, and
            # events published into nothing are simply lost.
            declare=[self._queue],
            retry=True,
        )


class AMQPSubscriber(Subscriber):
    """Consumes batches from a queue."""

    name = "amqp"

    def __init__(
        self,
        url: str,
        *,
        exchange: str = DEFAULT_EXCHANGE,
        queue: str = DEFAULT_QUEUE,
        routing_key: str = DEFAULT_ROUTING_KEY,
        connection: kombu.Connection | None = None,
    ) -> None:
        self.url = url
        self._queue = build_queue(
            queue, exchange=exchange, routing_key=routing_key
        )
        self._connection = connection
        self._owns_connection = connection is None

    @property
    def connection(self) -> kombu.Connection:
        if self._connection is None:
            self._connection = kombu.Connection(self.url)
        return self._connection

    def stop(self) -> None:
        if self._owns_connection and self._connection is not None:
            self._connection.release()
            self._connection = None

    def consume(
        self,
        handler: Callable[[Sequence[Event]], None],
        *,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> int:
        """Deliver what has arrived, and return how many events."""
        received = 0
        give_up = False

        def on_message(body: Any, message: Any) -> None:
            nonlocal received, give_up
            try:
                events = _decode(body)
            except Exception:
                # A message we cannot read will never become readable.
                # Rejecting it beats redelivering it forever.
                LOG.exception("discarding an unreadable message")
                message.reject()
                return
            try:
                handler(events)
            except Exception:
                # Leave it on the queue: the handler may recover, and
                # dropping monitoring data silently is the one outcome
                # worse than redelivering it.  Then stop draining --
                # a requeued message is redelivered immediately, so
                # carrying on would spin on it until the handler
                # recovered, which it cannot while we never return.
                LOG.exception("handler failed; leaving the message")
                message.requeue()
                give_up = True
                return
            received += len(events)
            message.ack()

        with self.connection.Consumer(
            [self._queue], callbacks=[on_message], accept=["json"]
        ):
            while not give_up:
                try:
                    self.connection.drain_events(timeout=timeout)
                except TimeoutError:
                    break
                except OSError:  # pragma: no cover - broker went away
                    LOG.exception("lost the broker connection")
                    break
        return received


def _decode(body: Any) -> list[Event]:
    if isinstance(body, str):
        import json

        body = json.loads(body)
    return from_payload(body)

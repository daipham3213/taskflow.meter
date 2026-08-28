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

"""Carrying events over oslo.messaging notifications.

Driven against the ``fake://`` driver, which is a real in-process
implementation of the notification bus rather than a mock: the
notifier, the listener, its executor threads and the requeue path all
run for real.  Only the wire is imaginary.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from collections.abc import Sequence

import oslo_messaging
import pytest
from oslo_config import cfg

from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.transports.oslo_messaging import OsloMessagingSubscriber
from taskflow_meter.transports.oslo_messaging import OsloMessagingTransport

URL = "fake://"


@pytest.fixture
def conf() -> cfg.ConfigOpts:
    return cfg.ConfigOpts()


@pytest.fixture
def topic(request: pytest.FixtureRequest) -> str:
    """A topic of this test's own.

    The fake driver keeps its exchanges for the whole process, so tests
    sharing a topic inherit each other's leftovers.
    """
    return f"tfm-test-{request.node.name}"[:180]


@pytest.fixture
def publisher(
    conf: cfg.ConfigOpts, topic: str
) -> Iterator[OsloMessagingTransport]:
    transport = OsloMessagingTransport(URL, topic=topic, conf=conf)
    with transport:
        yield transport


@pytest.fixture
def subscriber(
    conf: cfg.ConfigOpts, topic: str
) -> Iterator[OsloMessagingSubscriber]:
    # A pool of this test's own too, for the same reason.
    sub = OsloMessagingSubscriber(
        URL, topic=topic, pool=f"pool-{topic}", conf=conf, park=2.0
    )
    with sub:
        yield sub


def make_events(count: int = 3, *, run_id: str = "run-1") -> list[Event]:
    return [
        Event(
            run_id=run_id,
            seq=seq,
            ts=float(seq),
            kind=EventKind.ATOM_PROGRESS,
            atom_name="a",
            progress=seq / 10,
            details={"progress_details": {"at_progress": seq / 10}},
        )
        for seq in range(1, count + 1)
    ]


def drain(
    subscriber: OsloMessagingSubscriber, *, timeout: float = 2.0
) -> list[Event]:
    received: list[Event] = []

    def handler(events: Sequence[Event]) -> None:
        received.extend(events)

    subscriber.consume(handler, timeout=timeout)
    return received


def test_a_batch_round_trips_exactly(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    sent = make_events()
    publisher.publish(sent)
    assert drain(subscriber) == sent


def test_consume_reports_how_many_it_delivered(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    publisher.publish(make_events(4))
    assert subscriber.consume(lambda _events: None, timeout=2.0) == 4


def test_an_empty_topic_returns_promptly(
    subscriber: OsloMessagingSubscriber,
) -> None:
    assert subscriber.consume(lambda _events: None, timeout=0.1) == 0


def test_publishing_with_nobody_listening_does_not_fail(
    conf: cfg.ConfigOpts, topic: str
) -> None:
    """The flow must not notice that no collector is running.

    Note what this does *not* claim. Unlike the AMQP transport, which
    declares its durable queue on every publish, a notifier cannot
    create the collector's queue -- the listener does. So events
    published before any collector has ever run have nothing bound to
    catch them and are dropped by the broker. See the module docstring;
    this only pins that the publisher itself stays quiet about it.
    """
    publisher = OsloMessagingTransport(URL, topic=topic, conf=conf)
    with publisher:
        publisher.publish(make_events(2))


def test_several_batches_are_all_delivered(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    publisher.publish(make_events(2))
    publisher.publish(make_events(3))
    assert len(drain(subscriber)) == 5


def test_the_handler_sees_whole_batches(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    """One notification per batch, not per event."""
    batches: list[int] = []
    publisher.publish(make_events(2))
    publisher.publish(make_events(3))
    subscriber.consume(lambda events: batches.append(len(events)), timeout=2.0)
    assert sorted(batches) == [2, 3]


def test_a_handler_that_fails_leaves_the_notification(
    publisher: OsloMessagingTransport,
    subscriber: OsloMessagingSubscriber,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Dropping monitoring data silently is the worst outcome.

    A handler that raises must not acknowledge: the batch goes back on
    the bus and the next consumer gets it.
    """
    publisher.publish(make_events(2))

    logger = "taskflow_meter.transports.oslo_messaging"
    with caplog.at_level(logging.ERROR, logger=logger):
        delivered = subscriber.consume(_explode, timeout=2.0)

    assert delivered == 0
    assert "handler failed" in caplog.text

    # Still there for somebody who can cope with it.
    assert len(drain(subscriber)) == 2


def _explode(_events: Sequence[Event]) -> None:
    msg = "the store is down"
    raise RuntimeError(msg)


def test_a_failing_handler_does_not_spin(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    """A requeued notification comes straight back.

    Carrying on through the batch would busy-loop on it for as long as
    the handler stayed broken, so consume stops instead.
    """
    attempts = 0

    def counting(_events: Sequence[Event]) -> None:
        nonlocal attempts
        attempts += 1
        msg = "still down"
        raise RuntimeError(msg)

    publisher.publish(make_events(1))
    subscriber.consume(counting, timeout=1.0)
    assert attempts == 1


def test_an_unreadable_notification_is_discarded_not_replayed_forever(
    conf: cfg.ConfigOpts,
    topic: str,
    subscriber: OsloMessagingSubscriber,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It will never become readable, so requeueing only loops."""
    raw = OsloMessagingTransport(URL, topic=topic, conf=conf)
    raw.notifier.info({}, "taskflow_meter.events", {"not": "an envelope"})

    logger = "taskflow_meter.transports.oslo_messaging"
    with caplog.at_level(logging.ERROR, logger=logger):
        assert subscriber.consume(lambda _events: None, timeout=1.0) == 0
    assert "unreadable" in caplog.text


def test_the_handler_runs_on_the_calling_thread(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    """The point of the parking handshake.

    oslo.messaging would otherwise call the handler on one of its own
    executor threads, before the caller had any say in whether the
    batch was safely stored.
    """
    seen: list[int] = []
    publisher.publish(make_events(1))
    subscriber.consume(
        lambda _events: seen.append(threading.get_ident()), timeout=2.0
    )
    assert seen == [threading.get_ident()]


def test_a_batch_nobody_claims_goes_back_to_the_bus(
    conf: cfg.ConfigOpts, topic: str
) -> None:
    """A parked delivery must not pin a thread forever.

    With nobody consuming, the delivery thread gives up after ``park``
    and requeues, so the batch waits on the broker instead.
    """
    park = 0.2
    publisher = OsloMessagingTransport(URL, topic=topic, conf=conf)
    sub = OsloMessagingSubscriber(
        URL, topic=topic, pool=f"pool-{topic}", conf=conf, park=park
    )
    with publisher, sub:
        publisher.publish(make_events(2))
        # Consume nothing for well past `park`, so the delivery thread
        # is certain to have given up and handed the batch back.
        # Asserting on a consume() racing the delivery would pass or
        # fail on scheduling, and both outcomes would be correct.
        time.sleep(park * 4)
        assert len(drain(sub, timeout=2.0)) == 2


def test_details_survive_the_wire(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    sent = [
        Event(
            run_id="run-1",
            seq=1,
            ts=1.0,
            kind=EventKind.ATOM_STATE,
            atom_name="a",
            atom_type="task",
            state="SUCCESS",
            old_state="RUNNING",
            details={"has_result": True, "nested": {"a": [1, 2, 3]}},
        )
    ]
    publisher.publish(sent)
    assert drain(subscriber) == sent


def test_an_event_with_no_atom_survives(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    sent = [
        Event(
            run_id="run-1",
            seq=1,
            ts=1.0,
            kind=EventKind.FLOW_STATE,
            state="RUNNING",
        )
    ]
    publisher.publish(sent)
    assert drain(subscriber) == sent


def test_events_from_two_runs_in_one_batch(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    sent = make_events(1, run_id="a") + make_events(1, run_id="b")
    publisher.publish(sent)
    assert drain(subscriber) == sent


def test_consume_can_be_called_repeatedly(
    publisher: OsloMessagingTransport, subscriber: OsloMessagingSubscriber
) -> None:
    publisher.publish(make_events(1))
    assert len(drain(subscriber)) == 1
    publisher.publish(make_events(2))
    assert len(drain(subscriber)) == 2


def test_a_borrowed_transport_is_not_released(
    conf: cfg.ConfigOpts, topic: str
) -> None:
    """The service that lent it is still using it."""
    shared = oslo_messaging.get_notification_transport(conf, url=URL)
    publisher = OsloMessagingTransport(
        topic=topic, conf=conf, transport=shared
    )
    with publisher:
        publisher.publish(make_events(1))
    # Still usable: stop() released the notifier, not the transport.
    assert oslo_messaging.Notifier(shared, topics=[topic]) is not None


def test_an_owned_transport_is_built_on_demand(
    conf: cfg.ConfigOpts, topic: str
) -> None:
    publisher = OsloMessagingTransport(URL, topic=topic, conf=conf)
    assert publisher._transport is None
    publisher.start()
    assert publisher._transport is not None
    publisher.stop()
    assert publisher._transport is None


def test_both_ends_are_context_managers(
    conf: cfg.ConfigOpts, topic: str
) -> None:
    with (
        OsloMessagingTransport(URL, topic=topic, conf=conf) as pub,
        OsloMessagingSubscriber(
            URL, topic=topic, pool=f"pool-{topic}", conf=conf, park=2.0
        ) as sub,
    ):
        pub.publish(make_events(1))
        assert len(drain(sub)) == 1


def test_stopping_twice_is_harmless(conf: cfg.ConfigOpts, topic: str) -> None:
    sub = OsloMessagingSubscriber(
        URL, topic=topic, pool=f"pool-{topic}", conf=conf
    )
    sub.start()
    sub.start()  # idempotent
    sub.stop()
    sub.stop()


def test_a_publisher_and_subscriber_share_defaults() -> None:
    """Wired up with no arguments, the two ends must still meet."""
    from taskflow_meter.transports import oslo_messaging as tfm_om

    assert OsloMessagingTransport().topic == OsloMessagingSubscriber().topic
    assert tfm_om.DEFAULT_TOPIC == "taskflow-meter"


def test_a_slow_handler_still_gets_its_verdict_through(
    conf: cfg.ConfigOpts, topic: str
) -> None:
    """The narrow race the parking handshake has to survive.

    The delivery thread gives up waiting at the same moment a consumer
    claims the batch. It must not requeue behind the consumer's back --
    that would store the batch and redeliver it anyway -- so it waits
    out the verdict instead.
    """
    park = 0.1
    publisher = OsloMessagingTransport(URL, topic=topic, conf=conf)
    sub = OsloMessagingSubscriber(
        URL, topic=topic, pool=f"pool-{topic}", conf=conf, park=park
    )

    def slow(_events: Sequence[Event]) -> None:
        # Comfortably longer than park, so the delivery thread times
        # out while this is still holding the claim.
        time.sleep(park * 5)

    with publisher, sub:
        publisher.publish(make_events(2))
        assert sub.consume(slow, timeout=2.0) == 2
        # Handled, so it does not come back.
        assert drain(sub, timeout=0.5) == []


def test_stopping_releases_a_parked_delivery_promptly(
    conf: cfg.ConfigOpts, topic: str
) -> None:
    """stop() must not wait out `park` for every parked batch."""
    park = 30.0
    publisher = OsloMessagingTransport(URL, topic=topic, conf=conf)
    sub = OsloMessagingSubscriber(
        URL, topic=topic, pool=f"pool-{topic}", conf=conf, park=park
    )
    sub.start()
    try:
        publisher.publish(make_events(1))
        deadline = time.monotonic() + 5.0
        while sub._pending.empty() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not sub._pending.empty(), "nothing ever parked"

        began = time.monotonic()
        sub.stop()
        assert time.monotonic() - began < park / 2
    finally:
        sub.stop()
        publisher.stop()

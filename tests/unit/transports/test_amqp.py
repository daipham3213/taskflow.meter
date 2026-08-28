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

"""Carrying events over a broker.

Driven against kombu's in-process transport, which is a real broker
implementation rather than a mock -- the wire format, the declarations
and the acknowledgements all happen.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from collections.abc import Sequence

import kombu
import pytest

from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.transports.amqp import AMQPSubscriber
from taskflow_meter.transports.amqp import AMQPTransport
from taskflow_meter.transports.amqp import build_queue

URL = "memory://"


@pytest.fixture
def broker() -> Iterator[kombu.Connection]:
    connection = kombu.Connection(URL)
    try:
        yield connection
    finally:
        connection.release()


@pytest.fixture
def queue_name(request: pytest.FixtureRequest) -> str:
    """A queue of this test's own.

    kombu's in-process broker keeps its state for the whole process,
    not per connection, so tests sharing a queue name inherit each
    other's leftovers.
    """
    return f"tfm-test-{request.node.name}"[:180]


@pytest.fixture
def publisher(broker: kombu.Connection, queue_name: str) -> AMQPTransport:
    return AMQPTransport(
        URL, connection=broker, queue=queue_name, routing_key=queue_name
    )


@pytest.fixture
def subscriber(broker: kombu.Connection, queue_name: str) -> AMQPSubscriber:
    return AMQPSubscriber(
        URL, connection=broker, queue=queue_name, routing_key=queue_name
    )


def make_events(count: int = 3) -> list[Event]:
    return [
        Event(
            run_id="run-1",
            seq=seq,
            ts=float(seq),
            kind=EventKind.ATOM_PROGRESS,
            atom_name="a",
            progress=seq / 10,
            details={"progress_details": {"at_progress": seq / 10}},
        )
        for seq in range(1, count + 1)
    ]


def drain(subscriber: AMQPSubscriber) -> list[Event]:
    received: list[Event] = []

    def handler(events: Sequence[Event]) -> None:
        received.extend(events)

    subscriber.consume(handler, timeout=0.5)
    return received


def test_a_batch_round_trips_exactly(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    sent = make_events()
    publisher.publish(sent)
    assert drain(subscriber) == sent


def test_consume_reports_how_many_it_delivered(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    publisher.publish(make_events(4))
    assert subscriber.consume(lambda _events: None, timeout=0.5) == 4


def test_an_empty_queue_returns_promptly(
    subscriber: AMQPSubscriber,
) -> None:
    assert subscriber.consume(lambda _events: None, timeout=0.1) == 0


def test_events_published_before_anyone_listened_still_arrive(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    # The queue is durable and declared on publish, so a collector that
    # starts late is not a lost run.
    publisher.publish(make_events(2))
    assert len(drain(subscriber)) == 2


def test_several_batches_are_all_delivered(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    publisher.publish(make_events(2))
    publisher.publish(make_events(3))
    assert len(drain(subscriber)) == 5


def test_a_handler_that_fails_leaves_the_message(
    publisher: AMQPTransport,
    subscriber: AMQPSubscriber,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Dropping monitoring data silently is the worst outcome."""
    publisher.publish(make_events(1))

    def explode(_events: Sequence[Event]) -> None:
        msg = "cannot store that"
        raise RuntimeError(msg)

    logger = "taskflow_meter.transports.amqp"
    with caplog.at_level(logging.ERROR, logger=logger):
        assert subscriber.consume(explode, timeout=0.3) == 0
    assert "handler failed" in caplog.text

    # Still there for the next attempt.
    assert len(drain(subscriber)) == 1


def test_an_unreadable_message_is_discarded_not_replayed_forever(
    broker: kombu.Connection,
    subscriber: AMQPSubscriber,
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A message we cannot parse will never become parseable.
    queue = build_queue(queue_name, routing_key=queue_name)
    producer = broker.Producer(serializer="json")
    producer.publish(
        {"not-events": True},
        exchange=queue.exchange,
        routing_key=queue.routing_key,
        declare=[queue],
    )

    logger = "taskflow_meter.transports.amqp"
    with caplog.at_level(logging.ERROR, logger=logger):
        assert subscriber.consume(lambda _e: None, timeout=0.3) == 0
    assert "unreadable" in caplog.text
    assert drain(subscriber) == []


def test_a_borrowed_connection_is_not_released(
    broker: kombu.Connection,
) -> None:
    transport = AMQPTransport(URL, connection=broker)
    transport.stop()
    # Still usable: it belongs to whoever passed it in.
    transport.publish(make_events(1))


def test_an_owned_connection_is_opened_on_demand() -> None:
    transport = AMQPTransport(URL)
    assert transport.connection is transport.connection
    transport.stop()
    # Reopens rather than failing.
    assert transport.connection is not None
    transport.stop()


def test_both_ends_are_context_managers() -> None:
    with AMQPTransport(URL) as publisher, AMQPSubscriber(URL) as sub:
        publisher.publish(make_events(1))
        assert isinstance(sub, AMQPSubscriber)


def test_a_custom_queue_is_honoured(
    broker: kombu.Connection, queue_name: str
) -> None:
    other = f"{queue_name}-other"
    publisher = AMQPTransport(
        URL, connection=broker, queue=other, routing_key=other
    )
    listener = AMQPSubscriber(
        URL, connection=broker, queue=other, routing_key=other
    )
    publisher.publish(make_events(1))
    assert len(drain(listener)) == 1


def test_the_default_queue_is_durable() -> None:
    # A monitoring queue that discards what it could not deliver is one
    # that lies about the gap.
    assert build_queue().durable is True
    assert build_queue().exchange.durable is True


def test_the_plugin_names() -> None:
    assert AMQPTransport.name == "amqp"
    assert AMQPSubscriber.name == "amqp"


def test_a_json_string_body_is_accepted(
    broker: kombu.Connection, subscriber: AMQPSubscriber
) -> None:
    # Some brokers hand back the raw string rather than a parsed body.
    from taskflow_meter.transports.amqp import _decode

    sent = make_events(2)
    import json

    body = json.dumps({"events": [event.to_dict() for event in sent]})
    assert _decode(body) == sent


def test_publishing_declares_the_queue_it_needs(
    broker: kombu.Connection, queue_name: str
) -> None:
    """A flow starting before the collector must not publish into
    nothing."""
    # A queue name nothing has declared yet: if publish did not declare
    # it, the events would have nowhere to land.
    fresh = f"{queue_name}-never-seen"
    publisher = AMQPTransport(
        URL, connection=broker, queue=fresh, routing_key=fresh
    )
    publisher.publish(make_events(1))

    listener = AMQPSubscriber(
        URL, connection=broker, queue=fresh, routing_key=fresh
    )
    assert len(drain(listener)) == 1


def test_the_handler_sees_whole_batches(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    # Batching is what makes a broker round trip worth paying for.
    batches: list[int] = []
    publisher.publish(make_events(5))
    subscriber.consume(lambda events: batches.append(len(events)), timeout=0.5)
    assert batches == [5]


def test_an_event_with_no_atom_survives(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    event = Event(
        run_id="run-1",
        seq=1,
        ts=1.0,
        kind=EventKind.FLOW_STATE,
        state="RUNNING",
    )
    publisher.publish([event])
    assert drain(subscriber) == [event]


def test_details_survive_the_wire(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    event = Event(
        run_id="run-1",
        seq=1,
        ts=1.0,
        kind=EventKind.FLOW_STRUCTURE,
        details={"nodes": [{"name": "a", "kind": "task"}], "edges": []},
    )
    publisher.publish([event])
    (received,) = drain(subscriber)
    assert received.details == event.details


def test_consume_can_be_called_repeatedly(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    # The caller owns the loop, so it can shut down between batches
    # rather than being trapped inside one.
    for _ in range(3):
        publisher.publish(make_events(1))
        assert subscriber.consume(lambda _e: None, timeout=0.3) == 1


def test_a_subscriber_with_its_own_connection() -> None:
    subscriber = AMQPSubscriber(URL)
    assert subscriber.connection is subscriber.connection
    subscriber.stop()
    subscriber.stop()


def test_unknown_body_shape_is_reported(broker: kombu.Connection) -> None:
    from taskflow_meter.transports.amqp import _decode

    with pytest.raises(KeyError):
        _decode({"wrong": []})


def test_a_publisher_and_subscriber_share_defaults() -> None:
    publisher = AMQPTransport(URL)
    subscriber = AMQPSubscriber(URL)
    assert publisher._queue.name == subscriber._queue.name


def test_publish_accepts_a_tuple(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    publisher.publish(tuple(make_events(2)))
    assert len(drain(subscriber)) == 2


def test_events_from_two_runs_in_one_batch(
    publisher: AMQPTransport, subscriber: AMQPSubscriber
) -> None:
    events = [
        Event(run_id="run-1", seq=1, ts=1.0, kind=EventKind.HEARTBEAT),
        Event(run_id="run-2", seq=1, ts=1.0, kind=EventKind.HEARTBEAT),
    ]
    publisher.publish(events)
    assert {event.run_id for event in drain(subscriber)} == {
        "run-1",
        "run-2",
    }

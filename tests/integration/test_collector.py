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

"""The collector deployment, end to end.

M8's exit criterion. A flow publishes to a broker, a collector consumes
and writes to the meter's own database, and an API worker serves that
store without polling anything -- each of them a component that would
be its own process in a real deployment.

They are wired in one process here because kombu's in-memory broker
does not cross process boundaries and a test suite should not need a
running RabbitMQ. What that does not cover is genuinely inter-process
behaviour; everything else -- the wire format, the schema, idempotent
replay, and the API reading somebody else's writes -- is exercised for
real.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import kombu
import oslo_messaging
import pytest
from oslo_config import cfg
from taskflow import engines
from taskflow.patterns import linear_flow

from taskflow_meter import states
from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.collect import attach
from taskflow_meter.datasource.sqlalchemy import SQLADataSource
from taskflow_meter.datasource.sqlalchemy import upgrade
from taskflow_meter.events import EventKind
from taskflow_meter.meter import Meter
from taskflow_meter.transports.amqp import AMQPSubscriber
from taskflow_meter.transports.amqp import AMQPTransport
from taskflow_meter.transports.oslo_messaging import OsloMessagingSubscriber
from taskflow_meter.transports.oslo_messaging import OsloMessagingTransport
from tests import wsgi_client
from tests.conftest import ProgressingTask


@pytest.fixture
def store_url(tmp_path: Path) -> str:
    url = f"sqlite:///{tmp_path / 'meter.db'}"
    # The way a deployment does it: migrate, do not create_all.
    upgrade(url)
    return url


@pytest.fixture
def broker() -> Iterator[kombu.Connection]:
    connection = kombu.Connection("memory://")
    try:
        yield connection
    finally:
        connection.release()


def run_a_flow(broker: kombu.Connection) -> str:
    """The process running the flow: attach, publish, run."""
    publisher = AMQPTransport("memory://", connection=broker)
    flow = linear_flow.Flow("demo").add(
        ProgressingTask("first", steps=(0.5,)),
        ProgressingTask("second"),
    )
    engine = engines.load(flow)
    with attach(engine, publishers=[publisher]) as watched:
        engine.run()
        assert watched.flush(10.0)
        return watched.run_id


def collect_into(broker: kombu.Connection, store: SQLADataSource) -> int:
    """The collector process: consume, write, repeat."""
    subscriber = AMQPSubscriber("memory://", connection=broker)
    return subscriber.consume(store.apply_many, timeout=0.5)


@pytest.mark.integration
def test_a_flow_reaches_the_api_through_a_broker_and_a_database(
    store_url: str, broker: kombu.Connection
) -> None:
    run_id = run_a_flow(broker)

    store = SQLADataSource(store_url)
    assert collect_into(broker, store) > 0

    # The API worker: a different object, a different connection, and
    # nothing polling anything.
    reader = SQLADataSource(store_url)
    meter = Meter(reader, poll=False)
    app = __import__("taskflow_meter.api.wsgi", fromlist=["WSGIApp"]).WSGIApp(
        meter
    )

    listing = wsgi_client.request(app, "/api/v1/flows")
    assert listing.status == 200
    (flow,) = listing.json()["flows"]
    assert flow["run_id"] == run_id
    assert flow["state"] == states.SUCCESS
    assert flow["completion"] == pytest.approx(1.0)

    detail = wsgi_client.request(app, f"/api/v1/flows/{run_id}")
    assert [atom["name"] for atom in detail.json()["atoms"]] == [
        "first",
        "second",
    ]

    events = wsgi_client.request(app, f"/api/v1/flows/{run_id}/events").json()
    kinds = {event["kind"] for event in events["events"]}
    assert "atom_progress" in kinds
    assert events["truncated"] is False


@pytest.mark.integration
def test_the_same_journey_over_the_notification_bus(store_url: str) -> None:
    """The oslo.messaging transport, end to end.

    Same route as the AMQP test -- flow, bus, collector, database, API
    worker -- over the transport an OpenStack service already has
    configured, on oslo.messaging's in-process ``fake://`` driver.

    The collector starts first here, and has to: a notifier cannot
    create the listener's queue, so a bus with nothing bound to it
    drops what is published. See the transport's module docstring.
    """
    conf = cfg.ConfigOpts()
    topic = "tfm-integration"
    # One notification transport, shared -- which is how a service does
    # it, and which keeps either end from cleaning up the other's bus.
    bus = oslo_messaging.get_notification_transport(conf, url="fake://")
    subscriber = OsloMessagingSubscriber(
        topic=topic, pool=f"pool-{topic}", conf=conf, transport=bus, park=5.0
    )

    with subscriber:
        publisher = OsloMessagingTransport(
            topic=topic, conf=conf, transport=bus
        )
        flow = linear_flow.Flow("demo").add(
            ProgressingTask("first", steps=(0.5,)),
            ProgressingTask("second"),
        )
        engine = engines.load(flow)
        with attach(engine, publishers=[publisher]) as watched:
            engine.run()
            assert watched.flush(10.0)
            run_id = watched.run_id

        store = SQLADataSource(store_url)
        assert subscriber.consume(store.apply_many, timeout=5.0) > 0

    reader = SQLADataSource(store_url)
    app = WSGIApp(Meter(reader, poll=False))

    (flow_payload,) = wsgi_client.request(app, "/api/v1/flows").json()["flows"]
    assert flow_payload["run_id"] == run_id
    assert flow_payload["state"] == states.SUCCESS
    assert flow_payload["completion"] == pytest.approx(1.0)

    detail = wsgi_client.request(app, f"/api/v1/flows/{run_id}")
    assert [atom["name"] for atom in detail.json()["atoms"]] == [
        "first",
        "second",
    ]


@pytest.mark.integration
def test_the_graph_survives_the_whole_journey(
    store_url: str, broker: kombu.Connection
) -> None:
    # The one thing only the in-process path knows, carried over a
    # broker, through a schema, and back out of the API.
    run_id = run_a_flow(broker)
    store = SQLADataSource(store_url)
    collect_into(broker, store)

    page = store.events_since(run_id, limit=1000)
    structure = page.events[0]
    assert structure.kind is EventKind.FLOW_STRUCTURE
    assert structure.details["atom_count"] == 2
    assert {"from": "first", "to": "second"} in structure.details["edges"]


@pytest.mark.integration
def test_a_collector_that_replays_does_not_double_count(
    store_url: str, broker: kombu.Connection
) -> None:
    """A reconnecting collector redelivers; that must be a no-op."""
    run_id = run_a_flow(broker)
    store = SQLADataSource(store_url)
    collect_into(broker, store)

    before = store.events_since(run_id, limit=1000).events
    # Re-apply everything, as a redelivery would.
    store.apply_many(before)
    after = store.events_since(run_id, limit=1000).events

    assert [event.seq for event in after] == [event.seq for event in before]


@pytest.mark.integration
def test_the_store_keeps_what_the_deployment_decides(
    store_url: str, broker: kombu.Connection
) -> None:
    # Unlike the persistence datasource, which inherits taskflow's
    # retention, this one has its own.
    run_id = run_a_flow(broker)
    store = SQLADataSource(store_url)
    collect_into(broker, store)
    assert store.get_flow(run_id) is not None

    assert store.prune(before=0.0) == 0
    assert store.prune(before=1e12) == 1
    assert store.get_flow(run_id) is None
    assert store.events_since(run_id).events == ()


@pytest.mark.integration
def test_events_published_before_the_collector_started_still_arrive(
    store_url: str, broker: kombu.Connection
) -> None:
    """The queue is durable, so a late collector is not a lost run."""
    run_id = run_a_flow(broker)

    # Nothing was consuming while the flow ran.
    store = SQLADataSource(store_url)
    assert collect_into(broker, store) > 0
    assert store.get_flow(run_id) is not None


def test_the_schema_alembic_builds_matches_the_models(
    tmp_path: Path,
) -> None:
    """A migration that drifts from the models is worse than none."""
    import sqlalchemy as sa

    from taskflow_meter.datasource.sqlalchemy.models import metadata

    migrated = f"sqlite:///{tmp_path / 'migrated.db'}"
    upgrade(migrated)
    created = sa.create_engine(f"sqlite:///{tmp_path / 'created.db'}")
    metadata.create_all(created)

    def describe(engine: Any) -> dict[str, Any]:
        inspector = sa.inspect(engine)
        return {
            table: sorted(
                (column["name"], str(column["type"]))
                for column in inspector.get_columns(table)
            )
            for table in sorted(inspector.get_table_names())
            if table.startswith("taskflow_meter_")
        }

    assert describe(sa.create_engine(migrated)) == describe(created)

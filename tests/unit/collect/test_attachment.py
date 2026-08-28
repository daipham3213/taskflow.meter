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

"""Attaching to an engine, and the graph only the compiler knows."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from taskflow import engines
from taskflow.patterns import graph_flow
from taskflow.patterns import linear_flow
from taskflow.patterns import unordered_flow

from taskflow_meter import states
from taskflow_meter.collect.attachment import attach
from taskflow_meter.collect.attachment import describe_graph
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.transports.base import Publisher
from taskflow_meter.transports.memory import MemoryTransport
from tests.conftest import ExplodingTask
from tests.conftest import ProgressingTask


def linear() -> Any:
    return linear_flow.Flow("demo").add(
        ProgressingTask("first", steps=(0.5,)),
        ProgressingTask("second"),
    )


def events_of(store: Any, run_id: str) -> list[Event]:
    return list(store.events_since(run_id, limit=1000).events)


# -- the graph -----------------------------------------------------------


def test_the_graph_describes_the_flow() -> None:
    engine = engines.load(linear())
    graph = describe_graph(engine)

    names = {node["name"] for node in graph["nodes"]}
    assert {"first", "second"} <= names
    assert graph["atom_count"] == 2
    assert {"from": "first", "to": "second"} in graph["edges"]


def test_the_graph_of_a_parallel_flow_has_no_edge_between_siblings() -> None:
    # The shape is the point: these two can run at once, and nothing in
    # persistence would ever say so.
    flow = unordered_flow.Flow("parallel").add(
        ProgressingTask("a"), ProgressingTask("b")
    )
    graph = describe_graph(engines.load(flow))

    pairs = {(edge["from"], edge["to"]) for edge in graph["edges"]}
    assert ("a", "b") not in pairs
    assert ("b", "a") not in pairs


def test_the_graph_of_a_dependency_flow() -> None:
    flow = graph_flow.Flow("graph")
    first = ProgressingTask("first", steps=())
    second = ProgressingTask("second", steps=())
    flow.add(first, second)
    flow.link(first, second)

    graph = describe_graph(engines.load(flow))
    assert {"from": "first", "to": "second"} in graph["edges"]


def test_the_graph_is_ordered_deterministically() -> None:
    engine = engines.load(linear())
    assert describe_graph(engine) == describe_graph(engine)


# -- attaching -----------------------------------------------------------


def test_attaching_records_the_whole_run() -> None:
    engine = engines.load(linear())
    with attach(engine) as watched:
        engine.run()
        assert watched.flush()
        store = watched.store
        assert store is not None
        events = events_of(store, watched.run_id)

    kinds = [event.kind for event in events]
    assert kinds[0] is EventKind.FLOW_STRUCTURE
    assert EventKind.FLOW_STATE in kinds
    assert EventKind.ATOM_STATE in kinds
    assert EventKind.ATOM_PROGRESS in kinds


def test_the_store_can_be_read_as_a_snapshot() -> None:
    engine = engines.load(linear())
    with attach(engine) as watched:
        engine.run()
        assert watched.flush()
        store = watched.store
        assert store is not None
        flow = store.get_flow(watched.run_id)

    assert flow is not None
    assert flow.state == states.SUCCESS
    assert flow.atom_names == ("first", "second")
    assert flow.completion == pytest.approx(1.0)


def test_sequence_numbers_are_gap_free_across_both_producers() -> None:
    engine = engines.load(linear())
    with attach(engine) as watched:
        engine.run()
        assert watched.flush()
        store = watched.store
        assert store is not None
        events = events_of(store, watched.run_id)

    assert [event.seq for event in events] == list(range(1, len(events) + 1))


def test_extra_publishers_receive_everything() -> None:
    transport = MemoryTransport()
    engine = engines.load(linear())
    with attach(engine, publishers=[transport]) as watched:
        engine.run()
        assert watched.flush()
        store = watched.store
        assert store is not None
        expected = len(events_of(store, watched.run_id))

    assert len(transport.drain()) == expected


def test_a_supplied_store_is_used() -> None:
    store = MemoryDataSource()
    engine = engines.load(linear())
    with attach(engine, store=store) as watched:
        engine.run()
        assert watched.flush()
    assert store.get_flow(watched.run_id) is not None


def test_the_structure_event_can_be_turned_off() -> None:
    engine = engines.load(linear())
    with attach(engine, emit_structure=False) as watched:
        engine.run()
        assert watched.flush()
        store = watched.store
        assert store is not None
        kinds = {event.kind for event in events_of(store, watched.run_id)}

    assert EventKind.FLOW_STRUCTURE not in kinds


def test_an_undescribable_graph_does_not_stop_the_rest(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Topology is a bonus; states and progress are the point.
    def explode(_engine: Any) -> dict[str, Any]:
        msg = "no graph for you"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        "taskflow_meter.collect.attachment.describe_graph", explode
    )
    engine = engines.load(linear())
    logger = "taskflow_meter.collect.attachment"

    with (
        caplog.at_level(logging.ERROR, logger=logger),
        attach(engine) as watched,
    ):
        engine.run()
        assert watched.flush()
        store = watched.store
        assert store is not None
        kinds = {event.kind for event in events_of(store, watched.run_id)}

    assert "could not describe" in caplog.text
    assert EventKind.ATOM_STATE in kinds


# -- teardown ------------------------------------------------------------


def test_everything_is_released_on_the_way_out() -> None:
    engine = engines.load(linear())
    with attach(engine) as watched:
        engine.run()
    assert watched.tap.bound == 0
    assert not watched.pipeline.running


def test_everything_is_released_when_the_flow_raises() -> None:
    engine = engines.load(linear_flow.Flow("demo").add(ExplodingTask("boom")))
    with attach(engine) as watched, pytest.raises(RuntimeError, match="boom"):
        engine.run()

    assert watched.tap.bound == 0
    assert not watched.pipeline.running


def test_a_failing_publisher_does_not_change_the_flows_outcome() -> None:
    class Hostile(Publisher):
        name = "hostile"

        def publish(self, events: Any) -> None:
            msg = "no"
            raise RuntimeError(msg)

    engine = engines.load(linear())
    with attach(engine, publishers=[Hostile()]) as watched:
        engine.run()
        watched.flush()

    assert engine.storage.get_flow_state() == states.SUCCESS
    assert watched.pipeline.stats.errors > 0

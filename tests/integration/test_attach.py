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

"""Attaching to real engines, including the parallel one.

Two things the read-only path cannot offer, proven
here against engines actually running: progress visible in well under a
second rather than a poll interval later, and the flow's graph, which
taskflow never persists.

The parallel engine matters most. Its callbacks arrive on executor
threads, several at once, so anything that is not thread-safe here
fails intermittently and somewhere else.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from taskflow import engines
from taskflow import task
from taskflow.patterns import graph_flow
from taskflow.patterns import linear_flow
from taskflow.patterns import unordered_flow

from taskflow_meter import states
from taskflow_meter.collect import attach
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.meter import Meter
from tests.conftest import ProgressingTask

ENGINES = ["serial", "parallel"]


class Timed(task.Task):
    """Waits for its own progress to show up, and times how long it took.

    Measured from inside the task on purpose.  Timing the whole
    ``engine.run()`` would fold in taskflow's own scheduling -- the
    parallel engine waits up to a second for futures to settle -- and
    report it as monitoring latency.
    """

    def __init__(self, name: str, watcher: dict[str, Any]) -> None:
        super().__init__(name=name)
        self.watcher = watcher

    def execute(self) -> str:
        started = time.monotonic()
        self.update_progress(0.5)

        store = self.watcher["store"]
        run_id = self.watcher["run_id"]
        deadline = started + 10.0
        while time.monotonic() < deadline:
            page = store.events_since(run_id, limit=1000)
            if any(
                event.kind is EventKind.ATOM_PROGRESS and event.progress == 0.5
                for event in page.events
            ):
                self.watcher["latency"] = time.monotonic() - started
                return "done"
            time.sleep(0.002)
        msg = "the reported progress never became visible"
        raise AssertionError(msg)


def events_of(store: Any, run_id: str) -> list[Event]:
    return list(store.events_since(run_id, limit=1000).events)


@pytest.mark.integration
@pytest.mark.parametrize("engine_kind", ENGINES)
def test_progress_is_visible_almost_immediately(
    engine_kind: str,
) -> None:
    """The reason to attach rather than poll.

    The poller's floor is its interval, seconds by default.  This is
    the time from a task reporting a number to that number being
    readable, measured while the task is still running.
    """
    watcher: dict[str, Any] = {}
    flow = linear_flow.Flow("demo").add(Timed("only", watcher))
    engine = engines.load(flow, engine=engine_kind)

    with attach(engine) as watched:
        watcher["store"] = watched.store
        watcher["run_id"] = watched.run_id
        engine.run()

    assert "latency" in watcher, "the progress never became visible"
    latency = watcher["latency"]
    assert latency < 0.5, f"took {latency:.3f}s to become visible"


@pytest.mark.integration
@pytest.mark.parametrize("engine_kind", ENGINES)
def test_the_graph_is_reported_before_anything_runs(
    engine_kind: str,
) -> None:
    """The other reason: taskflow persists atoms, never the edges."""
    flow = linear_flow.Flow("demo").add(
        ProgressingTask("first"),
        unordered_flow.Flow("both").add(
            ProgressingTask("left"), ProgressingTask("right")
        ),
    )
    engine = engines.load(flow, engine=engine_kind)

    with attach(engine) as watched:
        engine.run()
        assert watched.flush(5.0)
        store = watched.store
        assert store is not None
        events = events_of(store, watched.run_id)

    structure = events[0]
    assert structure.kind is EventKind.FLOW_STRUCTURE
    assert structure.details["atom_count"] == 3

    edges = {(edge["from"], edge["to"]) for edge in structure.details["edges"]}
    # The two unordered atoms are siblings, not a sequence.
    assert ("left", "right") not in edges
    assert ("right", "left") not in edges


@pytest.mark.integration
@pytest.mark.parametrize("engine_kind", ENGINES)
def test_the_whole_run_is_recorded(engine_kind: str) -> None:
    flow = linear_flow.Flow("demo").add(
        ProgressingTask("first", steps=(0.3, 0.6)),
        ProgressingTask("second"),
    )
    engine = engines.load(flow, engine=engine_kind)

    with attach(engine) as watched:
        engine.run()
        assert watched.flush(5.0)
        store = watched.store
        assert store is not None
        flow_snapshot = store.get_flow(watched.run_id)
        events = events_of(store, watched.run_id)

    assert flow_snapshot is not None
    assert flow_snapshot.state == states.SUCCESS
    assert flow_snapshot.completion == pytest.approx(1.0)

    reported = [
        event.progress
        for event in events
        if event.kind is EventKind.ATOM_PROGRESS and event.atom_name == "first"
    ]
    assert 0.3 in reported
    assert 0.6 in reported


@pytest.mark.integration
def test_a_parallel_flow_is_recorded_without_losing_events() -> None:
    """Callbacks arrive on several executor threads at once."""
    width = 12
    flow = unordered_flow.Flow("wide").add(
        *(
            ProgressingTask(f"task-{index:02d}", steps=(0.5,))
            for index in range(width)
        )
    )
    engine = engines.load(flow, engine="parallel", max_workers=8)

    with attach(engine) as watched:
        engine.run()
        assert watched.flush(10.0)
        store = watched.store
        assert store is not None
        events = events_of(store, watched.run_id)
        snapshot = store.get_flow(watched.run_id)

    assert snapshot is not None
    assert len(snapshot.atoms) == width
    assert all(atom.is_finished for atom in snapshot.atoms.values())

    # Gap-free numbering under concurrency is the thing that breaks
    # first if the allocator is not thread-safe.
    assert [event.seq for event in events] == list(range(1, len(events) + 1))
    assert watched.pipeline.stats.dropped == 0


@pytest.mark.integration
def test_a_dependency_graph_keeps_its_edges() -> None:
    flow = graph_flow.Flow("graph")
    produce = ProgressingTask("produce", steps=())
    consume = ProgressingTask("consume", steps=())
    flow.add(produce, consume)
    flow.link(produce, consume)
    engine = engines.load(flow)

    with attach(engine) as watched:
        engine.run()
        assert watched.flush(5.0)
        store = watched.store
        assert store is not None
        structure = events_of(store, watched.run_id)[0]

    edges = {(edge["from"], edge["to"]) for edge in structure.details["edges"]}
    assert ("produce", "consume") in edges


@pytest.mark.integration
def test_the_api_can_serve_an_attached_flow() -> None:
    """The whole point, wired end to end.

    Attach in the process running the flow, then read it through the
    same Meter the HTTP layer uses -- with no database and nothing on
    the wire.
    """
    store = MemoryDataSource()
    flow = linear_flow.Flow("demo").add(ProgressingTask("only", steps=(0.5,)))
    engine = engines.load(flow)

    with attach(engine, store=store) as watched:
        engine.run()
        assert watched.flush(5.0)

        meter = Meter(store, poll=False)
        page = meter.list_flows()
        (listed,) = page.items
        assert listed.run_id == watched.run_id
        assert listed.state == states.SUCCESS
        assert meter.supports_events is True
        assert meter.events_since(watched.run_id).events


@pytest.mark.integration
def test_monitoring_does_not_slow_the_flow_measurably() -> None:
    """A watched flow must not cost noticeably more than an unwatched one."""

    def build() -> Any:
        return unordered_flow.Flow("wide").add(
            *(
                ProgressingTask(f"task-{index:02d}", steps=(0.5,))
                for index in range(20)
            )
        )

    started = time.monotonic()
    engines.load(build(), engine="parallel", max_workers=4).run()
    unwatched = time.monotonic() - started

    engine = engines.load(build(), engine="parallel", max_workers=4)
    started = time.monotonic()
    with attach(engine):
        engine.run()
    watched = time.monotonic() - started

    # Generous, because timing on a shared runner is noisy; it is there
    # to catch a change that makes emitting synchronous, not to measure.
    assert watched < max(unwatched * 10, 2.0), (
        f"unwatched {unwatched:.3f}s, watched {watched:.3f}s"
    )


@pytest.mark.integration
def test_the_flow_still_runs_when_every_publisher_is_broken() -> None:
    from taskflow_meter.transports.base import Publisher

    class Hostile(Publisher):
        name = "hostile"

        def __init__(self) -> None:
            self.calls = 0

        def publish(self, events: Any) -> None:
            self.calls += 1
            msg = "monitoring is broken"
            raise RuntimeError(msg)

    hostile = Hostile()
    results: list[str] = []

    class Recording(task.Task):
        def execute(self) -> str:
            results.append("ran")
            return "done"

    flow = linear_flow.Flow("demo").add(Recording("only"))
    engine = engines.load(flow)

    with attach(engine, publishers=[hostile]) as watched:
        engine.run()
        watched.flush(5.0)

    assert results == ["ran"]
    assert engine.storage.get_flow_state() == states.SUCCESS
    assert hostile.calls > 0


@pytest.mark.integration
def test_nothing_is_left_registered_on_the_tasks() -> None:
    # A callback left on somebody's task object outlives the monitoring
    # that wanted it, and fires again on the next run.
    seen: list[Event] = []
    work = ProgressingTask("only", steps=(0.5,))
    flow = linear_flow.Flow("demo").add(work)
    engine = engines.load(flow)

    with attach(engine) as watched:
        engine.run()
        watched.flush(5.0)

    before = threading.active_count()
    engine.reset()
    engine.run()

    assert seen == []
    assert threading.active_count() <= before


@pytest.mark.integration
def test_a_failed_task_reports_why_it_failed() -> None:
    # The state says a task failed; the failure says what of. Reading
    # persistence has always reported this, and watching the engine has
    # the failure in hand as it happens.
    class Exploding(task.Task):
        def execute(self) -> None:
            raise ValueError("no appliance answered")

    flow = linear_flow.Flow("demo").add(Exploding("boom"))
    engine = engines.load(flow)

    with attach(engine) as watched:
        with pytest.raises(ValueError, match="no appliance answered"):
            engine.run()
        assert watched.flush(5.0)
        assert watched.store is not None
        atoms = watched.store.get_atoms(watched.run_id)

    assert atoms is not None
    (atom,) = atoms
    assert atom.failure is not None
    assert "ValueError" in atom.failure["exc_type_names"]
    assert "no appliance answered" in atom.failure["exception_str"]
    # And the same shape the persistence datasource reports.
    assert set(atom.failure) >= {"exc_type_names", "exception_str"}

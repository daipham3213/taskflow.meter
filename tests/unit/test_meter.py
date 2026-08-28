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

"""The lifecycle handle everything else hangs off."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from taskflow_meter import states
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.meter import Meter
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource


@pytest.fixture
def flow_source() -> FakeSource:
    return FakeSource(
        make_flow(
            state=states.RUNNING,
            atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
        )
    )


# -- wiring --------------------------------------------------------------


def test_polling_gets_an_in_memory_store_by_default(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source)
    assert isinstance(meter.store, MemoryDataSource)
    assert meter.reader is meter.store
    assert meter.poller is not None


def test_reads_come_from_the_store_when_there_is_one(
    flow_source: FakeSource,
) -> None:
    # Serving snapshots from the source while serving events from the
    # store would let a client see a snapshot newer than the stream it is
    # resuming, and never learn it had missed the difference.
    meter = Meter(flow_source)
    assert meter.reader is meter.store


def test_without_polling_the_source_is_read_directly(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source, poll=False)
    assert meter.store is None
    assert meter.reader is flow_source
    assert meter.poller is None


def test_a_worker_can_read_a_store_someone_else_fills(
    flow_source: FakeSource,
) -> None:
    # poll=False with a store: the shape an API worker takes when a
    # separate collector process keeps the store warm.
    store = MemoryDataSource()
    meter = Meter(flow_source, store=store, poll=False)
    assert meter.reader is store
    assert meter.poller is None


# -- lifecycle -----------------------------------------------------------


def test_start_and_stop(flow_source: FakeSource) -> None:
    meter = Meter(flow_source, interval=0.01)
    assert not meter.running

    meter.start()
    try:
        assert meter.running
        assert meter.poller is not None
        assert meter.poller.running
    finally:
        meter.stop()

    assert not meter.running
    assert meter.poller is not None
    assert not meter.poller.running


def test_a_meter_with_no_poller_still_starts_and_stops(
    flow_source: FakeSource,
) -> None:
    started: list[str] = []

    def record(what: str) -> Callable[[], None]:
        return lambda: started.append(what)

    flow_source.start = record("start")  # type: ignore[method-assign]
    flow_source.stop = record("stop")  # type: ignore[method-assign]

    with Meter(flow_source, poll=False) as meter:
        assert meter.running
        assert meter.poller is None
    assert started == ["start", "stop"]


def test_references_are_counted(flow_source: FakeSource) -> None:
    # A host starting the meter and a request that also takes a reference
    # must not fight over who gets to stop it.
    meter = Meter(flow_source, interval=0.01)
    meter.start()
    meter.start()

    meter.stop()
    assert meter.running, "the second holder has not let go yet"

    meter.stop()
    assert not meter.running


def test_extra_stops_cannot_make_the_meter_unstoppable(
    flow_source: FakeSource,
) -> None:
    # A count allowed to go negative would need matching extra starts
    # before stop() worked again.
    meter = Meter(flow_source, interval=0.01)
    meter.stop()
    meter.stop()

    meter.start()
    assert meter.running
    meter.stop()
    assert not meter.running


def test_ensure_started_takes_no_reference(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source, interval=0.01)
    meter.ensure_started()
    meter.ensure_started()
    assert meter.running

    # One stop is enough: the lazy path owes no matching release.
    meter.stop()
    assert not meter.running


def test_ensure_started_restarts_after_a_stop(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source, interval=0.01)
    meter.start()
    meter.stop()

    meter.ensure_started()
    try:
        assert meter.running
    finally:
        meter.stop()


def test_the_context_manager_is_a_reference(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source, interval=0.01)
    with meter as entered:
        assert entered is meter
        assert meter.running
    assert not meter.running


def test_the_lifespan_handler_is_a_reference(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source, interval=0.01)

    async def exercise() -> None:
        async with meter.lifespan(app=None):
            assert meter.running

    asyncio.run(exercise())
    assert not meter.running


def test_the_source_and_store_are_started_and_stopped(
    flow_source: FakeSource,
) -> None:
    started: list[str] = []

    class Recording(MemoryDataSource):
        def start(self) -> None:
            started.append("start")

        def stop(self) -> None:
            started.append("stop")

    with Meter(flow_source, store=Recording(), interval=0.01):
        pass
    assert started == ["start", "stop"]


def test_shutdown_is_not_abandoned_when_one_part_objects(
    flow_source: FakeSource,
) -> None:
    class Awkward(MemoryDataSource):
        def stop(self) -> None:
            msg = "will not close"
            raise RuntimeError(msg)

    stopped: list[str] = []

    def record_stop() -> None:
        stopped.append("source")

    flow_source.stop = record_stop  # type: ignore[method-assign]

    meter = Meter(flow_source, store=Awkward(), interval=0.01)
    meter.start()
    meter.stop()

    assert not meter.running
    assert stopped == ["source"], "the source still has to be released"


def test_the_exit_hook_releases_every_holder(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source, interval=0.01)
    meter.start()
    meter.start()

    meter._atexit_stop()
    assert not meter.running


# -- queries -------------------------------------------------------------


def test_queries_are_answered_from_what_the_poller_stored(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source)
    meter.poll_once()

    page = meter.list_flows()
    assert [flow.run_id for flow in page.items] == ["run-1"]

    flow = meter.get_flow("run-1")
    assert flow is not None
    assert flow.state == states.RUNNING

    atoms = meter.get_atoms("run-1")
    assert atoms is not None
    assert [atom.name for atom in atoms] == ["a"]

    events = meter.events_since("run-1")
    assert [event.seq for event in events.events] == [1, 2, 3]


def test_query_filters_reach_the_reader(flow_source: FakeSource) -> None:
    meter = Meter(flow_source)
    meter.poll_once()
    assert meter.list_flows(state=states.SUCCESS).items == ()
    assert len(meter.list_flows(state=states.RUNNING).items) == 1


def test_whether_events_are_offered_follows_the_reader(
    flow_source: FakeSource,
) -> None:
    assert Meter(flow_source).supports_events is True

    # Reading straight through to a source that keeps no history -- what
    # the persistence datasource declares about itself.
    historyless = FakeSource()
    historyless.supports_events = False
    assert Meter(historyless, poll=False).supports_events is False


def test_polling_on_demand_needs_a_poller(
    flow_source: FakeSource,
) -> None:
    meter = Meter(flow_source, poll=False)
    with pytest.raises(RuntimeError, match="no poller"):
        meter.poll_once()

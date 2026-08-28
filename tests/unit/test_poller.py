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

"""Watching a state-only source and turning it into events."""

from __future__ import annotations

import logging
import threading
import time

import pytest

from taskflow_meter import states
from taskflow_meter.datasource.base import DEFAULT_EVENT_LIMIT
from taskflow_meter.datasource.base import DataSource
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.events import EventKind
from taskflow_meter.models import FlowSnapshot
from taskflow_meter.poller import Poller
from tests.conftest import make_atom
from tests.conftest import make_flow


class FakeSource(DataSource):
    """A source whose contents the test sets directly."""

    name = "fake"

    def __init__(self, *flows: FlowSnapshot) -> None:
        self.flows = list(flows)
        self.calls = 0
        self.fail_with: Exception | None = None
        self.polled = threading.Event()

    def set(self, *flows: FlowSnapshot) -> None:
        self.flows = list(flows)

    def list_flows(
        self,
        *,
        state: str | None = None,
        book_id: str | None = None,
        limit: int = 50,
        marker: str | None = None,
    ) -> FlowPage:
        self.calls += 1
        self.polled.set()
        if self.fail_with is not None:
            raise self.fail_with

        ordered = sorted(self.flows, key=lambda flow: flow.run_id)
        start = 0
        if marker is not None:
            positions = {f.run_id: i for i, f in enumerate(ordered)}
            start = positions[marker] + 1
        window = ordered[start : start + limit]
        more = len(ordered) > start + limit
        return FlowPage(
            items=tuple(window),
            next_marker=window[-1].run_id if more and window else None,
        )

    def get_flow(self, run_id: str) -> FlowSnapshot | None:
        return next((f for f in self.flows if f.run_id == run_id), None)

    def events_since(
        self,
        run_id: str,
        *,
        since_seq: int = 0,
        limit: int = DEFAULT_EVENT_LIMIT,
    ) -> EventPage:
        return EventPage(next_seq=since_seq)


@pytest.fixture
def sink() -> MemoryDataSource:
    return MemoryDataSource()


# -- construction --------------------------------------------------------


@pytest.mark.parametrize(
    ("interval", "page_size", "match"),
    [
        (0.0, 10, "interval"),
        (-1.0, 10, "interval"),
        (1.0, 0, "page_size"),
    ],
)
def test_useless_settings_are_rejected(
    sink: MemoryDataSource, interval: float, page_size: int, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        Poller(FakeSource(), sink, interval=interval, page_size=page_size)


# -- one pass ------------------------------------------------------------


def test_the_first_poll_reports_everything_it_finds(
    sink: MemoryDataSource,
) -> None:
    flow = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
    )
    poller = Poller(FakeSource(flow), sink)

    assert poller.poll_once() == 3
    stored = sink.get_flow("run-1")
    assert stored is not None
    assert stored.state == states.RUNNING
    assert stored.atoms["a"].progress == pytest.approx(0.5)


def test_a_quiet_poll_reports_nothing(sink: MemoryDataSource) -> None:
    source = FakeSource(make_flow(state=states.RUNNING))
    poller = Poller(source, sink)
    poller.poll_once()

    assert poller.poll_once() == 0
    assert sink.events_since("run-1").next_seq == 1


def test_only_the_difference_is_reported(sink: MemoryDataSource) -> None:
    source = FakeSource(
        make_flow(
            state=states.RUNNING,
            atoms=(make_atom("a", state=states.RUNNING, progress=0.1),),
        )
    )
    poller = Poller(source, sink)
    poller.poll_once()

    source.set(
        make_flow(
            state=states.RUNNING,
            atoms=(make_atom("a", state=states.RUNNING, progress=0.9),),
        )
    )
    assert poller.poll_once() == 1
    # The first poll emitted three: flow state, atom state, atom progress.
    page = sink.events_since("run-1", since_seq=3)
    (event,) = page.events
    assert event.kind is EventKind.ATOM_PROGRESS
    assert event.progress == pytest.approx(0.9)


def test_every_page_is_walked(sink: MemoryDataSource) -> None:
    source = FakeSource(
        *(
            make_flow(f"run-{index}", state=states.RUNNING)
            for index in range(5)
        )
    )
    poller = Poller(source, sink, page_size=2)
    poller.poll_once()

    assert len(sink.list_flows().items) == 5


def test_several_runs_are_tracked_independently(
    sink: MemoryDataSource,
) -> None:
    source = FakeSource(
        make_flow("run-1", state=states.RUNNING),
        make_flow("run-2", state=states.RUNNING),
    )
    poller = Poller(source, sink)
    poller.poll_once()

    source.set(
        make_flow("run-1", state=states.SUCCESS),
        make_flow("run-2", state=states.RUNNING),
    )
    assert poller.poll_once() == 1
    # Sequence numbers are per run, so run-2 is untouched.
    assert sink.events_since("run-2").next_seq == 1


def test_a_finished_flow_is_not_replayed(sink: MemoryDataSource) -> None:
    # Its snapshot is deliberately kept, so re-listing it says nothing.
    source = FakeSource(make_flow(state=states.SUCCESS))
    poller = Poller(source, sink)
    poller.poll_once()

    for _ in range(3):
        assert poller.poll_once() == 0


def test_a_vanished_run_is_dropped_quietly(
    sink: MemoryDataSource, caplog: pytest.LogCaptureFixture
) -> None:
    source = FakeSource(make_flow(state=states.RUNNING))
    poller = Poller(source, sink)
    poller.poll_once()

    source.set()
    with caplog.at_level(logging.DEBUG, logger="taskflow_meter.poller"):
        assert poller.poll_once() == 0
    assert "no longer reported" in caplog.text


def test_a_reappearing_run_is_described_afresh(
    sink: MemoryDataSource,
) -> None:
    # Retention expiring a run and the run coming back is indistinguishable
    # from a new one, so it is reported as first seen -- with sequence
    # numbers that continue rather than restart.
    flow = make_flow(state=states.RUNNING)
    source = FakeSource(flow)
    poller = Poller(source, sink)
    poller.poll_once()
    source.set()
    poller.poll_once()

    source.set(flow)
    assert poller.poll_once() == 1
    assert sink.events_since("run-1").next_seq == 2


def test_poll_once_lets_failures_out(sink: MemoryDataSource) -> None:
    source = FakeSource()
    source.fail_with = RuntimeError("backend down")
    poller = Poller(source, sink)

    with pytest.raises(RuntimeError, match="backend down"):
        poller.poll_once()


def test_counters_track_what_happened(sink: MemoryDataSource) -> None:
    source = FakeSource(make_flow(state=states.RUNNING))
    poller = Poller(source, sink)
    poller.poll_once()
    poller.poll_once()

    assert poller.stats.polls == 2
    assert poller.stats.events == 1
    assert poller.stats.flows_seen == 1
    assert poller.stats.errors == 0


# -- the background loop -------------------------------------------------


def wait_for(predicate: object, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return True
        time.sleep(0.01)
    return False


def test_the_loop_polls_until_stopped(sink: MemoryDataSource) -> None:
    source = FakeSource(make_flow(state=states.RUNNING))
    poller = Poller(source, sink, interval=0.01)

    poller.start()
    try:
        assert source.polled.wait(5.0)
        assert poller.running
        assert wait_for(lambda: poller.stats.polls >= 2)
    finally:
        poller.stop()

    assert not poller.running
    settled = poller.stats.polls
    time.sleep(0.05)
    assert poller.stats.polls == settled


def test_starting_twice_does_not_start_two_threads(
    sink: MemoryDataSource,
) -> None:
    poller = Poller(FakeSource(), sink, interval=0.01)
    before = threading.active_count()
    poller.start()
    poller.start()
    try:
        assert threading.active_count() == before + 1
    finally:
        poller.stop()


def test_stopping_an_unstarted_poller_is_harmless(
    sink: MemoryDataSource,
) -> None:
    poller = Poller(FakeSource(), sink)
    poller.stop()
    poller.stop()
    assert not poller.running


def test_the_loop_survives_a_failing_source(
    sink: MemoryDataSource,
) -> None:
    # A monitoring loop that dies on one bad read stops monitoring
    # silently, which is worse than a noisy one.
    seen: list[Exception] = []
    source = FakeSource()
    source.fail_with = RuntimeError("backend down")
    poller = Poller(source, sink, interval=0.01, on_error=seen.append)

    poller.start()
    try:
        assert wait_for(lambda: poller.stats.errors >= 2)
        assert poller.running
    finally:
        poller.stop()

    assert "backend down" in (poller.stats.last_error or "")
    assert seen


def test_a_failing_error_handler_does_not_stop_the_loop(
    sink: MemoryDataSource,
) -> None:
    def unhelpful(_exc: Exception) -> None:
        msg = "handler exploded"
        raise RuntimeError(msg)

    source = FakeSource()
    source.fail_with = RuntimeError("backend down")
    poller = Poller(source, sink, interval=0.01, on_error=unhelpful)

    poller.start()
    try:
        assert wait_for(lambda: poller.stats.errors >= 2)
    finally:
        poller.stop()


def test_the_allocator_is_the_one_doing_the_numbering(
    sink: MemoryDataSource,
) -> None:
    poller = Poller(FakeSource(make_flow(state=states.RUNNING)), sink)
    poller.poll_once()
    assert poller.allocator.peek("run-1") == 1


def test_an_error_with_no_handler_is_still_counted(
    sink: MemoryDataSource,
) -> None:
    source = FakeSource()
    source.fail_with = RuntimeError("backend down")
    poller = Poller(source, sink, interval=0.01)

    poller.start()
    try:
        assert wait_for(lambda: poller.stats.errors >= 2)
    finally:
        poller.stop()


def test_a_thread_that_will_not_stop_is_reported(
    sink: MemoryDataSource, caplog: pytest.LogCaptureFixture
) -> None:
    # Better a warning than a shutdown that blocks forever, or one that
    # claims to have stopped something it did not.
    entered = threading.Event()
    release = threading.Event()

    def blocking() -> int:
        entered.set()
        release.wait(30)
        return 0

    poller = Poller(FakeSource(), sink, interval=0.01)
    poller.poll_once = blocking  # type: ignore[method-assign]

    poller.start()
    try:
        assert entered.wait(5.0)
        with caplog.at_level(logging.WARNING, logger="taskflow_meter.poller"):
            poller.stop(timeout=0.01)
        assert "did not stop" in caplog.text
    finally:
        release.set()

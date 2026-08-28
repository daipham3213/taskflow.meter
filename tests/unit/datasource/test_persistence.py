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

"""Reading a real taskflow persistence backend."""

from __future__ import annotations

import contextlib
from typing import Any

import pytest
from taskflow import retry
from taskflow import task
from taskflow.patterns import linear_flow

from taskflow_meter import states
from taskflow_meter.datasource.base import UnknownMarkerError
from taskflow_meter.datasource.persistence import PersistenceDataSource
from taskflow_meter.datasource.persistence import _atom_snapshot
from taskflow_meter.datasource.persistence import _epoch
from taskflow_meter.models import RETRY
from taskflow_meter.models import TASK
from tests.conftest import ExplodingTask
from tests.conftest import ProgressingTask
from tests.conftest import make_logbook
from tests.conftest import run_flow


@pytest.fixture
def source(backend: Any) -> PersistenceDataSource:
    return PersistenceDataSource(backend)


# -- construction --------------------------------------------------------


def test_needs_exactly_one_of_backend_or_conf() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        PersistenceDataSource()
    with pytest.raises(ValueError, match="exactly one"):
        PersistenceDataSource(object(), conf={"connection": "sqlite://"})


def test_a_borrowed_backend_is_never_closed(backend: Any) -> None:
    # It belongs to the application being monitored.
    closed: list[bool] = []
    original = backend.close
    backend.close = lambda: closed.append(True)
    try:
        with PersistenceDataSource(backend):
            pass
    finally:
        backend.close = original
    assert closed == []


def test_a_backend_built_from_conf_is_ours_to_close(
    sqlite_url: str, backend: Any
) -> None:
    source = PersistenceDataSource(conf={"connection": sqlite_url})
    source.start()
    assert source.list_flows().items == ()
    source.stop()
    # Stopped and reusable: the next read reopens rather than failing.
    assert source.list_flows().items == ()


# -- an empty backend ----------------------------------------------------


def test_nothing_is_reported_for_an_empty_backend(
    source: PersistenceDataSource,
) -> None:
    assert source.list_flows().items == ()
    assert source.get_flow("nope") is None
    assert source.get_atoms("nope") is None


# -- a completed flow ----------------------------------------------------


@pytest.fixture
def completed(backend: Any) -> Any:
    book, flow_detail = make_logbook(backend)
    flow = linear_flow.Flow("demo-flow").add(
        ProgressingTask("first", steps=(0.25, 0.75)),
        ProgressingTask("second"),
    )
    run_flow(backend, flow, book, flow_detail)
    return book, flow_detail


def test_a_completed_flow_is_reported_in_full(
    source: PersistenceDataSource, completed: Any
) -> None:
    book, flow_detail = completed
    page = source.list_flows()

    (flow,) = page.items
    assert flow.run_id == flow_detail.uuid
    assert flow.name == "demo-flow"
    assert flow.state == states.SUCCESS
    assert flow.book_id == book.uuid
    assert flow.book_name == "demo-book"
    assert flow.atom_names == ("first", "second")
    assert flow.completion == pytest.approx(1.0)


def test_atom_detail_is_carried_over(
    source: PersistenceDataSource, completed: Any
) -> None:
    _book, flow_detail = completed
    flow = source.get_flow(flow_detail.uuid)
    assert flow is not None

    atom = flow.atoms["first"]
    assert atom.state == states.SUCCESS
    assert atom.intention == states.EXECUTE
    assert atom.atom_type == TASK
    assert atom.uuid is not None
    # taskflow sets progress to 1.0 when a task succeeds.
    assert atom.progress == pytest.approx(1.0)
    # Results are flagged, never copied into the monitoring path.
    assert atom.has_result is True


def test_the_books_creation_time_is_the_only_real_timestamp(
    source: PersistenceDataSource, completed: Any
) -> None:
    _book, flow_detail = completed
    flow = source.get_flow(flow_detail.uuid)
    assert flow is not None
    # Kept in meta rather than passed off as an observation time.
    assert isinstance(flow.meta["book_created_at"], float)


def test_get_atoms_is_sorted_and_skips_the_book_lookup(
    source: PersistenceDataSource, completed: Any
) -> None:
    _book, flow_detail = completed
    atoms = source.get_atoms(flow_detail.uuid)
    assert atoms is not None
    assert [atom.name for atom in atoms] == ["first", "second"]


def test_get_flow_is_none_for_an_unknown_run(
    source: PersistenceDataSource, completed: Any
) -> None:
    assert source.get_flow("00000000-0000-0000-0000-000000000000") is None


# -- observing a flow that is still running ------------------------------


def test_progress_is_visible_while_the_task_is_still_executing(
    backend: Any, source: PersistenceDataSource
) -> None:
    """The claim the whole read-only design rests on."""
    seen: list[Any] = []

    class Reporting(task.Task):
        def execute(self) -> str:
            self.update_progress(0.4)
            # A separate connection, opened from inside the running task.
            seen.append(source.get_flow(flow_detail.uuid))
            return "done"

    book, flow_detail = make_logbook(backend)
    flow = linear_flow.Flow("demo-flow").add(Reporting("reporter"))
    run_flow(backend, flow, book, flow_detail)

    (mid_run,) = seen
    assert mid_run is not None
    assert mid_run.state == states.RUNNING
    atom = mid_run.atoms["reporter"]
    assert atom.state == states.RUNNING
    assert atom.progress == pytest.approx(0.4)
    assert atom.has_result is False


# -- failure -------------------------------------------------------------


def test_a_failure_is_reported_with_its_detail(backend: Any) -> None:
    book, flow_detail = make_logbook(backend)
    flow = linear_flow.Flow("demo-flow").add(ExplodingTask("boom"))
    with contextlib.suppress(RuntimeError):
        run_flow(backend, flow, book, flow_detail)

    source = PersistenceDataSource(backend)
    observed = source.get_flow(flow_detail.uuid)
    assert observed is not None

    atom = observed.atoms["boom"]
    assert atom.state in {states.FAILURE, states.REVERTED}
    assert atom.failure is not None
    assert "RuntimeError" in str(atom.failure)


# -- retries -------------------------------------------------------------


def test_a_retry_controller_is_reported_as_a_retry(backend: Any) -> None:
    book, flow_detail = make_logbook(backend)
    flow = linear_flow.Flow("demo-flow", retry=retry.Times(2, "retrier")).add(
        ProgressingTask("work")
    )
    run_flow(backend, flow, book, flow_detail)

    source = PersistenceDataSource(backend)
    observed = source.get_flow(flow_detail.uuid)
    assert observed is not None
    assert observed.atoms["retrier"].atom_type == RETRY
    assert observed.atoms["work"].atom_type == TASK


# -- listing, filtering, paging ------------------------------------------


@pytest.fixture
def three_flows(backend: Any) -> list[str]:
    run_ids = []
    for index in range(3):
        book, flow_detail = make_logbook(
            backend,
            book_name=f"book-{index}",
            flow_name=f"flow-{index}",
        )
        flow = linear_flow.Flow(f"flow-{index}").add(
            ProgressingTask(f"task-{index}")
        )
        run_flow(backend, flow, book, flow_detail)
        run_ids.append(flow_detail.uuid)
    return run_ids


def test_every_flow_is_listed(
    source: PersistenceDataSource, three_flows: list[str]
) -> None:
    listed = {flow.run_id for flow in source.list_flows().items}
    assert listed == set(three_flows)


def test_filtering_by_state(
    source: PersistenceDataSource, three_flows: list[str]
) -> None:
    assert len(source.list_flows(state=states.SUCCESS).items) == 3
    assert source.list_flows(state=states.RUNNING).items == ()


def test_filtering_by_book(
    source: PersistenceDataSource, three_flows: list[str]
) -> None:
    first = source.list_flows().items[0]
    page = source.list_flows(book_id=first.book_id)
    assert [flow.run_id for flow in page.items] == [first.run_id]


def test_paging_walks_every_flow_exactly_once(
    source: PersistenceDataSource, three_flows: list[str]
) -> None:
    seen: list[str] = []
    marker: str | None = None
    while True:
        page = source.list_flows(limit=2, marker=marker)
        seen.extend(flow.run_id for flow in page.items)
        if not page.has_more:
            break
        marker = page.next_marker

    assert sorted(seen) == sorted(three_flows)


def test_the_owning_book_is_found_among_several(
    source: PersistenceDataSource, three_flows: list[str]
) -> None:
    # taskflow offers no way to go from a flow back to its book, so this
    # walks them until it finds the one that claims the flow.
    for run_id in three_flows:
        flow = source.get_flow(run_id)
        assert flow is not None
        assert flow.book_id is not None
        assert flow.book_name is not None


def test_an_unknown_marker_is_rejected(
    source: PersistenceDataSource, three_flows: list[str]
) -> None:
    with pytest.raises(UnknownMarkerError):
        source.list_flows(marker="not-a-run")


def test_a_non_positive_limit_is_rejected(
    source: PersistenceDataSource,
) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        source.list_flows(limit=0)


# -- events --------------------------------------------------------------


def test_no_event_history_is_offered(
    source: PersistenceDataSource, completed: Any
) -> None:
    # Persistence keeps current state, not a history.  Saying so beats
    # returning an empty stream that reads as silence.
    _book, flow_detail = completed
    assert source.supports_events is False
    page = source.events_since(flow_detail.uuid, since_seq=7)
    assert page.events == ()
    assert page.next_seq == 7


# -- tolerance -----------------------------------------------------------


class FakeDetail:
    """A duck-typed atom detail, for the paths real ones cannot reach."""

    def __init__(self, **kwargs: Any) -> None:
        self.name = "fake"
        self.uuid = "u-1"
        self.state = states.RUNNING
        self.intention = states.EXECUTE
        self.results = None
        self.failure = None
        self.meta: dict[str, Any] = {}
        self.__dict__.update(kwargs)


def test_an_unserialisable_failure_does_not_break_the_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Hostile:
        def to_dict(self) -> dict[str, Any]:
            msg = "nope"
            raise RuntimeError(msg)

        def __repr__(self) -> str:
            return "<hostile>"

    monkeypatch.setattr(
        "taskflow_meter.datasource.persistence.tf_models.atom_detail_type",
        lambda _detail: "TASK_DETAIL",
    )
    atom = _atom_snapshot(FakeDetail(failure=Hostile()))
    assert atom.failure == {"unserialisable": "<hostile>"}


def test_a_missing_progress_value_reads_as_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "taskflow_meter.datasource.persistence.tf_models.atom_detail_type",
        lambda _detail: "RETRY_DETAIL",
    )
    atom = _atom_snapshot(FakeDetail(meta={"progress": None}))
    assert atom.progress == 0.0
    assert atom.atom_type == RETRY


class FakeFlowDetail:
    """A flow detail with no atoms."""

    def __init__(self) -> None:
        self.uuid = "run-1"
        self.name = "orphan"
        self.state = states.RUNNING
        self.meta: dict[str, Any] = {}

    def __iter__(self) -> Any:
        return iter(())


class BooklessConnection:
    """A connection that reports no logbooks at all."""

    def get_logbooks(self, lazy: bool = False) -> Any:
        return iter(())


def test_a_flow_no_book_claims_is_still_readable(
    source: PersistenceDataSource,
) -> None:
    # Nothing in taskflow forbids it, and a monitor that raises on the
    # unexpected is worse than one that reports what it can see.
    assert source._find_book(BooklessConnection(), "run-1") == (
        None,
        None,
        None,
    )
    snapshot = source._flow_snapshot(FakeFlowDetail(), None, None, None)
    assert snapshot.book_id is None
    assert "book_created_at" not in snapshot.meta


def test_reading_without_a_backend_fails_loudly(backend: Any) -> None:
    # The constructor forbids this state; the guard is for anything that
    # reaches it another way.
    source = PersistenceDataSource(backend)
    source._backend = None
    with pytest.raises(RuntimeError, match="no backend"):
        source.list_flows()


@pytest.mark.parametrize("value", [None, "not-a-datetime", object()])
def test_an_unusable_timestamp_is_dropped_rather_than_raised(
    value: Any,
) -> None:
    assert _epoch(value) is None

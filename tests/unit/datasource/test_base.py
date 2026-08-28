"""The datasource contract, exercised through a minimal implementation."""

from __future__ import annotations

import pytest

from taskflow_meter.datasource.base import (
    DataSource,
    EventPage,
    FlowPage,
    UnknownMarkerError,
    WritableDataSource,
)
from taskflow_meter.events import Event, EventKind
from taskflow_meter.models import FlowSnapshot
from tests.conftest import make_atom, make_flow


class StubSource(WritableDataSource):
    """Just enough of a datasource to test what the base class provides."""

    name = "stub"

    def __init__(self, flow: FlowSnapshot | None = None) -> None:
        self.flow = flow
        self.applied: list[Event] = []
        self.starts = 0
        self.stops = 0

    def start(self) -> None:
        self.starts += 1

    def stop(self) -> None:
        self.stops += 1

    def list_flows(
        self,
        *,
        state: str | None = None,
        book_id: str | None = None,
        limit: int = 50,
        marker: str | None = None,
    ) -> FlowPage:
        return FlowPage()

    def get_flow(self, run_id: str) -> FlowSnapshot | None:
        if self.flow is not None and self.flow.run_id == run_id:
            return self.flow
        return None

    def events_since(
        self,
        run_id: str,
        *,
        since_seq: int = 0,
        limit: int = 500,
    ) -> EventPage:
        return EventPage(next_seq=since_seq)

    def apply(self, event: Event) -> None:
        self.applied.append(event)


def test_the_contract_cannot_be_instantiated_directly() -> None:
    with pytest.raises(TypeError):
        DataSource()  # type: ignore[abstract]


def test_get_atoms_defaults_to_reading_the_flow_in_name_order() -> None:
    source = StubSource(
        make_flow(atoms=(make_atom("zeta"), make_atom("alpha")))
    )
    atoms = source.get_atoms("run-1")
    assert atoms is not None
    assert [atom.name for atom in atoms] == ["alpha", "zeta"]


def test_get_atoms_is_none_for_a_flow_the_source_never_saw() -> None:
    assert StubSource().get_atoms("run-1") is None


def test_the_context_manager_starts_and_stops_once() -> None:
    source = StubSource()
    with source as entered:
        assert entered is source
        assert (source.starts, source.stops) == (1, 0)
    assert (source.starts, source.stops) == (1, 1)


def test_the_source_is_stopped_even_when_the_body_raises() -> None:
    source = StubSource()
    with pytest.raises(RuntimeError), source:
        raise RuntimeError("boom")
    assert source.stops == 1


def test_apply_many_preserves_order() -> None:
    source = StubSource()
    events = [
        Event(run_id="run-1", seq=seq, ts=0.0, kind=EventKind.HEARTBEAT)
        for seq in (1, 2, 3)
    ]
    source.apply_many(events)
    assert [event.seq for event in source.applied] == [1, 2, 3]


def test_the_default_hooks_do_nothing() -> None:
    # A source holding no resources need not override either.
    source = StubSource()
    DataSource.start(source)
    DataSource.stop(source)
    assert (source.starts, source.stops) == (0, 0)


def test_an_empty_flow_page_reports_no_more() -> None:
    assert not FlowPage().has_more
    assert FlowPage(next_marker="run-1").has_more


def test_an_empty_event_page_is_not_truncated() -> None:
    page = EventPage()
    assert page.events == ()
    assert page.oldest_seq is None
    assert not page.truncated


def test_an_unknown_marker_is_a_lookup_error() -> None:
    # So callers can catch it with the stdlib exception if they prefer.
    assert issubclass(UnknownMarkerError, LookupError)

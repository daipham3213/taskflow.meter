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

"""In-memory datasource: folding events back into state, and paging."""

from __future__ import annotations

import logging

import pytest

from taskflow_meter import states
from taskflow_meter.datasource import MemoryDataSource
from taskflow_meter.datasource import UnknownMarkerError
from taskflow_meter.diff import diff_flow
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.events import SequenceAllocator
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot
from tests.conftest import make_atom
from tests.conftest import make_flow


def essentials(flow: FlowSnapshot) -> dict[str, object]:
    """The parts of a snapshot an event stream is expected to carry.

    ``meta`` is deliberately excluded: events describe transitions, not
    arbitrary backend metadata, so a reconstruction cannot be held to it.
    """
    return {
        "run_id": flow.run_id,
        "name": flow.name,
        "state": flow.state,
        "book_id": flow.book_id,
        "book_name": flow.book_name,
        "atoms": {
            name: (
                atom.uuid,
                atom.atom_type,
                atom.state,
                atom.intention,
                atom.progress,
                atom.progress_details,
                atom.failure,
            )
            for name, atom in flow.atoms.items()
        },
    }


def test_unknown_run_reads_as_absent() -> None:
    source = MemoryDataSource()
    assert source.get_flow("nope") is None
    assert source.get_atoms("nope") is None
    assert source.events_since("nope").events == ()


def test_apply_builds_a_flow_from_its_first_event() -> None:
    source = MemoryDataSource()
    source.apply(
        Event(
            run_id="run-1",
            seq=1,
            ts=10.0,
            kind=EventKind.FLOW_STATE,
            book_id="book-1",
            state=states.RUNNING,
            details={"flow_name": "demo-flow", "book_name": "nightly"},
        )
    )
    flow = source.get_flow("run-1")
    assert flow is not None
    assert (flow.name, flow.state, flow.book_name) == (
        "demo-flow",
        states.RUNNING,
        "nightly",
    )


def test_joining_mid_stream_tolerates_missing_identity() -> None:
    source = MemoryDataSource()
    source.apply(
        Event(
            run_id="run-1",
            seq=57,
            ts=10.0,
            kind=EventKind.ATOM_STATE,
            atom_name="a",
            state=states.RUNNING,
        )
    )
    flow = source.get_flow("run-1")
    assert flow is not None
    assert flow.name == ""
    assert flow.atoms["a"].state == states.RUNNING


def test_atom_event_without_a_name_is_ignored_loudly(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = MemoryDataSource()
    logger = "taskflow_meter.datasource.memory"
    with caplog.at_level(logging.WARNING, logger=logger):
        source.apply(
            Event(
                run_id="run-1",
                seq=1,
                ts=1.0,
                kind=EventKind.ATOM_STATE,
                state=states.RUNNING,
            )
        )
    assert "no atom name" in caplog.text
    flow = source.get_flow("run-1")
    assert flow is not None
    assert flow.atoms == {}


def test_folding_a_diff_reproduces_the_snapshot_it_came_from() -> None:
    """The round trip the other datasources are measured against."""
    allocator = SequenceAllocator()
    source = MemoryDataSource()

    timeline = [
        make_flow(
            state=states.PENDING,
            book_id="book-1",
            book_name="nightly",
            observed_at=1.0,
            atoms=(
                make_atom("alpha", state=states.PENDING, uuid="u-a"),
                make_atom("beta", state=states.PENDING, uuid="u-b"),
            ),
        ),
        make_flow(
            state=states.RUNNING,
            book_id="book-1",
            book_name="nightly",
            observed_at=2.0,
            atoms=(
                make_atom(
                    "alpha",
                    state=states.RUNNING,
                    uuid="u-a",
                    progress=0.5,
                    intention=states.EXECUTE,
                    progress_details={"at_progress": 0.5},
                ),
                make_atom("beta", state=states.PENDING, uuid="u-b"),
            ),
        ),
        make_flow(
            state=states.SUCCESS,
            book_id="book-1",
            book_name="nightly",
            observed_at=3.0,
            atoms=(
                make_atom(
                    "alpha",
                    state=states.SUCCESS,
                    uuid="u-a",
                    progress=1.0,
                    intention=states.EXECUTE,
                    progress_details={"at_progress": 0.5},
                ),
                make_atom(
                    "beta",
                    state=states.SUCCESS,
                    uuid="u-b",
                    progress=1.0,
                    intention=states.EXECUTE,
                ),
            ),
        ),
    ]

    previous: FlowSnapshot | None = None
    for snapshot in timeline:
        source.apply_many(diff_flow(previous, snapshot, allocator=allocator))
        rebuilt = source.get_flow(snapshot.run_id)
        assert rebuilt is not None
        assert essentials(rebuilt) == essentials(snapshot)
        assert rebuilt.completion == pytest.approx(snapshot.completion)
        previous = snapshot


def test_failure_survives_the_round_trip() -> None:
    allocator = SequenceAllocator()
    source = MemoryDataSource()
    failure = {"exc_type_names": ["ValueError"]}

    running = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING),),
    )
    failed = make_flow(
        state=states.FAILURE,
        atoms=(make_atom("a", state=states.FAILURE, failure=failure),),
    )
    source.apply_many(diff_flow(None, running, allocator=allocator))
    source.apply_many(diff_flow(running, failed, allocator=allocator))

    rebuilt = source.get_flow("run-1")
    assert rebuilt is not None
    assert rebuilt.atoms["a"].failure == failure


def test_revert_failure_survives_the_round_trip() -> None:
    # A revert can fail on its own terms, separately from the failure that
    # triggered it, and both have to reach the API.
    allocator = SequenceAllocator()
    source = MemoryDataSource()
    failure = {"exc_type_names": ["ValueError"]}
    revert_failure = {"exc_type_names": ["OSError"]}

    reverting = make_flow(
        state=states.REVERTING,
        atoms=(make_atom("a", state=states.REVERTING, failure=failure),),
    )
    broken = make_flow(
        state=states.FAILURE,
        atoms=(
            make_atom(
                "a",
                state=states.REVERT_FAILURE,
                failure=failure,
                revert_failure=revert_failure,
            ),
        ),
    )
    source.apply_many(diff_flow(None, reverting, allocator=allocator))
    events = diff_flow(reverting, broken, allocator=allocator)
    source.apply_many(events)

    assert events[0].details == {"revert_failure": revert_failure}
    rebuilt = source.get_flow("run-1")
    assert rebuilt is not None
    assert rebuilt.atoms["a"].failure == failure
    assert rebuilt.atoms["a"].revert_failure == revert_failure


def test_result_availability_survives_the_round_trip() -> None:
    allocator = SequenceAllocator()
    source = MemoryDataSource()
    done = make_flow(
        state=states.SUCCESS,
        atoms=(make_atom("a", state=states.SUCCESS, has_result=True),),
    )
    source.apply_many(diff_flow(None, done, allocator=allocator))

    rebuilt = source.get_flow("run-1")
    assert rebuilt is not None
    assert rebuilt.atoms["a"].has_result


def test_get_atoms_returns_them_in_name_order() -> None:
    source = MemoryDataSource()
    allocator = SequenceAllocator()
    flow = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("zeta"), make_atom("alpha")),
    )
    source.apply_many(diff_flow(None, flow, allocator=allocator))

    atoms = source.get_atoms("run-1")
    assert atoms is not None
    assert [atom.name for atom in atoms] == ["alpha", "zeta"]


# -- event history ------------------------------------------------------


def seed_events(source: MemoryDataSource, count: int) -> None:
    for seq in range(1, count + 1):
        source.apply(
            Event(
                run_id="run-1",
                seq=seq,
                ts=float(seq),
                kind=EventKind.HEARTBEAT,
            )
        )


def test_events_since_returns_only_newer_events() -> None:
    source = MemoryDataSource()
    seed_events(source, 5)
    page = source.events_since("run-1", since_seq=3)
    assert [event.seq for event in page.events] == [4, 5]
    assert page.next_seq == 5
    assert not page.truncated


def test_events_since_respects_the_limit() -> None:
    source = MemoryDataSource()
    seed_events(source, 10)
    page = source.events_since("run-1", since_seq=0, limit=3)
    assert [event.seq for event in page.events] == [1, 2, 3]
    assert page.next_seq == 3


def test_an_exhausted_stream_reports_the_caller_position() -> None:
    source = MemoryDataSource()
    seed_events(source, 2)
    page = source.events_since("run-1", since_seq=2)
    assert page.events == ()
    assert page.next_seq == 2


def test_events_arriving_out_of_order_are_returned_in_order() -> None:
    # Concurrent producers do not arrive in sequence order: two threads
    # each allocate a number and then enqueue, and the enqueues invert.
    source = MemoryDataSource()
    for seq in (1, 3, 2):
        source.apply(
            Event(
                run_id="run-1",
                seq=seq,
                ts=float(seq),
                kind=EventKind.HEARTBEAT,
            )
        )
    page = source.events_since("run-1")
    assert [event.seq for event in page.events] == [1, 2, 3]


def test_events_beyond_a_gap_are_held_back() -> None:
    # Returning 3 while 2 is still in flight would advance the caller
    # past 2 for good.
    source = MemoryDataSource()
    for seq in (1, 3):
        source.apply(
            Event(
                run_id="run-1",
                seq=seq,
                ts=float(seq),
                kind=EventKind.HEARTBEAT,
            )
        )
    page = source.events_since("run-1")
    assert [event.seq for event in page.events] == [1]
    assert page.next_seq == 1

    source.apply(
        Event(run_id="run-1", seq=2, ts=2.0, kind=EventKind.HEARTBEAT)
    )
    page = source.events_since("run-1", since_seq=1)
    assert [event.seq for event in page.events] == [2, 3]


def test_eviction_is_reported_rather_than_hidden() -> None:
    # A caller that misses the window has a hole in its stream and needs
    # to know, or it will render a flow that silently skipped a state.
    source = MemoryDataSource(max_events_per_run=3)
    seed_events(source, 6)

    page = source.events_since("run-1", since_seq=0)
    assert [event.seq for event in page.events] == [4, 5, 6]
    assert page.oldest_seq == 4
    assert page.truncated


def test_a_caller_inside_the_window_is_not_told_it_was_truncated() -> None:
    source = MemoryDataSource(max_events_per_run=3)
    seed_events(source, 6)
    assert not source.events_since("run-1", since_seq=3).truncated


@pytest.mark.parametrize("limit", [0, -1])
def test_non_positive_limits_are_rejected(limit: int) -> None:
    source = MemoryDataSource()
    with pytest.raises(ValueError, match="at least 1"):
        source.events_since("run-1", limit=limit)
    with pytest.raises(ValueError, match="at least 1"):
        source.list_flows(limit=limit)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_events_per_run": 0}, "max_events_per_run"),
        ({"max_runs": 0}, "max_runs"),
    ],
)
def test_construction_rejects_useless_bounds(
    kwargs: dict[str, int], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        MemoryDataSource(**kwargs)


# -- listing and paging -------------------------------------------------


def populate(source: MemoryDataSource) -> None:
    for index, (run_id, state, book) in enumerate(
        [
            ("run-a", states.SUCCESS, "book-1"),
            ("run-b", states.RUNNING, "book-1"),
            ("run-c", states.RUNNING, "book-2"),
        ]
    ):
        source.apply(
            Event(
                run_id=run_id,
                seq=1,
                ts=float(index),
                kind=EventKind.FLOW_STATE,
                book_id=book,
                state=state,
            )
        )


def test_listing_is_newest_observation_first() -> None:
    source = MemoryDataSource()
    populate(source)
    page = source.list_flows()
    assert [flow.run_id for flow in page.items] == [
        "run-c",
        "run-b",
        "run-a",
    ]
    assert not page.has_more


@pytest.mark.parametrize(
    ("state", "book_id", "expected"),
    [
        (states.RUNNING, None, ["run-c", "run-b"]),
        (None, "book-1", ["run-b", "run-a"]),
        (states.RUNNING, "book-1", ["run-b"]),
        (states.PENDING, None, []),
    ],
)
def test_listing_filters(
    state: str | None, book_id: str | None, expected: list[str]
) -> None:
    source = MemoryDataSource()
    populate(source)
    page = source.list_flows(state=state, book_id=book_id)
    assert [flow.run_id for flow in page.items] == expected


def test_paging_walks_every_flow_exactly_once() -> None:
    source = MemoryDataSource()
    populate(source)

    seen: list[str] = []
    marker: str | None = None
    while True:
        page = source.list_flows(limit=2, marker=marker)
        seen.extend(flow.run_id for flow in page.items)
        if not page.has_more:
            break
        marker = page.next_marker

    assert seen == ["run-c", "run-b", "run-a"]


def test_an_expired_marker_is_an_error_not_a_silent_restart() -> None:
    source = MemoryDataSource()
    populate(source)
    with pytest.raises(UnknownMarkerError):
        source.list_flows(marker="run-gone")


def test_flows_observed_at_the_same_instant_page_deterministically() -> None:
    source = MemoryDataSource()
    for run_id in ("run-b", "run-a", "run-c"):
        source.apply(
            Event(
                run_id=run_id,
                seq=1,
                ts=5.0,
                kind=EventKind.FLOW_STATE,
                state=states.RUNNING,
            )
        )
    first = source.list_flows(limit=2)
    second = source.list_flows(limit=2, marker=first.next_marker)
    assert [flow.run_id for flow in first.items] == ["run-a", "run-b"]
    assert [flow.run_id for flow in second.items] == ["run-c"]


# -- lifecycle ----------------------------------------------------------


def test_forget_drops_a_run() -> None:
    source = MemoryDataSource()
    populate(source)
    assert source.forget("run-a")
    assert not source.forget("run-a")
    assert source.get_flow("run-a") is None


def test_run_eviction_warns(caplog: pytest.LogCaptureFixture) -> None:
    source = MemoryDataSource(max_runs=2)
    logger = "taskflow_meter.datasource.memory"
    with caplog.at_level(logging.WARNING, logger=logger):
        populate(source)
    assert "evicted run" in caplog.text
    # The oldest observation goes first.
    assert source.get_flow("run-a") is None
    assert source.get_flow("run-c") is not None


def test_context_manager_starts_and_stops() -> None:
    with MemoryDataSource() as source:
        assert isinstance(source, MemoryDataSource)


def test_a_reader_cannot_mutate_what_the_datasource_believes() -> None:
    # FlowSnapshot is frozen but its atoms mapping is not, so a read has
    # to hand back a copy.
    source = MemoryDataSource()
    populate(source)

    flow = source.get_flow("run-a")
    assert flow is not None
    flow.atoms["injected"] = AtomSnapshot(name="injected")

    refetched = source.get_flow("run-a")
    assert refetched is not None
    assert "injected" not in refetched.atoms
    assert "injected" not in source.list_flows().items[-1].atoms

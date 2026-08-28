"""The diff engine: snapshot pair in, event stream out.

This is the module the read-only producer is built on, so the cases below
try to pin down every rule the docstring in ``taskflow_meter.diff`` claims.
"""

from __future__ import annotations

import logging

import pytest

from taskflow_meter import states
from taskflow_meter.diff import diff_flow
from taskflow_meter.events import EventKind, SequenceAllocator
from tests.conftest import make_atom, make_flow


def test_no_change_produces_nothing_and_burns_no_sequence_numbers(
    allocator: SequenceAllocator,
) -> None:
    flow = make_flow(
        state=states.RUNNING, atoms=(make_atom("a", state=states.RUNNING),)
    )
    assert diff_flow(flow, flow, allocator=allocator) == []
    # A quiet poll must not advance the counter, or a client's since_seq
    # would drift past events that never existed.
    assert allocator.peek(flow.run_id) == 0


def test_first_observation_describes_everything(
    allocator: SequenceAllocator,
) -> None:
    flow = make_flow(
        state=states.RUNNING,
        book_id="book-1",
        book_name="nightly",
        atoms=(
            make_atom("a", state=states.SUCCESS, progress=1.0),
            make_atom("b", state=states.RUNNING, progress=0.25),
        ),
    )
    events = diff_flow(None, flow, allocator=allocator)

    assert [event.kind for event in events] == [
        EventKind.FLOW_STATE,
        EventKind.ATOM_STATE,
        EventKind.ATOM_PROGRESS,
        EventKind.ATOM_STATE,
        EventKind.ATOM_PROGRESS,
    ]
    assert events[0].old_state is None
    assert all(event.book_id == "book-1" for event in events)


def test_first_observation_carries_flow_identity(
    allocator: SequenceAllocator,
) -> None:
    # Later events omit these, so a datasource fed only the event stream
    # has exactly one chance to learn what the run is called.
    flow = make_flow(state=states.RUNNING, book_name="nightly")
    (event,) = diff_flow(None, flow, allocator=allocator)
    assert event.details == {
        "flow_name": "demo-flow",
        "book_name": "nightly",
    }


def test_later_events_do_not_repeat_identity(
    allocator: SequenceAllocator,
) -> None:
    old = make_flow(state=states.PENDING)
    new = make_flow(state=states.RUNNING)
    (event,) = diff_flow(old, new, allocator=allocator)
    assert event.details == {}


def test_flow_state_transition(allocator: SequenceAllocator) -> None:
    old = make_flow(state=states.PENDING)
    new = make_flow(state=states.RUNNING)
    (event,) = diff_flow(old, new, allocator=allocator)
    assert event.kind is EventKind.FLOW_STATE
    assert (event.old_state, event.state) == (states.PENDING, states.RUNNING)


def test_atom_state_transition_carries_old_state(
    allocator: SequenceAllocator,
) -> None:
    old = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.PENDING),),
    )
    new = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, uuid="u-1"),),
    )
    (event,) = diff_flow(old, new, allocator=allocator)
    assert event.kind is EventKind.ATOM_STATE
    assert event.atom_name == "a"
    assert event.atom_uuid == "u-1"
    assert (event.old_state, event.state) == (states.PENDING, states.RUNNING)


def test_atom_appearing_mid_run_has_no_old_state(
    allocator: SequenceAllocator,
) -> None:
    # A retry controller can add atoms to a running flow.
    old = make_flow(state=states.RUNNING, atoms=(make_atom("a"),))
    new = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a"), make_atom("b", state=states.PENDING)),
    )
    (event,) = diff_flow(old, new, allocator=allocator)
    assert (event.atom_name, event.old_state) == ("b", None)


def test_progress_only_change(allocator: SequenceAllocator) -> None:
    old = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, progress=0.1),),
    )
    new = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, progress=0.6),),
    )
    (event,) = diff_flow(old, new, allocator=allocator)
    assert event.kind is EventKind.ATOM_PROGRESS
    assert event.progress == pytest.approx(0.6)


def test_progress_details_change_at_the_same_value(
    allocator: SequenceAllocator,
) -> None:
    # taskflow lets a task attach details without moving the number.
    old = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
    )
    new = make_flow(
        state=states.RUNNING,
        atoms=(
            make_atom(
                "a",
                state=states.RUNNING,
                progress=0.5,
                progress_details={"at_progress": 0.5, "details": {"n": 3}},
            ),
        ),
    )
    (event,) = diff_flow(old, new, allocator=allocator)
    assert event.kind is EventKind.ATOM_PROGRESS
    assert event.details["progress_details"]["details"] == {"n": 3}


def test_new_atom_at_zero_progress_yields_only_a_state_event(
    allocator: SequenceAllocator,
) -> None:
    new = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.PENDING, progress=0.0),),
    )
    events = diff_flow(None, new, allocator=allocator)
    assert [event.kind for event in events] == [
        EventKind.FLOW_STATE,
        EventKind.ATOM_STATE,
    ]


def test_state_event_precedes_progress_event_for_one_atom(
    allocator: SequenceAllocator,
) -> None:
    old = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.PENDING),),
    )
    new = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, progress=0.3),),
    )
    events = diff_flow(old, new, allocator=allocator)
    assert [event.kind for event in events] == [
        EventKind.ATOM_STATE,
        EventKind.ATOM_PROGRESS,
    ]
    assert [event.seq for event in events] == [1, 2]


def test_flow_event_comes_first_while_the_flow_is_still_going(
    allocator: SequenceAllocator,
) -> None:
    old = make_flow(
        state=states.PENDING, atoms=(make_atom("a", state=states.PENDING),)
    )
    new = make_flow(
        state=states.RUNNING, atoms=(make_atom("a", state=states.RUNNING),)
    )
    events = diff_flow(old, new, allocator=allocator)
    assert [event.kind for event in events] == [
        EventKind.FLOW_STATE,
        EventKind.ATOM_STATE,
    ]


@pytest.mark.parametrize(
    "final_state",
    [states.SUCCESS, states.FAILURE, states.REVERTED, states.SUSPENDED],
)
def test_flow_event_comes_last_once_the_flow_has_finished(
    allocator: SequenceAllocator, final_state: str
) -> None:
    # A flow does not reach a finish state before its atoms do, so
    # reporting it first would show a completed flow with a running atom.
    old = make_flow(
        state=states.RUNNING, atoms=(make_atom("a", state=states.RUNNING),)
    )
    new = make_flow(
        state=final_state, atoms=(make_atom("a", state=states.SUCCESS),)
    )
    events = diff_flow(old, new, allocator=allocator)
    assert [event.kind for event in events] == [
        EventKind.ATOM_STATE,
        EventKind.FLOW_STATE,
    ]
    assert events[-1].state == final_state


def test_atoms_are_visited_in_name_order(
    allocator: SequenceAllocator,
) -> None:
    new = make_flow(
        state=states.RUNNING,
        atoms=(
            make_atom("zeta", state=states.PENDING),
            make_atom("alpha", state=states.PENDING),
            make_atom("mid", state=states.PENDING),
        ),
    )
    events = diff_flow(None, new, allocator=allocator)
    assert [event.atom_name for event in events[1:]] == [
        "alpha",
        "mid",
        "zeta",
    ]


def test_sequence_numbers_are_gap_free_across_calls(
    allocator: SequenceAllocator,
) -> None:
    first = make_flow(state=states.PENDING)
    second = make_flow(state=states.RUNNING)
    third = make_flow(state=states.SUCCESS)

    seqs = [
        event.seq
        for snapshot_pair in ((None, first), (first, second), (second, third))
        for event in diff_flow(*snapshot_pair, allocator=allocator)
    ]
    assert seqs == [1, 2, 3]


def test_sequence_numbers_are_independent_per_run(
    allocator: SequenceAllocator,
) -> None:
    one = make_flow("run-1", state=states.RUNNING)
    two = make_flow("run-2", state=states.RUNNING)
    assert diff_flow(None, one, allocator=allocator)[0].seq == 1
    assert diff_flow(None, two, allocator=allocator)[0].seq == 1


def test_timestamp_defaults_to_the_observation_time(
    allocator: SequenceAllocator,
) -> None:
    new = make_flow(state=states.RUNNING, observed_at=1234.5)
    (event,) = diff_flow(None, new, allocator=allocator)
    assert event.ts == pytest.approx(1234.5)


def test_timestamp_can_be_overridden(
    allocator: SequenceAllocator,
) -> None:
    new = make_flow(state=states.RUNNING, observed_at=1234.5)
    (event,) = diff_flow(None, new, allocator=allocator, ts=42.0)
    assert event.ts == pytest.approx(42.0)


def test_diffing_across_runs_is_rejected(
    allocator: SequenceAllocator,
) -> None:
    with pytest.raises(ValueError, match="cannot diff across runs"):
        diff_flow(make_flow("run-1"), make_flow("run-2"), allocator=allocator)


def test_failure_is_carried_on_the_state_event(
    allocator: SequenceAllocator,
) -> None:
    failure = {"exc_type_names": ["ValueError"], "exception_str": "boom"}
    old = make_flow(
        state=states.RUNNING, atoms=(make_atom("a", state=states.RUNNING),)
    )
    new = make_flow(
        state=states.FAILURE,
        atoms=(make_atom("a", state=states.FAILURE, failure=failure),),
    )
    events = diff_flow(old, new, allocator=allocator)
    assert events[0].details["failure"] == failure


def test_result_availability_is_flagged_not_carried(
    allocator: SequenceAllocator,
) -> None:
    # Results can be arbitrarily large and are not ours to copy around.
    old = make_flow(
        state=states.RUNNING, atoms=(make_atom("a", state=states.RUNNING),)
    )
    new = make_flow(
        state=states.SUCCESS,
        atoms=(make_atom("a", state=states.SUCCESS, has_result=True),),
    )
    events = diff_flow(old, new, allocator=allocator)
    assert events[0].details == {"has_result": True}


def test_a_full_revert_cycle(allocator: SequenceAllocator) -> None:
    running = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
    )
    reverting = make_flow(
        state=states.REVERTING,
        atoms=(make_atom("a", state=states.REVERTING, progress=0.0),),
    )
    reverted = make_flow(
        state=states.REVERTED,
        atoms=(make_atom("a", state=states.REVERTED, progress=1.0),),
    )

    first = diff_flow(running, reverting, allocator=allocator)
    assert [event.kind for event in first] == [
        EventKind.FLOW_STATE,
        EventKind.ATOM_STATE,
        EventKind.ATOM_PROGRESS,
    ]
    assert first[2].progress == pytest.approx(0.0)

    second = diff_flow(reverting, reverted, allocator=allocator)
    # Finished flow, so the atom is reported before the flow.
    assert [event.kind for event in second] == [
        EventKind.ATOM_STATE,
        EventKind.ATOM_PROGRESS,
        EventKind.FLOW_STATE,
    ]
    # Progress back at 1.0 means the revert finished, and the completion
    # arithmetic in the model is what stops that reading as done work.
    assert reverted.completion == 0.0


def test_a_vanishing_atom_is_reported_not_invented(
    allocator: SequenceAllocator, caplog: pytest.LogCaptureFixture
) -> None:
    old = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a"), make_atom("b")),
    )
    new = make_flow(state=states.RUNNING, atoms=(make_atom("a"),))

    with caplog.at_level(logging.WARNING, logger="taskflow_meter.diff"):
        assert diff_flow(old, new, allocator=allocator) == []

    assert "disappeared" in caplog.text
    assert "'b'" in caplog.text

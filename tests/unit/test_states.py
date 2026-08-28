"""State groupings, checked against taskflow's own vocabulary."""

from __future__ import annotations

from taskflow import states as tf_states

from taskflow_meter import states


def test_values_come_from_taskflow_not_from_string_literals() -> None:
    # The point of the module is to avoid drift if upstream renames one.
    assert states.SUCCESS is tf_states.SUCCESS
    assert states.REVERT_FAILURE is tf_states.REVERT_FAILURE
    assert states.EXECUTE is tf_states.EXECUTE


def test_every_grouped_state_is_a_real_taskflow_state() -> None:
    known = {
        value
        for name, value in vars(tf_states).items()
        if not name.startswith("_") and isinstance(value, str)
    }
    grouped = (
        states.FLOW_FINISH_STATES
        | states.ATOM_FINISH_STATES
        | states.ATOM_RUNNING_STATES
        | states.ATOM_COMPLETE_STATES
    )
    assert grouped <= known


def test_finished_atoms_are_never_also_running() -> None:
    assert not states.ATOM_FINISH_STATES & states.ATOM_RUNNING_STATES


def test_reverted_finishes_an_atom_without_completing_its_work() -> None:
    # The distinction the completion arithmetic depends on.
    assert states.REVERTED in states.ATOM_FINISH_STATES
    assert states.REVERTED not in states.ATOM_COMPLETE_STATES


def test_failure_states_never_count_as_complete() -> None:
    failures = {states.FAILURE, states.REVERT_FAILURE}
    assert not failures & states.ATOM_COMPLETE_STATES


def test_a_suspended_flow_counts_as_finished() -> None:
    # It has stopped and will emit nothing further until resumed.
    assert states.SUSPENDED in states.FLOW_FINISH_STATES
    assert states.RUNNING not in states.FLOW_FINISH_STATES

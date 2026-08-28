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

"""Snapshot model behaviour, especially the completion arithmetic."""

from __future__ import annotations

import pytest

from taskflow_meter import states
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot
from tests.conftest import make_atom
from tests.conftest import make_flow


@pytest.mark.parametrize(
    ("state", "progress", "expected"),
    [
        (states.SUCCESS, 1.0, 1.0),
        # Ruled out by a decider: it will never run, so counting it as
        # outstanding would strand the flow below 100% forever.
        (states.IGNORE, 0.0, 1.0),
        (states.RUNNING, 0.25, 0.25),
        (states.REVERTING, 0.5, 0.5),
        (states.PENDING, 0.0, 0.0),
        # taskflow sets progress to 1.0 when a revert finishes; that says
        # the revert completed, not the work.
        (states.REVERTED, 1.0, 0.0),
        # taskflow leaves progress stale on failure.
        (states.FAILURE, 0.7, 0.0),
        (states.REVERT_FAILURE, 0.7, 0.0),
    ],
)
def test_atom_completion(state: str, progress: float, expected: float) -> None:
    atom = make_atom("t", state=state, progress=progress)
    assert atom.completion == pytest.approx(expected)


@pytest.mark.parametrize(
    ("progress", "expected"), [(-0.5, 0.0), (1.7, 1.0), (0.4, 0.4)]
)
def test_running_progress_is_clamped(progress: float, expected: float) -> None:
    atom = make_atom("t", state=states.RUNNING, progress=progress)
    assert atom.completion == pytest.approx(expected)


def test_atom_finished_and_running_predicates() -> None:
    assert make_atom("t", state=states.SUCCESS).is_finished
    assert not make_atom("t", state=states.RUNNING).is_finished
    assert make_atom("t", state=states.REVERTING).is_running
    assert not make_atom("t", state=states.PENDING).is_running


def test_flow_completion_is_the_mean_of_its_atoms() -> None:
    flow = make_flow(
        atoms=(
            make_atom("a", state=states.SUCCESS),
            make_atom("b", state=states.RUNNING, progress=0.5),
            make_atom("c", state=states.PENDING),
        )
    )
    assert flow.completion == pytest.approx(0.5)


def test_empty_flow_completion_is_zero() -> None:
    assert make_flow().completion == 0.0


def test_atom_names_are_sorted() -> None:
    flow = make_flow(
        atoms=(make_atom("zeta"), make_atom("alpha"), make_atom("mid"))
    )
    assert flow.atom_names == ("alpha", "mid", "zeta")


def test_state_counts_labels_unknown_states() -> None:
    flow = make_flow(
        atoms=(
            make_atom("a", state=states.SUCCESS),
            make_atom("b", state=states.SUCCESS),
            make_atom("c"),
        )
    )
    assert flow.state_counts == {states.SUCCESS: 2, "UNKNOWN": 1}


def test_atom_lookup_returns_none_when_absent() -> None:
    flow = make_flow(atoms=(make_atom("a"),))
    assert flow.atom("a") is not None
    assert flow.atom("nope") is None


def test_flow_is_finished_for_terminal_states() -> None:
    assert make_flow(state=states.SUCCESS).is_finished
    assert make_flow(state=states.SUSPENDED).is_finished
    assert not make_flow(state=states.RUNNING).is_finished


def test_snapshots_are_immutable() -> None:
    with pytest.raises(AttributeError):
        make_flow().state = states.SUCCESS  # type: ignore[misc]
    with pytest.raises(AttributeError):
        make_atom("a").progress = 1.0  # type: ignore[misc]


def test_snapshots_do_not_share_mutable_defaults() -> None:
    first, second = FlowSnapshot(run_id="a"), FlowSnapshot(run_id="b")
    first.atoms["x"] = AtomSnapshot(name="x")
    assert second.atoms == {}


def test_running_atoms_answers_what_it_is_doing_now() -> None:
    flow = make_flow(
        atoms=(
            make_atom("done", state=states.SUCCESS),
            make_atom("zeta", state=states.RUNNING),
            make_atom("alpha", state=states.RUNNING),
            make_atom("waiting", state=states.PENDING),
        )
    )
    # Plural and ordered: parallel flows run several at once.
    assert [atom.name for atom in flow.running_atoms] == ["alpha", "zeta"]


def test_a_reverting_atom_counts_as_running() -> None:
    # It is what the flow is doing, even though it is undoing.
    flow = make_flow(atoms=(make_atom("a", state=states.REVERTING),))
    assert [atom.name for atom in flow.running_atoms] == ["a"]


def test_nothing_is_running_between_atoms_or_after_the_end() -> None:
    finished = make_flow(
        state=states.SUCCESS,
        atoms=(make_atom("a", state=states.SUCCESS),),
    )
    assert finished.running_atoms == ()
    assert make_flow().running_atoms == ()

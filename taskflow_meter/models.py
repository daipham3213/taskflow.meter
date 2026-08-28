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

"""Point-in-time view of a flow and its atoms.

These are the types the API serialises and the diff engine compares.  They
are deliberately plain: adapters translate taskflow's persistence models or
notifier callbacks *into* these, so nothing downstream needs to know which
producer it is talking to.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any

from taskflow_meter import states

TASK = "task"
RETRY = "retry"


@dataclass(frozen=True, slots=True)
class AtomSnapshot:
    """What is known about a single atom at one moment."""

    name: str
    uuid: str | None = None
    atom_type: str = TASK
    state: str | None = None
    intention: str | None = None
    progress: float = 0.0
    progress_details: dict[str, Any] | None = None
    failure: dict[str, Any] | None = None
    revert_failure: dict[str, Any] | None = None
    has_result: bool = False
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def is_finished(self) -> bool:
        return self.state in states.ATOM_FINISH_STATES

    @property
    def is_running(self) -> bool:
        return self.state in states.ATOM_RUNNING_STATES

    @property
    def completion(self) -> float:
        """This atom's contribution to flow completion, in ``[0, 1]``.

        Reported progress is only trusted while the atom is running.  A
        finished atom's stored progress cannot be taken at face value:
        taskflow sets it to 1.0 on both ``SUCCESS`` and ``REVERTED``, and
        leaves it untouched on ``FAILURE``, so it would otherwise report a
        reverted or failed atom as complete.
        """
        if self.state in states.ATOM_COMPLETE_STATES:
            return 1.0
        if self.is_running:
            return _clamp(self.progress)
        return 0.0


@dataclass(frozen=True, slots=True)
class FlowSnapshot:
    """What is known about one flow run at one moment.

    ``observed_at`` is when *we* looked, not when the flow actually changed:
    taskflow records no timestamps below the logbook, so nothing else is
    available.  For the live listener that is close enough to be the same
    thing; for the poller it is only accurate to the poll interval.
    """

    run_id: str
    name: str = ""
    state: str | None = None
    book_id: str | None = None
    book_name: str | None = None
    observed_at: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)
    atoms: dict[str, AtomSnapshot] = field(default_factory=dict)

    @property
    def is_finished(self) -> bool:
        return self.state in states.FLOW_FINISH_STATES

    @property
    def atom_names(self) -> tuple[str, ...]:
        """Atom names in a stable order, so output never reshuffles."""
        return tuple(sorted(self.atoms))

    def atom(self, name: str) -> AtomSnapshot | None:
        return self.atoms.get(name)

    @property
    def running_atoms(self) -> tuple[AtomSnapshot, ...]:
        """The atoms executing right now, in name order.

        The answer to "what is it doing?".  A plural, because taskflow
        runs unordered and graph flows in parallel, so there is often
        more than one -- and none at all between two atoms, or once the
        flow has finished.
        """
        return tuple(
            self.atoms[name]
            for name in self.atom_names
            if self.atoms[name].is_running
        )

    @property
    def state_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for atom in self.atoms.values():
            key = atom.state if atom.state is not None else "UNKNOWN"
            counts[key] = counts.get(key, 0) + 1
        return counts

    @property
    def completion(self) -> float:
        """Unweighted mean of the atoms' completion, in ``[0, 1]``.

        Every atom counts the same because taskflow gives us nothing to
        weight them by -- no durations, no cost hints.  Treat it as a rough
        indicator, not an estimate of time remaining.
        """
        if not self.atoms:
            return 0.0
        total = sum(atom.completion for atom in self.atoms.values())
        return total / len(self.atoms)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))

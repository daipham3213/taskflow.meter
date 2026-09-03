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

"""Folding events back into a snapshot.

Shared by every writable datasource, so they cannot disagree about what
an event means.  The property this exists to keep true: applying the
events a diff produced reproduces the snapshots the diff came from --
whichever store is doing the applying.
"""

from __future__ import annotations

import logging
from typing import Any

from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot

LOG = logging.getLogger(__name__)


def flow_from_event(event: Event) -> FlowSnapshot:
    """Seed a run from the first event seen for it.

    The diff engine puts the flow's identity on the first event of a
    run, but a client may join mid-stream, so everything here has to
    tolerate absence.
    """
    details = event.details
    return FlowSnapshot(
        run_id=event.run_id,
        name=str(details.get("flow_name", "")),
        book_id=event.book_id,
        book_name=_optional_str(details.get("book_name")),
        observed_at=event.ts,
    )


def fold(flow: FlowSnapshot, event: Event) -> FlowSnapshot:
    """Return ``flow`` updated by ``event``."""
    from dataclasses import replace

    flow = replace(flow, observed_at=max(flow.observed_at, event.ts))

    if event.kind is EventKind.FLOW_STATE:
        # The name arrives on these events too, and has to be taken from
        # one of them when the run was seeded by something else: the
        # in-process producer emits the graph before anything runs, and a
        # structure event carries no name to seed from.
        name = _optional_str(event.details.get("flow_name")) or flow.name
        return replace(flow, state=event.state, name=name)

    if event.kind in (EventKind.ATOM_STATE, EventKind.ATOM_PROGRESS):
        if event.atom_name is None:
            LOG.warning(
                "ignoring %s event %d for run %s: no atom name",
                event.kind,
                event.seq,
                event.run_id,
            )
            return flow
        atoms = dict(flow.atoms)
        atoms[event.atom_name] = fold_atom(
            atoms.get(event.atom_name), event, event.atom_name
        )
        return replace(flow, atoms=atoms)

    return flow


def fold_atom(
    atom: AtomSnapshot | None, event: Event, name: str
) -> AtomSnapshot:
    from dataclasses import replace

    if atom is None:
        atom = AtomSnapshot(name=name)

    changes: dict[str, Any] = {}
    if event.atom_uuid is not None:
        changes["uuid"] = event.atom_uuid
    if event.atom_type is not None:
        changes["atom_type"] = event.atom_type
    if event.intention is not None:
        changes["intention"] = event.intention
    if event.progress is not None:
        changes["progress"] = event.progress

    if event.kind is EventKind.ATOM_STATE:
        changes["state"] = event.state
        failure = event.details.get("failure")
        if failure is not None:
            changes["failure"] = failure
        revert_failure = event.details.get("revert_failure")
        if revert_failure is not None:
            changes["revert_failure"] = revert_failure
        if event.details.get("has_result"):
            changes["has_result"] = True

    if event.kind is EventKind.ATOM_PROGRESS:
        changes["progress_details"] = event.details.get("progress_details")

    return replace(atom, **changes)


def contiguous_from(
    events: list[Event], expected: int, limit: int
) -> list[Event]:
    """Take the unbroken run of events starting at ``expected``.

    Concurrent producers do not arrive in sequence order: two threads
    each allocate a number and then hand it on, and the hand-offs can
    invert.  Returning event 8 while 7 is still in flight would advance
    a caller past 7 for good, so anything beyond a gap waits.
    """
    taken: list[Event] = []
    for event in events:
        if event.seq < expected:
            continue
        if event.seq != expected:
            break
        taken.append(event)
        expected += 1
        if len(taken) >= limit:
            break
    return taken


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)

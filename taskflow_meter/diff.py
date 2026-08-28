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

"""Turn a pair of snapshots into the events that explain the difference.

This is how the read-only producer works: poll the flow, compare against
what was seen last time, and synthesise exactly the events the in-process
listener would have emitted.  One event vocabulary, two producers.

What it cannot recover is ordering *within* a pair of snapshots.  If two
atoms changed between polls, we did not see which moved first, so the rules
below are chosen to be deterministic and defensible rather than to guess:

* atoms are visited in name order, so replaying the same pair of snapshots
  always produces the same events in the same order;
* an atom's state event precedes its progress event, because a state change
  is the coarser fact;
* the flow's own event comes first when the flow is starting or continuing,
  and last when the flow has reached a finish state -- a flow does not
  finish before its atoms do.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import replace

from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.events import SequenceAllocator
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot

LOG = logging.getLogger(__name__)

_UNSTAMPED_SEQ = 0
_UNSTAMPED_TS = 0.0


def diff_flow(
    old: FlowSnapshot | None,
    new: FlowSnapshot,
    *,
    allocator: SequenceAllocator,
    ts: float | None = None,
) -> list[Event]:
    """Return the events describing ``old`` -> ``new``.

    ``old`` is ``None`` for a run's first observation, which yields one
    event per known fact so that a client joining late still receives a
    complete picture rather than only future changes.

    Sequence numbers come from ``allocator`` in emission order.  Nothing is
    allocated when there is nothing to report, so a quiet poll costs no
    sequence numbers and a client's ``since_seq`` stays meaningful.
    """
    if old is not None and old.run_id != new.run_id:
        msg = f"cannot diff across runs: {old.run_id!r} -> {new.run_id!r}"
        raise ValueError(msg)

    stamp = new.observed_at if ts is None else ts

    flow_events = list(_flow_events(old, new))
    atom_events = list(_atom_events(old, new))

    ordered = (
        atom_events + flow_events
        if new.is_finished
        else flow_events + atom_events
    )
    return [
        replace(event, seq=allocator.allocate(new.run_id), ts=stamp)
        for event in ordered
    ]


def _flow_events(
    old: FlowSnapshot | None, new: FlowSnapshot
) -> Iterator[Event]:
    if old is not None and old.state == new.state:
        return

    details: dict[str, object] = {}
    if old is None:
        # First sight of this run: carry the identity that later events
        # will not repeat, so a datasource can reconstruct the flow from
        # the event stream alone.
        details["flow_name"] = new.name
        if new.book_name is not None:
            details["book_name"] = new.book_name

    yield _new_event(
        new,
        kind=EventKind.FLOW_STATE,
        state=new.state,
        old_state=old.state if old is not None else None,
        details=details,
    )


def _atom_events(
    old: FlowSnapshot | None, new: FlowSnapshot
) -> Iterator[Event]:
    previous = old.atoms if old is not None else {}

    for name in new.atom_names:
        current = new.atoms[name]
        before = previous.get(name)

        if before is None or before.state != current.state:
            yield _atom_state_event(new, before, current)

        if _progress_changed(before, current):
            yield _atom_progress_event(new, current)

    for name in sorted(set(previous) - set(new.atoms)):
        # Atoms do not leave a flow while it runs.  If one vanishes the
        # snapshot source is lying to us; say so rather than emit an event
        # kind that means nothing downstream.
        LOG.warning(
            "atom %r disappeared from run %s between observations",
            name,
            new.run_id,
        )


def _progress_changed(
    before: AtomSnapshot | None, current: AtomSnapshot
) -> bool:
    if before is None:
        # Only worth an event if there is something to say; a brand new
        # atom sitting at 0.0 is already fully described by its state.
        return current.progress != 0.0 or current.progress_details is not None
    return (
        before.progress != current.progress
        or before.progress_details != current.progress_details
    )


def _atom_state_event(
    flow: FlowSnapshot,
    before: AtomSnapshot | None,
    current: AtomSnapshot,
) -> Event:
    details: dict[str, object] = {}
    if current.failure is not None and (
        before is None or before.failure != current.failure
    ):
        details["failure"] = current.failure
    if current.revert_failure is not None and (
        before is None or before.revert_failure != current.revert_failure
    ):
        details["revert_failure"] = current.revert_failure
    if current.has_result:
        details["has_result"] = True

    return _new_event(
        flow,
        kind=EventKind.ATOM_STATE,
        atom=current,
        state=current.state,
        old_state=before.state if before is not None else None,
        progress=current.progress,
        details=details,
    )


def _atom_progress_event(flow: FlowSnapshot, current: AtomSnapshot) -> Event:
    details: dict[str, object] = {}
    if current.progress_details is not None:
        details["progress_details"] = current.progress_details
    return _new_event(
        flow,
        kind=EventKind.ATOM_PROGRESS,
        atom=current,
        state=current.state,
        progress=current.progress,
        details=details,
    )


def _new_event(
    flow: FlowSnapshot,
    *,
    kind: EventKind,
    atom: AtomSnapshot | None = None,
    state: str | None = None,
    old_state: str | None = None,
    progress: float | None = None,
    details: dict[str, object] | None = None,
) -> Event:
    """Build an event with placeholder ``seq``/``ts``.

    :func:`diff_flow` stamps both once emission order is settled.
    """
    return Event(
        run_id=flow.run_id,
        seq=_UNSTAMPED_SEQ,
        ts=_UNSTAMPED_TS,
        kind=kind,
        book_id=flow.book_id,
        atom_name=atom.name if atom is not None else None,
        atom_uuid=atom.uuid if atom is not None else None,
        atom_type=atom.atom_type if atom is not None else None,
        state=state,
        old_state=old_state,
        intention=atom.intention if atom is not None else None,
        progress=progress,
        details=dict(details or {}),
    )


__all__ = ["diff_flow"]

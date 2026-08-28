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

"""In-process datasource: state and a bounded event history, in RAM.

Useful on its own for embedded monitoring of a single process, and it is
the reference implementation the other datasources are checked against --
folding an event stream into it must reproduce the snapshots the stream was
derived from.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import replace
from typing import Any

from taskflow_meter.datasource.base import DEFAULT_EVENT_LIMIT
from taskflow_meter.datasource.base import DEFAULT_FLOW_LIMIT
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.datasource.base import UnknownMarkerError
from taskflow_meter.datasource.base import WritableDataSource
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot

LOG = logging.getLogger(__name__)

#: Events retained per run before the oldest are dropped.
DEFAULT_MAX_EVENTS_PER_RUN = 1000


class _Run:
    """Stored state for one flow run."""

    __slots__ = ("events", "flow", "highest_seq", "oldest_seq")

    def __init__(self, flow: FlowSnapshot, max_events: int) -> None:
        self.flow = flow
        self.events: deque[Event] = deque(maxlen=max_events)
        self.oldest_seq: int | None = None
        self.highest_seq = 0


class MemoryDataSource(WritableDataSource):
    """Keeps every run it is told about until told to forget it."""

    name = "memory"

    def __init__(
        self,
        *,
        max_events_per_run: int = DEFAULT_MAX_EVENTS_PER_RUN,
        max_runs: int | None = None,
    ) -> None:
        if max_events_per_run < 1:
            msg = "max_events_per_run must be at least 1"
            raise ValueError(msg)
        if max_runs is not None and max_runs < 1:
            msg = "max_runs must be at least 1 when set"
            raise ValueError(msg)
        self._max_events_per_run = max_events_per_run
        self._max_runs = max_runs
        # Producers write from a poller or executor thread while the API
        # reads from its own; every access below holds this.
        self._lock = threading.RLock()
        self._runs: dict[str, _Run] = {}

    @property
    def max_events_per_run(self) -> int:
        """How much history each run keeps before the oldest is dropped."""
        return self._max_events_per_run

    # -- writing ---------------------------------------------------------

    def apply(self, event: Event) -> None:
        with self._lock:
            run = self._runs.get(event.run_id)
            if run is None:
                run = _Run(_flow_from_event(event), self._max_events_per_run)
                self._runs[event.run_id] = run
                self._evict_if_needed()

            run.flow = _fold(run.flow, event)
            self._record(run, event)

    def _record(self, run: _Run, event: Event) -> None:
        if len(run.events) == self._max_events_per_run:
            # The deque drops the oldest for us; say which one went, so a
            # gap reported later by events_since() can be explained.
            LOG.debug(
                "dropping event %d for run %s: history is full at %d",
                run.events[0].seq,
                event.run_id,
                self._max_events_per_run,
            )
        run.events.append(event)
        run.oldest_seq = run.events[0].seq
        run.highest_seq = max(run.highest_seq, event.seq)

    def _evict_if_needed(self) -> None:
        if self._max_runs is None or len(self._runs) <= self._max_runs:
            return
        # Oldest observation first, so a long-finished run goes before a
        # live one.  Never silent: an operator needs to know the window is
        # too small for their flow volume.
        victim = min(self._runs.values(), key=lambda run: run.flow.observed_at)
        del self._runs[victim.flow.run_id]
        LOG.warning(
            "evicted run %s: at the %d run limit",
            victim.flow.run_id,
            self._max_runs,
        )

    def forget(self, run_id: str) -> bool:
        """Drop a run entirely.  Returns whether it was there."""
        with self._lock:
            return self._runs.pop(run_id, None) is not None

    # -- reading ---------------------------------------------------------

    def get_flow(self, run_id: str) -> FlowSnapshot | None:
        with self._lock:
            run = self._runs.get(run_id)
            return _detach(run.flow) if run is not None else None

    def list_flows(
        self,
        *,
        state: str | None = None,
        book_id: str | None = None,
        limit: int = DEFAULT_FLOW_LIMIT,
        marker: str | None = None,
    ) -> FlowPage:
        if limit < 1:
            msg = "limit must be at least 1"
            raise ValueError(msg)

        with self._lock:
            flows = [
                _detach(run.flow)
                for run in self._runs.values()
                if (state is None or run.flow.state == state)
                and (book_id is None or run.flow.book_id == book_id)
            ]

        # Newest observation first; run_id breaks ties so that paging over
        # flows observed in the same instant cannot repeat or skip one.
        flows.sort(key=lambda flow: (-flow.observed_at, flow.run_id))

        start = 0
        if marker is not None:
            positions = {
                flow.run_id: index for index, flow in enumerate(flows)
            }
            if marker not in positions:
                msg = f"unknown paging marker: {marker!r}"
                raise UnknownMarkerError(msg)
            start = positions[marker] + 1

        window = flows[start : start + limit]
        more = len(flows) > start + limit
        return FlowPage(
            items=tuple(window),
            next_marker=window[-1].run_id if more and window else None,
        )

    def events_since(
        self,
        run_id: str,
        *,
        since_seq: int = 0,
        limit: int = DEFAULT_EVENT_LIMIT,
    ) -> EventPage:
        """Return the contiguous run of events after ``since_seq``.

        Contiguous, and in sequence order, because concurrent producers
        do not arrive in it: two threads under the parallel engine each
        allocate a number and then enqueue, and the enqueues can
        invert.  Returning event 8 while 7 is still in flight would
        advance the caller past 7 for good, so anything beyond a gap is
        held back until the gap fills.
        """
        if limit < 1:
            msg = "limit must be at least 1"
            raise ValueError(msg)

        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                return EventPage(next_seq=since_seq)
            stored = sorted(run.events, key=lambda event: event.seq)

        oldest = stored[0].seq if stored else None
        # A hole exists when the caller's next expected event has
        # already been evicted.
        truncated = oldest is not None and since_seq + 1 < oldest
        expected = (
            oldest if truncated and oldest is not None else since_seq + 1
        )

        selected: list[Event] = []
        for event in stored:
            if event.seq < expected:
                continue
            if event.seq != expected:
                break
            selected.append(event)
            expected += 1
            if len(selected) >= limit:
                break

        return EventPage(
            events=tuple(selected),
            next_seq=selected[-1].seq if selected else since_seq,
            oldest_seq=oldest,
            truncated=truncated,
        )


def _detach(flow: FlowSnapshot) -> FlowSnapshot:
    """Copy a snapshot on its way out.

    The dataclass is frozen but its ``atoms`` mapping is not, so handing
    out the stored object would let a reader mutate what the datasource
    believes.  A monitoring read must never be able to do that.
    """
    return replace(flow, atoms=dict(flow.atoms))


def _flow_from_event(event: Event) -> FlowSnapshot:
    """Seed a run from the first event seen for it.

    The diff engine puts the flow's identity on the first event of a run,
    but a client may still join mid-stream, so everything here has to
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


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _fold(flow: FlowSnapshot, event: Event) -> FlowSnapshot:
    """Return ``flow`` updated by ``event``."""
    flow = replace(flow, observed_at=max(flow.observed_at, event.ts))

    if event.kind is EventKind.FLOW_STATE:
        return replace(flow, state=event.state)

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
        atoms[event.atom_name] = _fold_atom(
            atoms.get(event.atom_name), event, event.atom_name
        )
        return replace(flow, atoms=atoms)

    return flow


def _fold_atom(
    atom: AtomSnapshot | None, event: Event, name: str
) -> AtomSnapshot:
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

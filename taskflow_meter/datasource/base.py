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

"""The contract every datasource implements.

Read and write are separate on purpose.  The primary datasource reads
taskflow's own persistence and can never accept events, while the in-memory
and SQL ones are fed by a producer; making that a type distinction stops a
read-only source from being wired up as a sink by mistake.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable
from dataclasses import dataclass
from types import TracebackType
from typing import TypeVar

from taskflow_meter.events import Event
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot

_D = TypeVar("_D", bound="DataSource")

#: Flows returned by a single unqualified listing.
DEFAULT_FLOW_LIMIT = 50

#: Events returned by a single :meth:`DataSource.events_since` call.
DEFAULT_EVENT_LIMIT = 500


class UnknownMarkerError(LookupError):
    """A paging marker refers to a flow the datasource cannot place.

    Usually means the run expired between pages.  Raised rather than
    silently restarting from the top, which would make a client loop over
    the same first page forever.
    """


@dataclass(frozen=True, slots=True)
class FlowPage:
    """One page of flows, newest observation first."""

    items: tuple[FlowSnapshot, ...] = ()
    next_marker: str | None = None

    @property
    def has_more(self) -> bool:
        return self.next_marker is not None


@dataclass(frozen=True, slots=True)
class EventPage:
    """One page of a run's event stream.

    ``truncated`` is the important field: it says the caller asked for
    events that have already been evicted, so there is a hole between what
    it last saw and what it is being given.  A client that cares must
    re-read the snapshot instead of assuming continuity.
    """

    events: tuple[Event, ...] = ()
    next_seq: int = 0
    oldest_seq: int | None = None
    truncated: bool = False


class DataSource(abc.ABC):
    """Read access to observed flows."""

    #: Stevedore plugin name, set by subclasses.
    name: str = ""

    #: Whether :meth:`events_since` can actually return a history.  A
    #: source reading a store that only keeps current state sets this
    #: False, so an API can decline to advertise a stream it cannot serve
    #: rather than handing clients an empty one that is indistinguishable
    #: from silence.
    supports_events: bool = True

    def start(self) -> None:  # noqa: B027 - optional hook, not abstract
        """Acquire whatever the source needs.  Idempotent.

        Sources holding nothing (the in-memory one) need not override it.
        """

    def stop(self) -> None:  # noqa: B027 - optional hook, not abstract
        """Release it again.  Idempotent, and safe to call unstarted."""

    def __enter__(self: _D) -> _D:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    @abc.abstractmethod
    def list_flows(
        self,
        *,
        state: str | None = None,
        book_id: str | None = None,
        limit: int = DEFAULT_FLOW_LIMIT,
        marker: str | None = None,
    ) -> FlowPage:
        """Return flows, newest observation first."""

    @abc.abstractmethod
    def get_flow(self, run_id: str) -> FlowSnapshot | None:
        """Return one flow, or ``None`` if the source has never seen it."""

    @abc.abstractmethod
    def events_since(
        self,
        run_id: str,
        *,
        since_seq: int = 0,
        limit: int = DEFAULT_EVENT_LIMIT,
    ) -> EventPage:
        """Return events for ``run_id`` with ``seq`` greater than
        ``since_seq``."""

    def get_atoms(self, run_id: str) -> tuple[AtomSnapshot, ...] | None:
        """Return a flow's atoms in name order, or ``None`` if unknown."""
        flow = self.get_flow(run_id)
        if flow is None:
            return None
        return tuple(flow.atoms[name] for name in flow.atom_names)


class WritableDataSource(DataSource):
    """A datasource that is fed by a producer."""

    @abc.abstractmethod
    def apply(self, event: Event) -> None:
        """Fold one event into the stored state."""

    def apply_many(self, events: Iterable[Event]) -> None:
        for event in events:
            self.apply(event)

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

"""The single event shape every producer emits.

The in-process listener and the persistence poller observe a flow in very
different ways, but both express what they saw as one of these, so the
datasources, the API and any transport only ever handle one vocabulary.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Mapping
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import TypeVar

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):
        """As much of 3.11's ``StrEnum`` as this module relies on.

        A plain ``str, Enum`` mixin renders as ``EventKind.FLOW_STATE``
        under ``str()``, and the value is what goes on the wire.
        """

        def __str__(self) -> str:
            return str.__str__(self)


_E = TypeVar("_E", bound="Event")


class EventKind(StrEnum):
    """What an event is reporting."""

    #: The flow itself moved between states.
    FLOW_STATE = "flow_state"
    #: An atom moved between states, or was seen for the first time.
    ATOM_STATE = "atom_state"
    #: An atom reported progress without necessarily changing state.
    ATOM_PROGRESS = "atom_progress"
    #: The flow's graph, emitted once by the in-process producer.  The
    #: poller cannot produce this: taskflow persists atoms but not edges.
    FLOW_STRUCTURE = "flow_structure"
    #: The flow finished and a result or failure summary is available.
    FLOW_RESULT = "flow_result"
    #: Liveness only; carries no state change.
    HEARTBEAT = "heartbeat"


@dataclass(frozen=True, slots=True)
class Event:
    """One observation about one flow run.

    ``seq`` is gap-free and monotonic per ``run_id``, which is what lets a
    client reconnect with ``since_seq`` and know whether it missed
    anything.  ``ts`` is observation time -- see :class:`.FlowSnapshot`.
    """

    run_id: str
    seq: int
    ts: float
    kind: EventKind
    book_id: str | None = None
    atom_name: str | None = None
    atom_uuid: str | None = None
    atom_type: str | None = None
    state: str | None = None
    old_state: str | None = None
    intention: str | None = None
    progress: float | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Render to plain JSON-serialisable types."""
        data = asdict(self)
        data["kind"] = str(self.kind)
        return data

    @classmethod
    def from_dict(cls: type[_E], data: Mapping[str, Any]) -> _E:
        """Rebuild from :meth:`to_dict` output.

        Unknown keys are rejected rather than dropped -- a transport
        speaking a newer dialect should fail loudly, not silently discard
        the part we did not understand.
        """
        payload = dict(data)
        payload["kind"] = EventKind(payload["kind"])
        return cls(**payload)


class SequenceAllocator:
    """Hands out gap-free per-run sequence numbers, starting at 1.

    Thread-safe by necessity: the parallel engine fires notifier callbacks
    from executor threads, and the poller runs on its own thread.
    """

    __slots__ = ("_counters", "_lock")

    def __init__(self, counters: Mapping[str, int] | None = None) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = dict(counters or {})

    def allocate(self, run_id: str) -> int:
        """Return the next sequence number for ``run_id``."""
        with self._lock:
            nxt = self._counters.get(run_id, 0) + 1
            self._counters[run_id] = nxt
            return nxt

    def peek(self, run_id: str) -> int:
        """Return the last number handed out, or 0 if none has been."""
        with self._lock:
            return self._counters.get(run_id, 0)

    def resume_from(self, run_id: str, seq: int) -> None:
        """Continue numbering after ``seq``.

        A restarted collector calls this with the highest sequence already
        stored, so it does not renumber events a client has already seen.
        """
        with self._lock:
            self._counters[run_id] = max(self._counters.get(run_id, 0), seq)

    def forget(self, run_id: str) -> None:
        """Drop the counter for a run that is finished and expired."""
        with self._lock:
            self._counters.pop(run_id, None)

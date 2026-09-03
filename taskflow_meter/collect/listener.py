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

"""Watch an engine's notifiers for flow and atom state changes.

The half of the in-process path taskflow makes easy: a listener sees
every state transition, with the engine reporting them as they happen
rather than a poll interval later.

It is only half.  taskflow never re-emits intra-task progress on the
atom notifier, so the numbers a task reports about itself arrive
through :mod:`taskflow_meter.collect.progress` instead.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from taskflow import states
from taskflow.listeners import base as tf_listeners
from taskflow.types import failure as tf_failure

from taskflow_meter import models
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.events import SequenceAllocator
from taskflow_meter.models import RETRY
from taskflow_meter.models import TASK

LOG = logging.getLogger(__name__)

Emit = Callable[[Event], Any]


class MeterListener(tf_listeners.Listener):
    """Turns an engine's notifications into events."""

    def __init__(
        self,
        engine: Any,
        emit: Emit,
        *,
        allocator: SequenceAllocator | None = None,
        clock: Callable[[], float] = time.time,
        book_id: str | None = None,
    ) -> None:
        """Watch ``engine``, handing each event to ``emit``.

        ``book_id`` has to be supplied because nothing on the engine
        knows it: storage holds a flow detail and a backend, and a flow
        detail carries no reference back to the logbook that owns it.
        Whoever created the book has it; we would only be guessing.
        """
        super().__init__(engine)
        self._emit = emit
        self._allocator = allocator or SequenceAllocator()
        self._clock = clock
        self._book_id = book_id

    @property
    def allocator(self) -> SequenceAllocator:
        return self._allocator

    @property
    def run_id(self) -> str:
        """The flow detail uuid, which is how a run is identified."""
        return str(self._engine.storage.flow_uuid)

    # -- receivers -------------------------------------------------------

    def _flow_receiver(self, state: str, details: dict[str, Any]) -> None:
        self._safely(
            EventKind.FLOW_STATE,
            state=state,
            old_state=details.get("old_state"),
            extra={"flow_name": details.get("flow_name")},
        )

    def _task_receiver(self, state: str, details: dict[str, Any]) -> None:
        self._atom_event(state, details, TASK, "task")

    def _retry_receiver(self, state: str, details: dict[str, Any]) -> None:
        self._atom_event(state, details, RETRY, "retry")

    def _atom_event(
        self,
        state: str,
        details: dict[str, Any],
        atom_type: str,
        prefix: str,
    ) -> None:
        extra: dict[str, Any] = {}
        if "result" in details:
            result = details["result"]
            # Results are arbitrary application objects and are not
            # ours to copy around; their presence is the useful part.
            extra["has_result"] = result is not None
            # Except when the result *is* the failure, which is the one
            # thing anybody watching a flow needs the detail of. Rendered
            # in the same shape the persistence datasource reports, so a
            # client cannot tell which producer it is reading.
            if isinstance(result, tf_failure.Failure):
                # Which one it is follows from the state, not from the
                # intention: taskflow does not put an intention on these
                # notifications, and only a revert that itself failed
                # reaches REVERT_FAILURE.
                key = (
                    "revert_failure"
                    if state == states.REVERT_FAILURE
                    else "failure"
                )
                extra[key] = models.failure_dict(result)
        self._safely(
            EventKind.ATOM_STATE,
            state=state,
            old_state=details.get("old_state"),
            atom_name=details.get(f"{prefix}_name"),
            atom_uuid=details.get(f"{prefix}_uuid"),
            atom_type=atom_type,
            extra=extra,
        )

    # -- emitting --------------------------------------------------------

    def _safely(self, kind: EventKind, **fields: Any) -> None:
        """Build and emit one event, swallowing anything that goes wrong.

        This runs on the engine's thread -- an executor thread under the
        parallel engine.  An exception escaping here would surface
        inside somebody's flow as a monitoring bug they did not ask for.
        """
        try:
            self._emit(self._build(kind, **fields))
        except Exception:
            LOG.exception("could not emit a %s event", kind)

    def _build(
        self,
        kind: EventKind,
        *,
        state: str | None = None,
        old_state: str | None = None,
        atom_name: str | None = None,
        atom_uuid: str | None = None,
        atom_type: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> Event:
        run_id = self.run_id
        details = {
            key: value
            for key, value in (extra or {}).items()
            if value is not None
        }
        return Event(
            run_id=run_id,
            seq=self._allocator.allocate(run_id),
            # Unlike the poller's, this timestamp really is when it
            # happened: the engine is telling us as it does it.
            ts=self._clock(),
            kind=kind,
            book_id=self._book_id,
            atom_name=atom_name,
            atom_uuid=atom_uuid,
            atom_type=atom_type,
            state=state,
            old_state=old_state,
            details=details,
        )

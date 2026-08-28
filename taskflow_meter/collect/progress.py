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

"""Capture the progress a task reports about itself.

The gap a listener cannot close.  When a task calls
``self.update_progress(0.4)``, taskflow's ``TaskAction`` writes the
number to storage and **never re-emits it on the engine's atom
notifier** -- so a listener sees state transitions and nothing in
between.

The number is published on the *task's own* notifier, one per atom, so
this walks the compiled graph and registers there.  Registering is
symmetrical: whatever is bound is unbound again, because leaving a
callback on somebody's task object outlives the monitoring that wanted
it.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from taskflow import task as tf_task
from taskflow.engines.action_engine import compiler as tf_compiler

from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.events import SequenceAllocator
from taskflow_meter.models import TASK

LOG = logging.getLogger(__name__)

Emit = Callable[[Event], Any]


class ProgressTap:
    """Registers for ``EVENT_UPDATE_PROGRESS`` on every task in a flow."""

    def __init__(
        self,
        engine: Any,
        emit: Emit,
        *,
        allocator: SequenceAllocator | None = None,
        clock: Callable[[], float] = time.time,
        book_id: str | None = None,
    ) -> None:
        self._engine = engine
        self._emit = emit
        self._allocator = allocator or SequenceAllocator()
        self._clock = clock
        self._book_id = book_id
        self._bound: list[tuple[Any, Callable[..., None]]] = []

    @property
    def bound(self) -> int:
        """How many tasks are currently being listened to."""
        return len(self._bound)

    def register(self) -> None:
        """Bind to every task in the flow.  Idempotent."""
        if self._bound:
            return
        for atom in atoms_of(self._engine):
            if not isinstance(atom, tf_task.Task):
                # Only tasks report progress; a retry controller has no
                # notifier to bind to.
                continue
            if not atom.notifier.can_be_registered(
                tf_task.EVENT_UPDATE_PROGRESS
            ):
                LOG.debug(
                    "task %r does not accept progress listeners", atom.name
                )
                continue
            callback = self._callback_for(atom)
            atom.notifier.register(tf_task.EVENT_UPDATE_PROGRESS, callback)
            self._bound.append((atom, callback))

    def deregister(self) -> None:
        """Unbind everything.  Idempotent, and never raises."""
        while self._bound:
            atom, callback = self._bound.pop()
            try:
                atom.notifier.deregister(
                    tf_task.EVENT_UPDATE_PROGRESS, callback
                )
            except Exception:
                LOG.warning(
                    "could not deregister from task %r",
                    getattr(atom, "name", atom),
                    exc_info=True,
                )

    def __enter__(self) -> ProgressTap:
        self.register()
        return self

    def __exit__(self, *exc: object) -> None:
        self.deregister()

    def _callback_for(self, atom: Any) -> Callable[..., None]:
        def on_progress(
            event_type: str,  # noqa: ARG001 - taskflow's callback shape
            details: Any,
        ) -> None:
            # Runs on the thread executing the task.  Nothing may
            # escape: a monitoring bug must not fail somebody's task.
            try:
                self._emit(self._build(atom, details))
            except Exception:
                LOG.exception("could not emit progress for task %r", atom.name)

        return on_progress

    def _build(self, atom: Any, details: Any) -> Event:
        payload = dict(details or {})
        progress = payload.pop("progress", None)
        run_id = str(self._engine.storage.flow_uuid)
        return Event(
            run_id=run_id,
            seq=self._allocator.allocate(run_id),
            ts=self._clock(),
            kind=EventKind.ATOM_PROGRESS,
            book_id=self._book_id,
            atom_name=atom.name,
            atom_uuid=_atom_uuid(self._engine, atom.name),
            atom_type=TASK,
            progress=float(progress) if progress is not None else None,
            details={"progress_details": payload} if payload else {},
        )


def atoms_of(engine: Any) -> list[Any]:
    """Every atom in the flow, from the compiled graph.

    Compiling here rather than waiting for ``run()`` is what lets the
    tap bind before the first task starts -- and it is idempotent, so
    an engine that was already compiled is untouched.
    """
    engine.compile()
    compilation = engine.compilation
    if compilation is None:  # pragma: no cover - compile() just ran
        return []
    graph = compilation.execution_graph
    return [
        node
        for node, data in graph.nodes(data=True)
        if data.get("kind") in tf_compiler.ATOMS
    ]


def _atom_uuid(engine: Any, name: str) -> str | None:
    try:
        return str(engine.storage.get_atom_uuid(name))
    except Exception:
        # Before the flow is prepared the atom may have no detail yet.
        return None

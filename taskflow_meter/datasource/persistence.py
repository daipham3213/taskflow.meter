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

"""Read taskflow's own persistence layer, and write nothing back.

This is the datasource that makes an existing deployment observable
without touching its flow code.  Everything it reports was put there by
taskflow itself: states via the engine, and per-atom progress via
``storage.set_task_progress()``, which write-throughs to the backend on
every ``update_progress()`` call.

Two limits are inherent rather than incidental, and callers are better
off knowing them than discovering them:

* **No timestamps.**  Only the logbook carries ``created_at`` and
  ``updated_at``; flow and atom details carry none.  Snapshots are
  therefore stamped with our own observation time.
* **No topology.**  Atoms are persisted, edges are not, so nothing here
  can describe the graph.  That needs the in-process listener.

Listing is a full scan: taskflow's connection API offers no filtering or
paging, so filters are applied after reading.  Put a cache in front of it
before pointing a busy API at a large logbook.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from collections.abc import Callable
from collections.abc import Iterator
from typing import Any

from taskflow import exceptions as tf_exc
from taskflow.persistence import backends as tf_backends
from taskflow.persistence import models as tf_models

from taskflow_meter.datasource.base import DEFAULT_EVENT_LIMIT
from taskflow_meter.datasource.base import DEFAULT_FLOW_LIMIT
from taskflow_meter.datasource.base import DataSource
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.datasource.base import UnknownMarkerError
from taskflow_meter.models import RETRY
from taskflow_meter.models import TASK
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot

LOG = logging.getLogger(__name__)

#: taskflow's name for a task detail, as reported by ``atom_detail_type``.
_TASK_DETAIL = "TASK_DETAIL"

#: Where ``storage.set_task_progress`` files what it records.
_META_PROGRESS = "progress"
_META_PROGRESS_DETAILS = "progress_details"


class PersistenceDataSource(DataSource):
    """A read-only view of a taskflow persistence backend."""

    name = "persistence"

    #: Persistence stores state, not history.  Pair this source with a
    #: Poller feeding a writable datasource to get an event stream.
    supports_events = False

    def __init__(
        self,
        backend: Any = None,
        *,
        conf: dict[str, Any] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Wrap an existing ``backend``, or build one from ``conf``.

        A backend passed in is borrowed and never closed -- it usually
        belongs to the application being monitored.  One built from
        ``conf`` is ours, and :meth:`stop` closes it.
        """
        if (backend is None) == (conf is None):
            msg = "pass exactly one of backend or conf"
            raise ValueError(msg)
        self._backend = backend
        self._conf = conf
        self._owns_backend = backend is None
        self._clock = clock
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._backend is None and self._conf is not None:
                self._backend = tf_backends.fetch(self._conf)

    def stop(self) -> None:
        with self._lock:
            if self._owns_backend and self._backend is not None:
                self._backend.close()
                self._backend = None

    @contextlib.contextmanager
    def _connection(self) -> Iterator[Any]:
        """Borrow a connection, closing it however we leave."""
        with self._lock:
            backend = self._backend
        if backend is None:
            self.start()
            with self._lock:
                backend = self._backend
            if backend is None:
                msg = "datasource has no backend"
                raise RuntimeError(msg)
        connection = backend.get_connection()
        try:
            yield connection
        finally:
            with contextlib.suppress(Exception):
                connection.close()

    # -- reading ---------------------------------------------------------

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

        flows = [
            flow
            for flow in self._scan()
            if (state is None or flow.state == state)
            and (book_id is None or flow.book_id == book_id)
        ]
        # Flow details carry no timestamp of their own, so ordering falls
        # back to the owning book's creation time, then the run id to keep
        # paging stable.
        flows.sort(key=lambda flow: (-flow.observed_at, flow.run_id))

        start = 0
        if marker is not None:
            positions = {flow.run_id: i for i, flow in enumerate(flows)}
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

    def get_flow(self, run_id: str) -> FlowSnapshot | None:
        """Return one flow.

        Reads the flow detail directly, then scans the logbooks only to
        attach the owning book's identity -- taskflow's connection API
        offers no way to go from a flow back to its book.
        """
        with self._connection() as conn:
            try:
                detail = conn.get_flow_details(run_id, lazy=False)
            except tf_exc.NotFound:
                return None
            book_id, book_name, created = self._find_book(conn, run_id)
            return self._flow_snapshot(detail, book_id, book_name, created)

    def get_atoms(self, run_id: str) -> tuple[AtomSnapshot, ...] | None:
        """Return a flow's atoms without paying for the book lookup.

        The existence check is not redundant: ``get_atoms_for_flow``
        answers an unknown flow with an empty list, which would otherwise
        be indistinguishable from a flow that has no atoms yet -- a 404
        reported as an empty collection.
        """
        with self._connection() as conn:
            try:
                conn.get_flow_details(run_id, lazy=True)
                details = list(conn.get_atoms_for_flow(run_id))
            except tf_exc.NotFound:
                return None
            atoms = [_atom_snapshot(detail) for detail in details]
            return tuple(sorted(atoms, key=lambda atom: atom.name))

    def events_since(
        self,
        run_id: str,  # noqa: ARG002 - part of the DataSource contract
        *,
        since_seq: int = 0,
        limit: int = DEFAULT_EVENT_LIMIT,  # noqa: ARG002 - ditto
    ) -> EventPage:
        """Always empty: taskflow persists state, never a history.

        Reported through :attr:`supports_events` so an API can decline to
        advertise a stream it cannot serve, rather than handing clients an
        empty one that is indistinguishable from silence.
        """
        return EventPage(next_seq=since_seq)

    # -- internals -------------------------------------------------------

    def _scan(self) -> list[FlowSnapshot]:
        """Walk every logbook.  See the module docstring on cost."""
        snapshots: list[FlowSnapshot] = []
        with self._connection() as conn:
            for book in conn.get_logbooks(lazy=True):
                created = _epoch(book.created_at)
                for detail in conn.get_flows_for_book(book.uuid):
                    snapshots.append(
                        self._flow_snapshot(
                            detail, book.uuid, book.name, created
                        )
                    )
        return snapshots

    def _find_book(
        self, conn: Any, run_id: str
    ) -> tuple[str | None, str | None, float | None]:
        for book in conn.get_logbooks(lazy=True):
            for detail in conn.get_flows_for_book(book.uuid):
                if detail.uuid == run_id:
                    return book.uuid, book.name, _epoch(book.created_at)
        return None, None, None

    def _flow_snapshot(
        self,
        detail: Any,
        book_id: str | None,
        book_name: str | None,
        book_created_at: float | None,
    ) -> FlowSnapshot:
        atoms = {
            atom.name: atom for atom in (_atom_snapshot(ad) for ad in detail)
        }
        meta = dict(detail.meta or {})
        if book_created_at is not None:
            # The only real timestamp taskflow gives us.  Kept in meta
            # rather than passed off as an observation time.
            meta.setdefault("book_created_at", book_created_at)
        return FlowSnapshot(
            run_id=detail.uuid,
            name=detail.name,
            state=detail.state,
            book_id=book_id,
            book_name=book_name,
            observed_at=self._clock(),
            meta=meta,
            atoms=atoms,
        )


def _atom_snapshot(detail: Any) -> AtomSnapshot:
    meta = dict(detail.meta or {})
    return AtomSnapshot(
        name=detail.name,
        uuid=detail.uuid,
        atom_type=(
            TASK
            if tf_models.atom_detail_type(detail) == _TASK_DETAIL
            else RETRY
        ),
        state=detail.state,
        intention=detail.intention,
        progress=float(meta.get(_META_PROGRESS) or 0.0),
        progress_details=meta.get(_META_PROGRESS_DETAILS),
        failure=_failure_dict(detail.failure),
        revert_failure=_failure_dict(getattr(detail, "revert_failure", None)),
        # Carrying results would mean copying arbitrary application data
        # into a monitoring path, so only their presence is reported.  A
        # task that returns None is indistinguishable from one that
        # returned nothing, which is a trade we accept.
        has_result=detail.results is not None,
        meta=meta,
    )


def _failure_dict(failure: Any) -> dict[str, Any] | None:
    """Render a taskflow Failure, tolerating anything that is not one."""
    if failure is None:
        return None
    try:
        return dict(failure.to_dict())
    except Exception:
        LOG.warning("could not serialise a failure", exc_info=True)
        return {"unserialisable": repr(failure)}


def _epoch(when: Any) -> float | None:
    """Convert a datetime to a float, or give up quietly."""
    try:
        return float(when.timestamp())
    except (AttributeError, TypeError, ValueError, OSError):
        return None

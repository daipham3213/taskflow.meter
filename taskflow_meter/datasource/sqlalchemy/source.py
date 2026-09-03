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

"""A datasource with a schema of its own.

Where the persistence datasource reads what taskflow happens to keep,
this one owns what it stores: the full event history, real filtering
and paging in SQL rather than a full scan, and a retention policy the
deployment sets rather than inherits.

It is the far end of the collector deployment -- one process consumes
events from a transport and writes them here, and any number of API
workers read from it with ``Meter(poll=False)``.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from taskflow_meter.datasource.base import DEFAULT_EVENT_LIMIT
from taskflow_meter.datasource.base import DEFAULT_FLOW_LIMIT
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.datasource.base import UnknownMarkerError
from taskflow_meter.datasource.base import WritableDataSource
from taskflow_meter.datasource.sqlalchemy.models import events as events_t
from taskflow_meter.datasource.sqlalchemy.models import flows as flows_t
from taskflow_meter.datasource.sqlalchemy.models import metadata
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.fold import contiguous_from
from taskflow_meter.fold import flow_from_event
from taskflow_meter.fold import fold
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot

LOG = logging.getLogger(__name__)

MIGRATIONS = Path(__file__).parent / "migrations"


class SQLADataSource(WritableDataSource):
    """Stores flows and their events in a database we own."""

    name = "sqlalchemy"

    def __init__(
        self,
        url: str | None = None,
        *,
        engine: Any = None,
        create_schema: bool = False,
    ) -> None:
        """Connect to ``url``, or use an ``engine`` somebody else owns.

        ``create_schema`` is for tests and throwaway databases; a real
        deployment runs :func:`upgrade` so the schema has a version.
        """
        if (url is None) == (engine is None):
            msg = "pass exactly one of url or engine"
            raise ValueError(msg)
        self._url = url
        self._engine = engine
        self._owns_engine = engine is None
        if create_schema:
            metadata.create_all(self.engine)

    @property
    def engine(self) -> Any:
        if self._engine is None:
            self._engine = sa.create_engine(str(self._url))
        return self._engine

    def stop(self) -> None:
        if self._owns_engine and self._engine is not None:
            self._engine.dispose()
            self._engine = None

    # -- writing ---------------------------------------------------------

    def apply(self, event: Event) -> None:
        self.apply_many([event])

    def apply_many(self, events: Iterable[Event]) -> None:
        """Fold a batch into the stored state, in one transaction.

        Grouped by run so a batch touching several flows still reads and
        rewrites each row once.
        """
        batch = list(events)
        if not batch:
            return

        by_run: dict[str, list[Event]] = {}
        for event in batch:
            by_run.setdefault(event.run_id, []).append(event)

        with self.engine.begin() as conn:
            for run_id, run_events in by_run.items():
                self._apply_run(conn, run_id, run_events)

    def _apply_run(
        self, conn: Any, run_id: str, run_events: list[Event]
    ) -> None:
        snapshot = self._load_flow(conn, run_id)
        if snapshot is None:
            snapshot = flow_from_event(run_events[0])
        for event in run_events:
            snapshot = fold(snapshot, event)
        self._save_flow(conn, snapshot)

        for event in run_events:
            row = _event_row(event)
            try:
                with conn.begin_nested():
                    conn.execute(sa.insert(events_t).values(**row))
            except sa.exc.IntegrityError:
                # (run_id, seq) already stored: a collector that
                # reconnected and replayed, which must be a no-op
                # rather than a duplicate.
                LOG.debug(
                    "event %d for run %s is already stored",
                    event.seq,
                    run_id,
                )

    def _save_flow(self, conn: Any, snapshot: FlowSnapshot) -> None:
        values = {
            "run_id": snapshot.run_id,
            "book_id": snapshot.book_id,
            "book_name": snapshot.book_name,
            "name": snapshot.name,
            "state": snapshot.state,
            "observed_at": snapshot.observed_at,
            "meta": snapshot.meta,
            "atoms": {
                name: asdict(atom) for name, atom in snapshot.atoms.items()
            },
        }
        updated = conn.execute(
            sa.update(flows_t)
            .where(flows_t.c.run_id == snapshot.run_id)
            .values(**values)
        )
        if updated.rowcount == 0:
            conn.execute(sa.insert(flows_t).values(**values))

    def forget(self, run_id: str) -> bool:
        """Drop a run and its events.  Returns whether it was there."""
        with self.engine.begin() as conn:
            conn.execute(
                sa.delete(events_t).where(events_t.c.run_id == run_id)
            )
            deleted = conn.execute(
                sa.delete(flows_t).where(flows_t.c.run_id == run_id)
            )
        return bool(deleted.rowcount)

    def prune(self, before: float) -> int:
        """Drop runs last observed before ``before``.

        This datasource keeps what it is told forever otherwise -- the
        deployment owns the retention policy, unlike the persistence
        source, which inherits taskflow's.
        """
        with self.engine.begin() as conn:
            stale = [
                row.run_id
                for row in conn.execute(
                    sa.select(flows_t.c.run_id).where(
                        flows_t.c.observed_at < before
                    )
                )
            ]
            if not stale:
                return 0
            conn.execute(
                sa.delete(events_t).where(events_t.c.run_id.in_(stale))
            )
            conn.execute(sa.delete(flows_t).where(flows_t.c.run_id.in_(stale)))
        return len(stale)

    # -- reading ---------------------------------------------------------

    def get_flow(self, run_id: str) -> FlowSnapshot | None:
        with self._connect() as conn:
            return self._load_flow(conn, run_id)

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

        query = sa.select(flows_t)
        if state is not None:
            query = query.where(flows_t.c.state == state)
        if book_id is not None:
            query = query.where(flows_t.c.book_id == book_id)

        with self._connect() as conn:
            if marker is not None:
                anchor = conn.execute(
                    sa.select(flows_t.c.observed_at, flows_t.c.run_id).where(
                        flows_t.c.run_id == marker
                    )
                ).first()
                if anchor is None:
                    msg = f"unknown paging marker: {marker!r}"
                    raise UnknownMarkerError(msg)
                # Keyset paging in the listing order: newest first,
                # ties broken by run_id ascending.
                query = query.where(
                    sa.or_(
                        flows_t.c.observed_at < anchor.observed_at,
                        sa.and_(
                            flows_t.c.observed_at == anchor.observed_at,
                            flows_t.c.run_id > anchor.run_id,
                        ),
                    )
                )

            query = query.order_by(
                flows_t.c.observed_at.desc(), flows_t.c.run_id.asc()
            ).limit(limit + 1)
            rows = list(conn.execute(query))

        more = len(rows) > limit
        window = [_flow_from_row(row) for row in rows[:limit]]
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
        if limit < 1:
            msg = "limit must be at least 1"
            raise ValueError(msg)

        with self._connect() as conn:
            oldest = conn.execute(
                sa.select(sa.func.min(events_t.c.seq)).where(
                    events_t.c.run_id == run_id
                )
            ).scalar()
            rows = list(
                conn.execute(
                    sa.select(events_t)
                    .where(
                        events_t.c.run_id == run_id,
                        events_t.c.seq > since_seq,
                    )
                    .order_by(events_t.c.seq.asc())
                    .limit(limit)
                )
            )

        truncated = oldest is not None and since_seq + 1 < oldest
        expected = (
            oldest if truncated and oldest is not None else since_seq + 1
        )
        selected = contiguous_from(
            [_event_from_row(row) for row in rows], expected, limit
        )
        return EventPage(
            events=tuple(selected),
            next_seq=selected[-1].seq if selected else since_seq,
            oldest_seq=oldest,
            truncated=truncated,
        )

    # -- internals -------------------------------------------------------

    @contextlib.contextmanager
    def _connect(self) -> Iterator[Any]:
        with self.engine.connect() as conn:
            yield conn

    def _load_flow(self, conn: Any, run_id: str) -> FlowSnapshot | None:
        row = conn.execute(
            sa.select(flows_t).where(flows_t.c.run_id == run_id)
        ).first()
        return None if row is None else _flow_from_row(row)


def _flow_from_row(row: Any) -> FlowSnapshot:
    return FlowSnapshot(
        run_id=row.run_id,
        name=row.name,
        state=row.state,
        book_id=row.book_id,
        book_name=row.book_name,
        observed_at=row.observed_at,
        meta=dict(row.meta or {}),
        atoms={
            name: AtomSnapshot(**data)
            for name, data in (row.atoms or {}).items()
        },
    )


def _event_row(event: Event) -> dict[str, Any]:
    row = asdict(event)
    row["kind"] = str(event.kind)
    return row


def _event_from_row(row: Any) -> Event:
    return Event(
        run_id=row.run_id,
        seq=row.seq,
        ts=row.ts,
        kind=EventKind(row.kind),
        book_id=row.book_id,
        atom_name=row.atom_name,
        atom_uuid=row.atom_uuid,
        atom_type=row.atom_type,
        state=row.state,
        old_state=row.old_state,
        intention=row.intention,
        progress=row.progress,
        details=dict(row.details or {}),
    )


def upgrade(
    url: str,
    revision: str = "head",
    *,
    version_table: str | None = None,
) -> None:
    """Bring a database up to date, the way a deployment should.

    Imported lazily: alembic is only needed by whoever runs migrations,
    not by every process that reads the results.

    ``version_table`` renames alembic's bookkeeping table, which matters
    when this schema shares a database with a host service that migrates
    its own: both would otherwise keep their revision in
    ``alembic_version``, and each would read the other's as a revision it
    has never heard of.  Pass the same name on every upgrade of a given
    database -- changing it later looks exactly like an unmigrated one.
    """
    from alembic import command
    from alembic.config import Config

    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS))
    config.set_main_option("sqlalchemy.url", url)
    if version_table is not None:
        config.set_main_option("version_table", version_table)
    command.upgrade(config, revision)

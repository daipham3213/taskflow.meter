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

"""The handle everything else hangs off.

A :class:`Meter` owns a source, optionally a store and a poller, and the
lifecycle that ties them together.  The API layer holds one of these and
nothing else, which is what lets the same object serve an embedded
single-process dashboard and a worker reading a shared store.

Lifecycle deserves its own note, because the obvious mechanism does not
work.  **A mounted ASGI app never receives the lifespan scope** -- the
host router handles it at the root and does not forward it to a mounted
sub-application -- so startup cannot hang off lifespan alone.  Hence
three ways in, all safe to combine:

1. :meth:`start` / :meth:`stop`, or the context manager.  What a host
   application should call from its own lifespan or ``AppConfig.ready``.
2. :meth:`lifespan`, for when our ASGI app *is* the root application.
3. :meth:`ensure_started`, the lazy fallback a first request can call.

:meth:`start` and :meth:`stop` are reference counted, so a host that
starts the meter and a first request that also touches it do not fight:
the poller stops when the last holder lets go.  :meth:`ensure_started`
deliberately takes no reference -- it has no paired release.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import threading
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Self

from taskflow_meter.datasource.base import DEFAULT_EVENT_LIMIT
from taskflow_meter.datasource.base import DEFAULT_FLOW_LIMIT
from taskflow_meter.datasource.base import DataSource
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.datasource.base import WritableDataSource
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot
from taskflow_meter.poller import DEFAULT_INTERVAL
from taskflow_meter.poller import Poller

LOG = logging.getLogger(__name__)


class Meter:
    """Owns a source, a store, a poller, and their lifecycle."""

    def __init__(
        self,
        source: DataSource,
        *,
        store: WritableDataSource | None = None,
        poll: bool = True,
        interval: float = DEFAULT_INTERVAL,
    ) -> None:
        """Watch ``source``, keeping what is observed in ``store``.

        With ``poll`` set, a store is required and defaults to an
        in-memory one.  Without it there is no poller and no thread: the
        meter is a read-through to ``source``, or to ``store`` when one
        is supplied -- which is the shape an API worker takes when a
        separate collector process keeps the store warm.
        """
        self.source = source
        self.store = (
            store
            if store is not None
            else (MemoryDataSource() if poll else None)
        )
        self._poller: Poller | None = None
        if poll:
            assert self.store is not None
            self._poller = Poller(source, self.store, interval=interval)

        self._lock = threading.RLock()
        self._holders = 0
        self._started = False

    # -- what to read from -----------------------------------------------

    @property
    def reader(self) -> DataSource:
        """Where queries are answered from.

        The store wins when there is one, even though the source may be
        fresher.  Serving snapshots from the source while serving events
        from the store would let a client see a snapshot newer than the
        stream it is resuming, and silently miss the difference.
        """
        return self.store if self.store is not None else self.source

    @property
    def poller(self) -> Poller | None:
        return self._poller

    @property
    def running(self) -> bool:
        with self._lock:
            return self._started

    # -- lifecycle -------------------------------------------------------

    def start(self) -> None:
        """Take a reference, starting everything on the first one."""
        with self._lock:
            self._holders += 1
            self._start_locked()

    def stop(self) -> None:
        """Release a reference, stopping when the last one goes.

        Extra calls are harmless: the count floors at zero rather than
        going negative and leaving the meter unstoppable.
        """
        with self._lock:
            if self._holders > 0:
                self._holders -= 1
            if self._holders == 0:
                self._stop_locked()

    def ensure_started(self) -> None:
        """Start if not already running, without taking a reference.

        For the lazy path: a first request can call this without owing a
        matching :meth:`stop`.
        """
        with self._lock:
            self._start_locked()

    def _start_locked(self) -> None:
        if self._started:
            return
        self.source.start()
        if self.store is not None:
            self.store.start()
        if self._poller is not None:
            self._poller.start()
        # Registered once per start cycle: _start_locked returns early
        # when already started, so this cannot stack up.
        atexit.register(self._atexit_stop)
        self._started = True

    def _stop_locked(self) -> None:
        if not self._started:
            return
        self._started = False
        if self._poller is not None:
            self._poller.stop()
        # Shutdown must not be abandoned halfway because one component
        # objected, or the rest leak.
        for closing in (self.store, self.source):
            if closing is None:
                continue
            try:
                closing.stop()
            except Exception:
                LOG.exception("failed stopping %r", closing)
        # A no-op when it was never registered.
        atexit.unregister(self._atexit_stop)

    def _atexit_stop(self) -> None:
        with self._lock:
            self._holders = 0
            self._stop_locked()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    @contextlib.asynccontextmanager
    async def lifespan(
        self,
        app: object = None,  # noqa: ARG002 - ASGI hands the app over
    ) -> AsyncIterator[None]:
        """ASGI lifespan handler, for when our app is the root one.

        Useless on a *mounted* app -- see the module docstring -- so this
        is a convenience, never the only way in.
        """
        self.start()
        try:
            yield
        finally:
            self.stop()

    # -- queries ---------------------------------------------------------

    @property
    def supports_events(self) -> bool:
        return self.reader.supports_events

    def list_flows(
        self,
        *,
        state: str | None = None,
        book_id: str | None = None,
        limit: int = DEFAULT_FLOW_LIMIT,
        marker: str | None = None,
    ) -> FlowPage:
        return self.reader.list_flows(
            state=state, book_id=book_id, limit=limit, marker=marker
        )

    def get_flow(self, run_id: str) -> FlowSnapshot | None:
        return self.reader.get_flow(run_id)

    def get_atoms(self, run_id: str) -> tuple[AtomSnapshot, ...] | None:
        return self.reader.get_atoms(run_id)

    def events_since(
        self,
        run_id: str,
        *,
        since_seq: int = 0,
        limit: int = DEFAULT_EVENT_LIMIT,
    ) -> EventPage:
        return self.reader.events_since(
            run_id, since_seq=since_seq, limit=limit
        )

    def poll_once(self) -> int:
        """Run one poll on the calling thread.

        Lets a caller drive the meter deterministically instead of
        waiting on the interval.
        """
        if self._poller is None:
            msg = "this meter has no poller (poll=False)"
            raise RuntimeError(msg)
        return self._poller.poll_once()

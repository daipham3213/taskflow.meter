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

"""Turn a state-only datasource into an event stream, by watching it.

The poller is the second producer.  It reads whatever a source can see,
diffs each flow against what it saw last time, and feeds the resulting
events to a sink -- which is how a deployment whose persistence records
only current state ends up with a resumable stream.

What it cannot do is see between polls.  A state a flow passed through
and left within one interval was never observable, and no amount of
diffing invents it.  Shorter intervals narrow that window at the cost of
load; the in-process listener closes it entirely.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field

from taskflow_meter.datasource.base import DEFAULT_FLOW_LIMIT
from taskflow_meter.datasource.base import DataSource
from taskflow_meter.datasource.base import WritableDataSource
from taskflow_meter.diff import diff_flow
from taskflow_meter.events import SequenceAllocator
from taskflow_meter.models import FlowSnapshot

LOG = logging.getLogger(__name__)

#: Seconds between polls, when nothing says otherwise.
DEFAULT_INTERVAL = 2.0


@dataclass(slots=True)
class PollStats:
    """Counters worth exposing on a health endpoint."""

    polls: int = 0
    events: int = 0
    errors: int = 0
    flows_seen: int = 0
    last_error: str | None = field(default=None)


class Poller:
    """Watches ``source``, feeding what changes into ``sink``."""

    def __init__(
        self,
        source: DataSource,
        sink: WritableDataSource,
        *,
        interval: float = DEFAULT_INTERVAL,
        allocator: SequenceAllocator | None = None,
        page_size: int = DEFAULT_FLOW_LIMIT,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        if interval <= 0:
            msg = "interval must be positive"
            raise ValueError(msg)
        if page_size < 1:
            msg = "page_size must be at least 1"
            raise ValueError(msg)

        self.source = source
        self.sink = sink
        self.interval = interval
        self.stats = PollStats()

        self._allocator = allocator or SequenceAllocator()
        self._page_size = page_size
        self._on_error = on_error
        # Every flow the source still reports, as last seen.  Finished
        # flows stay here on purpose: dropping one whose events were
        # already emitted would make the next poll rediscover it and
        # replay its whole history.
        self._previous: dict[str, FlowSnapshot] = {}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()

    @property
    def allocator(self) -> SequenceAllocator:
        return self._allocator

    @property
    def running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    # -- one pass --------------------------------------------------------

    def poll_once(self) -> int:
        """Read, diff, emit.  Returns how many events were produced.

        Raises whatever the source or sink raises; the background loop is
        what turns that into a counted, logged error.
        """
        seen: dict[str, FlowSnapshot] = {}
        emitted = 0

        for snapshot in self._read_all():
            seen[snapshot.run_id] = snapshot
            previous = self._previous.get(snapshot.run_id)
            events = diff_flow(
                previous,
                snapshot,
                allocator=self._allocator,
                ts=snapshot.observed_at,
            )
            if events:
                self.sink.apply_many(events)
                emitted += len(events)

        for run_id in self._previous.keys() - seen.keys():
            # The source stopped reporting it -- the logbook was deleted,
            # or retention expired it.  Forget it rather than pretend.
            LOG.debug("run %s is no longer reported by the source", run_id)

        self._previous = seen
        self.stats.polls += 1
        self.stats.events += emitted
        self.stats.flows_seen = len(seen)
        return emitted

    def _read_all(self) -> list[FlowSnapshot]:
        """Walk every page the source offers."""
        flows: list[FlowSnapshot] = []
        marker: str | None = None
        while True:
            page = self.source.list_flows(limit=self._page_size, marker=marker)
            flows.extend(page.items)
            if not page.has_more:
                return flows
            marker = page.next_marker

    # -- background loop -------------------------------------------------

    def start(self) -> None:
        """Begin polling on a daemon thread.  Idempotent."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopping.clear()
            self._thread = threading.Thread(
                target=self._loop,
                name="taskflow-meter-poller",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout: float | None = 5.0) -> None:
        """Ask the loop to finish and wait for it.  Idempotent."""
        with self._lock:
            thread = self._thread
            self._thread = None
        self._stopping.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout)
            if thread.is_alive():
                LOG.warning("poller thread did not stop within %.1fs", timeout)

    def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                self.poll_once()
            except Exception as exc:
                # A monitoring loop that dies on one bad read stops
                # monitoring silently, which is worse than a noisy one.
                self.stats.errors += 1
                self.stats.last_error = repr(exc)
                LOG.exception("poll failed")
                if self._on_error is not None:
                    try:
                        self._on_error(exc)
                    except Exception:
                        LOG.exception("poller error handler failed")
            self._stopping.wait(self.interval)

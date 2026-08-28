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

"""The barrier between a running flow and everything downstream.

We are inside somebody else's task.  A slow webhook, a full disk or a
bug in a publisher must cost that task nothing, so the rules here are
absolute:

* :meth:`EventPipeline.submit` never raises and never blocks.  It puts
  the event on a bounded queue and returns.
* All delivery happens on a sender thread.  Nothing downstream runs on
  the thread executing the task.
* A full queue drops, counts and logs.  Blocking would stall the flow;
  growing without limit would take the process down later, for reasons
  nobody would connect back to monitoring.
* A publisher that raises is logged and counted.  The next batch is
  still attempted, because one bad delivery is not a reason to stop
  watching.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass

from taskflow_meter.events import Event
from taskflow_meter.transports.base import Publisher

LOG = logging.getLogger(__name__)

DEFAULT_MAX_QUEUE = 1000
DEFAULT_BATCH = 100

#: Pushed on the queue to wake the sender for shutdown.
_STOP = object()


@dataclass(slots=True)
class PipelineStats:
    """Counters worth exposing on a health endpoint."""

    submitted: int = 0
    delivered: int = 0
    dropped: int = 0
    errors: int = 0
    last_error: str | None = None


class EventPipeline:
    """Queues events and delivers them to publishers off-thread."""

    def __init__(
        self,
        publishers: Sequence[Publisher],
        *,
        max_queue: int = DEFAULT_MAX_QUEUE,
        batch_size: int = DEFAULT_BATCH,
        on_drop: Callable[[Event], None] | None = None,
    ) -> None:
        if max_queue < 1:
            msg = "max_queue must be at least 1"
            raise ValueError(msg)
        if batch_size < 1:
            msg = "batch_size must be at least 1"
            raise ValueError(msg)

        self.publishers = tuple(publishers)
        self.batch_size = batch_size
        self.stats = PipelineStats()
        self._on_drop = on_drop
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._idle = threading.Event()
        self._idle.set()

    @property
    def running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    # -- the flow's side -------------------------------------------------

    def submit(self, event: Event) -> bool:
        """Queue an event.  Returns whether *this* event was queued.

        A full queue makes room by discarding the oldest, so a true
        return does not mean nothing was lost -- ``stats.dropped`` is
        what says that.

        Called from task and executor threads.  It cannot raise: a
        monitoring bug must never fail somebody's task.
        """
        self.stats.submitted += 1
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            return self._make_room_for(event)
        except Exception:  # pragma: no cover - defensive
            LOG.exception("could not queue an event")
            return False
        else:
            self._idle.clear()
            return True

    def _make_room_for(self, event: Event) -> bool:
        """Discard the oldest to fit ``event`` in, and say so."""
        try:
            self._queue.get_nowait()
            self._queue.task_done()
        except queue.Empty:  # pragma: no cover - racing a fast drain
            pass
        self.stats.dropped += 1
        LOG.warning(
            "event queue is full at %d; dropped the oldest (%d so far)",
            self._queue.maxsize,
            self.stats.dropped,
        )
        if self._on_drop is not None:
            try:
                self._on_drop(event)
            except Exception:
                LOG.exception("drop handler failed")
        try:
            self._queue.put_nowait(event)
        except queue.Full:  # pragma: no cover - racing another producer
            self.stats.dropped += 1
            return False
        self._idle.clear()
        return True

    # -- lifecycle -------------------------------------------------------

    def start(self) -> None:
        """Begin delivering on a daemon thread.  Idempotent."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            for publisher in self.publishers:
                publisher.start()
            self._thread = threading.Thread(
                target=self._run,
                name="taskflow-meter-pipeline",
                daemon=True,
            )
            self._thread.start()

    def stop(self, *, drain: bool = True, timeout: float = 5.0) -> None:
        """Finish delivering and shut the sender down.  Idempotent."""
        with self._lock:
            thread = self._thread
            self._thread = None
        if thread is not None and thread.is_alive():
            if drain:
                self.flush(timeout)
            self._queue.put(_STOP)
            thread.join(timeout)
            if thread.is_alive():
                LOG.warning(
                    "pipeline thread did not stop within %.1fs", timeout
                )
        for publisher in self.publishers:
            try:
                publisher.stop()
            except Exception:
                LOG.exception("failed stopping %r", publisher)

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait for the queue to empty.  Returns whether it did."""
        return self._idle.wait(timeout)

    # -- the sender's side -----------------------------------------------

    def _run(self) -> None:
        while True:
            taken = [self._queue.get()]
            taken.extend(self._take_more())
            # The sentinel can be picked up mid-batch, not just on its
            # own: a producer racing shutdown puts events behind it.
            # Missing it here would leave the sender running forever.
            stopping = any(entry is _STOP for entry in taken)

            self._deliver(
                [entry for entry in taken if isinstance(entry, Event)]
            )
            for _ in taken:
                self._queue.task_done()
            if self._queue.empty():
                self._idle.set()
            if stopping:
                return

    def _take_more(self) -> list[object]:
        """Drain what is already waiting, up to a batch."""
        more: list[object] = []
        while len(more) + 1 < self.batch_size:
            try:
                more.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return more

    def _deliver(self, events: list[Event]) -> None:
        if not events:
            return
        for publisher in self.publishers:
            try:
                publisher.publish(events)
            except Exception as exc:
                # One bad delivery is not a reason to stop watching.
                self.stats.errors += 1
                self.stats.last_error = repr(exc)
                LOG.exception("publisher %r failed", publisher)
            else:
                self.stats.delivered += len(events)

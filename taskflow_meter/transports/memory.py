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

"""In-process transports: keep the events, or fold them into a store."""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Sequence

from taskflow_meter.datasource.base import WritableDataSource
from taskflow_meter.events import Event
from taskflow_meter.transports.base import Publisher

#: Events retained before the oldest are dropped.
DEFAULT_MAX_EVENTS = 10000


class MemoryTransport(Publisher):
    """Holds events in a bounded buffer for something else to drain.

    Useful for tests and for handing a batch to another thread in the
    same process.  Bounded because an undrained buffer is a leak with a
    slower fuse than a crash.
    """

    name = "memory"

    def __init__(self, *, max_events: int = DEFAULT_MAX_EVENTS) -> None:
        if max_events < 1:
            msg = "max_events must be at least 1"
            raise ValueError(msg)
        self._events: deque[Event] = deque(maxlen=max_events)
        self._lock = threading.Lock()
        self.dropped = 0

    def publish(self, events: Sequence[Event]) -> None:
        with self._lock:
            room = self._events.maxlen or 0
            overflow = max(0, len(self._events) + len(events) - room)
            self.dropped += overflow
            self._events.extend(events)

    def drain(self) -> tuple[Event, ...]:
        """Take everything buffered so far."""
        with self._lock:
            taken = tuple(self._events)
            self._events.clear()
            return taken

    def peek(self) -> tuple[Event, ...]:
        with self._lock:
            return tuple(self._events)

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)


class DataSourcePublisher(Publisher):
    """Folds events straight into a writable datasource.

    The whole in-process path: a listener produces events and the API
    can serve them from the same process, with nothing on the wire.
    """

    name = "datasource"

    def __init__(self, sink: WritableDataSource) -> None:
        self.sink = sink

    def publish(self, events: Sequence[Event]) -> None:
        self.sink.apply_many(events)

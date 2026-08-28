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

"""Server-sent events: framing, and the cursor that drives a stream.

The framing is shared by both adapters.  The cursor is deliberately
synchronous and pull-based -- it answers "what is new since the last
call?" -- so the ASGI adapter can drive it from an event loop and the
WSGI one from a thread without either owning the logic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field

from taskflow_meter.api import serializers
from taskflow_meter.datasource.base import DataSource
from taskflow_meter.events import Event

#: Note the absence of ``connection: keep-alive``.  It is a hop-by-hop
#: header, which PEP 3333 forbids an application from emitting -- and
#: wsgiref rightly refuses to send.  Connection persistence is the
#: server's business, not ours.
SSE_HEADERS: tuple[tuple[str, str], ...] = (
    ("content-type", "text/event-stream; charset=utf-8"),
    ("cache-control", "no-cache"),
    # nginx buffers proxied responses by default, which turns a live
    # stream into one long silence followed by everything at once.
    ("x-accel-buffering", "no"),
)

#: Sent once, telling a browser how long to wait before reconnecting.
DEFAULT_RETRY_MS = 3000


def frame(
    data: str,
    *,
    event: str | None = None,
    event_id: int | str | None = None,
    retry: int | None = None,
) -> bytes:
    """Render one SSE frame.

    Multi-line data is split across ``data:`` lines, as the protocol
    requires -- a raw newline would end the frame early and truncate
    whatever followed it.
    """
    lines: list[str] = []
    if event is not None:
        lines.append(f"event: {event}")
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if retry is not None:
        lines.append(f"retry: {retry}")
    lines.extend(f"data: {line}" for line in data.split("\n"))
    return ("\n".join(lines) + "\n\n").encode()


def comment(text: str = "") -> bytes:
    """A frame clients ignore, used to keep the connection alive."""
    return f": {text}\n\n".encode()


@dataclass(slots=True)
class EventCursor:
    """Tracks one client's position in one run's event stream."""

    reader: DataSource
    run_id: str
    since_seq: int = 0
    batch_limit: int = 200
    #: Set once the flow has finished and its events have been drained.
    complete: bool = field(default=False, init=False)

    def opening(self) -> bytes:
        """The first bytes on the wire.

        A retry hint plus a comment, so proxies that wait for output
        before forwarding headers let the response through immediately.
        """
        return frame("", retry=DEFAULT_RETRY_MS) + comment("stream open")

    def poll(self) -> list[bytes]:
        """Return frames for everything new since the last call."""
        page = self.reader.events_since(
            self.run_id, since_seq=self.since_seq, limit=self.batch_limit
        )
        frames: list[bytes] = []

        if page.truncated:
            # The client's next event was evicted before it got here.
            # Telling it beats letting it stitch a gap into a history it
            # believes is continuous.
            frames.append(
                frame(
                    json.dumps(
                        {
                            "reason": "events_evicted",
                            "resume_from": page.oldest_seq,
                        },
                        separators=(",", ":"),
                    ),
                    event="gap",
                )
            )

        for item in page.events:
            frames.append(self._event_frame(item))
        self.since_seq = page.next_seq

        if not page.events and self._flow_finished():
            frames.append(frame("{}", event="end"))
            self.complete = True
        return frames

    def heartbeat(self) -> bytes:
        return comment("keep-alive")

    def _event_frame(self, item: Event) -> bytes:
        return frame(
            json.dumps(serializers.event(item), separators=(",", ":")),
            event=str(item.kind),
            event_id=item.seq,
        )

    def _flow_finished(self) -> bool:
        snapshot = self.reader.get_flow(self.run_id)
        return snapshot is not None and snapshot.is_finished


@dataclass(frozen=True, slots=True)
class StreamResponse:
    """A response an adapter drives, rather than one it just writes.

    Carries the headers and the cursor; how the polling loop is run is
    the adapter's business, because an event loop and a worker thread
    want to do it differently.
    """

    cursor: EventCursor
    status: int = 200
    headers: tuple[tuple[str, str], ...] = SSE_HEADERS

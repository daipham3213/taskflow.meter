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

"""A plain ASGI 3 callable, mountable anywhere.

Nothing here is Starlette-aware beyond one rule it also follows: the
path to route on is ``scope["path"]`` with ``root_path`` removed *only
when it is genuinely a prefix*.  That covers modern Starlette (which
extends ``root_path`` and leaves ``path`` whole), older versions (which
stripped ``path`` instead), and a server given ``--root-path`` where the
prefix never appears in ``path`` at all.

Startup is the other thing a mounted app has to get right.  **A mounted
ASGI app never receives the lifespan scope** -- the host router handles
it at the root and does not forward it -- so the meter is started
lazily on the first request as well as from lifespan, and neither path
minds the other having gone first.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from typing import Any

from taskflow_meter.api.dispatch import Dispatcher
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import MeterResponse
from taskflow_meter.api.http import split_path
from taskflow_meter.api.service import MeterService
from taskflow_meter.api.sse import StreamResponse
from taskflow_meter.meter import Meter

LOG = logging.getLogger(__name__)

Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]

#: How often a live stream looks for new events.
DEFAULT_STREAM_INTERVAL = 1.0

#: How long a quiet stream waits before sending a keep-alive comment.
DEFAULT_HEARTBEAT = 15.0

#: Honoured when a reverse proxy strips a prefix the app never sees.
PREFIX_HEADER = "x-forwarded-prefix"


class ASGIApp:
    """Serves the meter over ASGI, standalone or mounted."""

    def __init__(
        self,
        meter: Meter,
        *,
        service: MeterService | None = None,
        stream_interval: float = DEFAULT_STREAM_INTERVAL,
        heartbeat: float = DEFAULT_HEARTBEAT,
    ) -> None:
        self.meter = meter
        self.service = service or MeterService(meter)
        self.dispatcher = Dispatcher(self.service)
        self.stream_interval = stream_interval
        self.heartbeat = heartbeat

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        kind = scope["type"]
        if kind == "lifespan":
            await self._lifespan(receive, send)
            return
        if kind == "http":
            await self._http(scope, receive, send)
            return
        if kind == "websocket":
            # Declining cleanly beats leaving the handshake hanging.
            await send({"type": "websocket.close", "code": 1000})
            return
        msg = f"unsupported ASGI scope: {kind!r}"
        raise RuntimeError(msg)

    # -- lifespan --------------------------------------------------------

    async def _lifespan(self, receive: Receive, send: Send) -> None:
        """Run the meter for as long as the server is up.

        Only ever reached when this app is the root one; a mount never
        sees these messages.
        """
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                try:
                    await asyncio.to_thread(self.meter.start)
                except Exception as exc:
                    LOG.exception("meter failed to start")
                    await send(
                        {
                            "type": "lifespan.startup.failed",
                            "message": repr(exc),
                        }
                    )
                    return
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                try:
                    await asyncio.to_thread(self.meter.stop)
                except Exception as exc:  # pragma: no cover - defensive
                    await send(
                        {
                            "type": "lifespan.shutdown.failed",
                            "message": repr(exc),
                        }
                    )
                    return
                await send({"type": "lifespan.shutdown.complete"})
                return

    # -- requests --------------------------------------------------------

    async def _http(self, scope: Scope, receive: Receive, send: Send) -> None:
        # The lazy half of the lifecycle: a mounted app is never told
        # when the server started, so the first request says so.
        await asyncio.to_thread(self.meter.ensure_started)

        request = build_request(scope)
        result = await asyncio.to_thread(self.dispatcher.dispatch, request)

        if isinstance(result, StreamResponse):
            await self._stream(result, receive, send)
            return
        await send_response(send, result)

    # -- streaming -------------------------------------------------------

    async def _stream(
        self, stream: StreamResponse, receive: Receive, send: Send
    ) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": stream.status,
                "headers": encode_headers(stream.headers),
            }
        )
        cursor = stream.cursor
        disconnected = asyncio.Event()
        watcher = asyncio.create_task(watch_disconnect(receive, disconnected))
        quiet = 0.0
        try:
            await send_chunk(send, cursor.opening())
            while not disconnected.is_set():
                frames = await asyncio.to_thread(cursor.poll)
                for chunk in frames:
                    # Re-checked per frame: a client that leaves partway
                    # through a large batch should not have the rest of
                    # it written at them.
                    if disconnected.is_set():
                        break
                    await send_chunk(send, chunk)
                if cursor.complete:
                    break

                quiet = 0.0 if frames else quiet + self.stream_interval
                if quiet >= self.heartbeat:
                    await send_chunk(send, cursor.heartbeat())
                    quiet = 0.0

                # Sleep, but wake immediately if the client leaves.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        disconnected.wait(), self.stream_interval
                    )
        finally:
            watcher.cancel()
            if not disconnected.is_set():
                # An empty final chunk is what tells the server the
                # response is over; without it the client waits forever.
                await send({"type": "http.response.body", "body": b""})


async def watch_disconnect(
    receive: Receive, disconnected: asyncio.Event
) -> None:
    """Set ``disconnected`` when the client goes away."""
    try:
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected.set()
                return
    except asyncio.CancelledError:  # pragma: no cover - shutdown path
        raise


async def send_chunk(send: Send, body: bytes) -> None:
    await send({"type": "http.response.body", "body": body, "more_body": True})


async def send_response(send: Send, response: MeterResponse) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": response.status,
            "headers": encode_headers(response.headers),
        }
    )
    await send({"type": "http.response.body", "body": response.body})


def encode_headers(
    headers: tuple[tuple[str, str], ...],
) -> list[tuple[bytes, bytes]]:
    return [(key.encode(), value.encode()) for key, value in headers]


def decode_headers(
    raw: Iterable[tuple[bytes, bytes]] | None,
) -> dict[str, str]:
    return {
        key.decode("latin-1").lower(): value.decode("latin-1")
        for key, value in raw or ()
    }


def build_request(scope: Scope) -> MeterRequest:
    """Turn an ASGI scope into a framework-neutral request."""
    root_path = scope.get("root_path", "")
    path = scope.get("path", "/")
    headers = decode_headers(scope.get("headers"))
    query_string = scope.get("query_string", b"").decode("latin-1")
    return MeterRequest.from_query_string(
        query_string,
        method=scope.get("method", "GET"),
        path=split_path(path, root_path),
        prefix=headers.get(PREFIX_HEADER, "") + root_path,
        headers=headers,
    )

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

"""A minimal ASGI client, so the tests need no web framework either.

Drives the app the way a server would -- build a scope, feed it
messages, collect what it sends back -- which is also the only way to
prove the app works without one.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from dataclasses import field
from typing import Any


@dataclass
class Response:
    status: int = 0
    headers: dict[str, str] = field(default_factory=dict)
    chunks: list[bytes] = field(default_factory=list)

    @property
    def body(self) -> bytes:
        return b"".join(self.chunks)

    @property
    def text(self) -> str:
        return self.body.decode()

    def json(self) -> Any:
        return json.loads(self.text)

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())


def build_scope(
    path: str,
    *,
    method: str = "GET",
    root_path: str = "",
    query_string: str = "",
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "root_path": root_path,
        "query_string": query_string.encode(),
        "headers": [
            (key.lower().encode(), value.encode())
            for key, value in (headers or {}).items()
        ],
    }


async def call(
    app: Any,
    scope: dict[str, Any],
    *,
    incoming: list[dict[str, Any]] | None = None,
    max_chunks: int | None = None,
) -> Response:
    """Run one request against ``app`` and collect the response.

    ``max_chunks`` disconnects after that many body chunks, which is how
    a streaming response is ended without waiting for one that never
    finishes on its own.
    """
    response = Response()
    queue: list[dict[str, Any]] = list(
        incoming or [{"type": "http.request", "body": b""}]
    )
    disconnect = asyncio.Event()

    async def receive() -> dict[str, Any]:
        if queue:
            return queue.pop(0)
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            response.status = message["status"]
            response.headers = {
                key.decode().lower(): value.decode()
                for key, value in message["headers"]
            }
        elif message["type"] == "http.response.body":
            body = message.get("body", b"")
            if body:
                response.chunks.append(body)
            if max_chunks is not None and len(response.chunks) >= max_chunks:
                disconnect.set()

    await app(scope, receive, send)
    return response


def request(
    app: Any,
    path: str,
    *,
    timeout: float = 10.0,
    **kwargs: Any,
) -> Response:
    """Synchronous convenience wrapper around :func:`call`."""
    scope_keys = {"method", "root_path", "query_string", "headers"}
    scope = build_scope(
        path, **{k: v for k, v in kwargs.items() if k in scope_keys}
    )
    rest = {k: v for k, v in kwargs.items() if k not in scope_keys}

    async def run() -> Response:
        return await asyncio.wait_for(call(app, scope, **rest), timeout)

    return asyncio.run(run())


async def lifespan_cycle(app: Any) -> list[dict[str, Any]]:
    """Send startup and shutdown, and return what the app replied."""
    sent: list[dict[str, Any]] = []
    queue = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]

    async def receive() -> dict[str, Any]:
        return queue.pop(0)

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app({"type": "lifespan"}, receive, send)
    return sent

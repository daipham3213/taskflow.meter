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

"""A minimal WSGI client, matching the ASGI one in shape.

Same idea: build the environ a server would, call the app, collect what
comes back -- so the two adapters can be driven identically and their
answers compared.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from io import BytesIO
from typing import Any


@dataclass
class Response:
    status: int = 0
    reason: str = ""
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


def build_environ(
    path: str,
    *,
    method: str = "GET",
    script_name: str = "",
    query_string: str = "",
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    environ: dict[str, Any] = {
        "REQUEST_METHOD": method,
        "SCRIPT_NAME": script_name,
        "PATH_INFO": path,
        "QUERY_STRING": query_string,
        "SERVER_NAME": "testserver",
        "SERVER_PORT": "80",
        "SERVER_PROTOCOL": "HTTP/1.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": BytesIO(b""),
        "wsgi.errors": BytesIO(),
        "wsgi.multithread": True,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
    }
    for name, value in (headers or {}).items():
        environ[f"HTTP_{name.upper().replace('-', '_')}"] = value
    return environ


def request(
    app: Any,
    path: str,
    *,
    max_chunks: int | None = None,
    **kwargs: Any,
) -> Response:
    """Run one request and collect the response.

    ``max_chunks`` closes the iterable after that many chunks, which is
    how a server behaves when the client goes away -- and the only way
    to end a stream that would otherwise run forever.
    """
    response = Response()

    def start_response(status: str, headers: list[tuple[str, str]]) -> None:
        code, _, reason = status.partition(" ")
        response.status = int(code)
        response.reason = reason
        response.headers = {key.lower(): value for key, value in headers}

    body = app(build_environ(path, **kwargs), start_response)
    try:
        for chunk in body:
            if chunk:
                response.chunks.append(chunk)
            if max_chunks is not None and len(response.chunks) >= max_chunks:
                break
    finally:
        closer = getattr(body, "close", None)
        if closer is not None:
            closer()
    return response

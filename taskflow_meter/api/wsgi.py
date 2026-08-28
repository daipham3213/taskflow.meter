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

"""A plain WSGI callable, mountable anywhere.

The same service and dispatcher the ASGI app uses, so the two cannot
answer the same request differently -- a parity suite compares them byte
for byte.

Mount handling is simpler here than under ASGI: WSGI has always split
``SCRIPT_NAME`` from ``PATH_INFO``, so there is one convention rather
than three.  Streaming is the harder half.  A synchronous worker holds
one thread for as long as an SSE response stays open, so a deployment
serving streams from this callable wants gevent or eventlet workers --
or should let clients poll ``/events?since_seq=`` instead, which costs
nothing while idle.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from http import HTTPStatus
from typing import Any

from taskflow_meter.api.dispatch import Dispatcher
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.service import MeterService
from taskflow_meter.api.sse import StreamResponse
from taskflow_meter.meter import Meter

LOG = logging.getLogger(__name__)

Environ = dict[str, Any]
StartResponse = Callable[[str, list[tuple[str, str]]], Any]

DEFAULT_STREAM_INTERVAL = 1.0
DEFAULT_HEARTBEAT = 15.0

#: Honoured when a reverse proxy strips a prefix the app never sees.
PREFIX_HEADER = "HTTP_X_FORWARDED_PREFIX"


class WSGIApp:
    """Serves the meter over WSGI, standalone or mounted."""

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

    def __call__(
        self, environ: Environ, start_response: StartResponse
    ) -> Iterable[bytes]:
        # WSGI has no lifespan at all, so the first request is the only
        # place to notice that nobody started the meter.
        self.meter.ensure_started()

        result = self.dispatcher.dispatch(build_request(environ))
        if isinstance(result, StreamResponse):
            return self._stream(result, start_response)

        start_response(status_line(result.status), list(result.headers))
        return [result.body]

    def _stream(
        self, stream: StreamResponse, start_response: StartResponse
    ) -> Iterator[bytes]:
        start_response(status_line(stream.status), list(stream.headers))
        return self._frames(stream)

    def _frames(self, stream: StreamResponse) -> Iterator[bytes]:
        """Yield frames until the flow ends or the client goes away.

        A generator, so that when the server closes the iterable the
        ``GeneratorExit`` lands here and the loop stops rather than
        polling a datasource nobody is listening to.
        """
        cursor = stream.cursor
        quiet = 0.0
        try:
            yield cursor.opening()
            while True:
                frames = cursor.poll()
                yield from frames
                if cursor.complete:
                    return

                quiet = 0.0 if frames else quiet + self.stream_interval
                if quiet >= self.heartbeat:
                    yield cursor.heartbeat()
                    quiet = 0.0
                time.sleep(self.stream_interval)
        except GeneratorExit:
            LOG.debug("client closed the stream for run %s", cursor.run_id)
            raise


def status_line(status: int) -> str:
    """Render ``200`` as ``"200 OK"``, which is what WSGI expects."""
    try:
        return f"{status} {HTTPStatus(status).phrase}"
    except ValueError:  # pragma: no cover - defensive
        return f"{status} Unknown"


def build_request(environ: Environ) -> MeterRequest:
    """Turn a WSGI environ into a framework-neutral request."""
    # The server split these for us; PATH_INFO is empty when the request
    # lands exactly on the mount point.
    script_name = environ.get("SCRIPT_NAME", "")
    path = environ.get("PATH_INFO", "") or "/"
    return MeterRequest.from_query_string(
        environ.get("QUERY_STRING", ""),
        method=environ.get("REQUEST_METHOD", "GET"),
        path=path,
        prefix=environ.get(PREFIX_HEADER, "") + script_name,
        headers=request_headers(environ),
    )


def request_headers(environ: Environ) -> dict[str, str]:
    """Recover header names from the CGI-style keys WSGI hands over."""
    headers = {
        key[5:].replace("_", "-").lower(): value
        for key, value in environ.items()
        if key.startswith("HTTP_")
    }
    for key, name in (
        ("CONTENT_TYPE", "content-type"),
        ("CONTENT_LENGTH", "content-length"),
    ):
        # These two lose their HTTP_ prefix on the way into WSGI.
        if key in environ:
            headers[name] = environ[key]
    return headers

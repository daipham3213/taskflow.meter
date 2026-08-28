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

"""The development server, over a real socket.

Everything else drives the callables directly.  This one runs the thing
``taskflow-meter serve`` actually starts and talks to it with an HTTP
client, which is the only way to catch what a real server does to a
response -- status lines, header casing, chunked bodies, the lot.
"""

from __future__ import annotations

import contextlib
import json
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler
from wsgiref.simple_server import make_server

import pytest
from taskflow.patterns import linear_flow

from taskflow_meter import states
from taskflow_meter.cli import ThreadingWSGIServer
from taskflow_meter.cli import build_app
from tests.conftest import ProgressingTask
from tests.conftest import make_logbook
from tests.conftest import run_flow

TIMEOUT = 30.0


class QuietHandler(WSGIRequestHandler):
    """The default handler logs every request to stderr."""

    def log_message(
        self,
        format: str,  # noqa: A002 - the name is the base class's
        *args: Any,
    ) -> None:
        pass


@contextlib.contextmanager
def running(app: Any) -> Iterator[str]:
    """Serve ``app`` on a free port for the duration of the block."""
    with make_server(
        "127.0.0.1",
        0,
        app,
        server_class=ThreadingWSGIServer,
        handler_class=QuietHandler,
    ) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address[:2]
        try:
            yield f"http://{host!s}:{port}"
        finally:
            server.shutdown()
            thread.join(TIMEOUT)


def get(url: str) -> tuple[int, Any]:
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


@pytest.fixture
def served(backend: Any, sqlite_url: str) -> Iterator[tuple[str, str]]:
    """A finished flow in a logbook, and a server reading it."""
    book, flow_detail = make_logbook(backend)
    flow = linear_flow.Flow("demo-flow").add(
        ProgressingTask("first", steps=(0.5,)),
        ProgressingTask("second"),
    )
    run_flow(backend, flow, book, flow_detail)

    app = build_app(sqlite_url, poll=True, interval=0.05)
    with app.meter, running(app) as base:
        app.meter.poll_once()
        yield base, flow_detail.uuid


def test_health_is_served(served: tuple[str, str]) -> None:
    base, _run_id = served
    status, body = get(f"{base}/healthz")
    assert status == 200
    assert body["status"] == "ok"
    assert body["poller"]["polls"] >= 1


def test_a_real_flow_is_visible_over_http(
    served: tuple[str, str],
) -> None:
    base, run_id = served
    status, body = get(f"{base}/api/v1/flows")
    assert status == 200

    (flow,) = body["flows"]
    assert flow["run_id"] == run_id
    assert flow["state"] == states.SUCCESS
    assert flow["completion"] == pytest.approx(1.0)


def test_a_flow_detail_over_http(served: tuple[str, str]) -> None:
    base, run_id = served
    status, body = get(f"{base}/api/v1/flows/{run_id}")
    assert status == 200
    assert [atom["name"] for atom in body["atoms"]] == ["first", "second"]
    assert all(atom["finished"] for atom in body["atoms"])


def test_the_advertised_links_are_followable(
    served: tuple[str, str],
) -> None:
    base, _run_id = served
    _status, listing = get(f"{base}/api/v1/flows")
    links = listing["flows"][0]["links"]

    for name in ("self", "atoms", "events"):
        status, _body = get(f"{base}{links[name]}")
        assert status == 200, f"{name} link was not followable"


def test_the_event_history_is_served(served: tuple[str, str]) -> None:
    base, run_id = served
    status, body = get(f"{base}/api/v1/flows/{run_id}/events")
    assert status == 200
    kinds = [event["kind"] for event in body["events"]]
    assert "flow_state" in kinds
    assert "atom_state" in kinds
    assert body["truncated"] is False


def test_a_live_stream_is_served(served: tuple[str, str]) -> None:
    # A finished flow, so the stream ends on its own rather than
    # needing the client to give up on it.
    base, run_id = served
    with urllib.request.urlopen(
        f"{base}/api/v1/flows/{run_id}/stream", timeout=TIMEOUT
    ) as response:
        assert response.status == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        body = response.read().decode()

    assert "retry: " in body
    assert "event: flow_state" in body
    assert body.rstrip().endswith("data: {}")


def test_an_unknown_flow_is_a_404_over_http(
    served: tuple[str, str],
) -> None:
    base, _run_id = served
    status, body = get(f"{base}/api/v1/flows/nope")
    assert status == 404
    assert body["error"]["title"] == "Not Found"


def test_reading_without_polling_declines_the_event_endpoints(
    backend: Any, sqlite_url: str, tmp_path: Path
) -> None:
    book, flow_detail = make_logbook(backend)
    flow = linear_flow.Flow("demo-flow").add(ProgressingTask("only"))
    run_flow(backend, flow, book, flow_detail)

    app = build_app(sqlite_url, poll=False, interval=1.0)
    with app.meter, running(app) as base:
        status, body = get(f"{base}/api/v1/flows")
        assert status == 200
        # No poller, so no history -- and the payload says so by not
        # advertising a stream it cannot serve.
        assert "stream" not in body["flows"][0]["links"]

        status, body = get(f"{base}/api/v1/flows/{flow_detail.uuid}/events")
        assert status == 501
        assert "history" in body["error"]["detail"]

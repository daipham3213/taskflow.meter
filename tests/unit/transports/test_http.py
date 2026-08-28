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

"""The webhook transport, against a real HTTP server."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from typing import Any
from typing import ClassVar

import pytest

from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.transports.http import HTTPTransport


class Recorder(BaseHTTPRequestHandler):
    """Records what it was posted, and answers however told to."""

    posts: ClassVar[list[Any]] = []
    statuses: ClassVar[list[int]] = []

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", 0))
        Recorder.posts.append(json.loads(self.rfile.read(length)))
        status = Recorder.statuses.pop(0) if Recorder.statuses else 204
        self.send_response(status)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


@pytest.fixture
def server() -> Iterator[str]:
    Recorder.posts = []
    Recorder.statuses = []
    httpd = HTTPServer(("127.0.0.1", 0), Recorder)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = httpd.server_address[:2]
        yield f"http://{host!s}:{port}"
    finally:
        httpd.shutdown()
        thread.join(10)
        httpd.server_close()


def make_events(count: int = 2) -> list[Event]:
    return [
        Event(
            run_id="run-1",
            seq=seq,
            ts=float(seq),
            kind=EventKind.ATOM_PROGRESS,
            atom_name="a",
            progress=0.5,
        )
        for seq in range(1, count + 1)
    ]


def test_a_batch_is_posted_as_one_request(server: str) -> None:
    HTTPTransport(server).publish(make_events(3))

    assert len(Recorder.posts) == 1
    assert [event["seq"] for event in Recorder.posts[0]["events"]] == [
        1,
        2,
        3,
    ]


def test_the_payload_round_trips(server: str) -> None:
    HTTPTransport(server).publish(make_events(1))

    (sent,) = Recorder.posts[0]["events"]
    assert Event.from_dict(sent) == make_events(1)[0]


def test_a_server_error_is_retried(server: str) -> None:
    Recorder.statuses = [503, 500]
    HTTPTransport(server, retries=2, backoff=0.0).publish(make_events(1))

    assert len(Recorder.posts) == 3


def test_retries_run_out(server: str) -> None:
    Recorder.statuses = [503, 503, 503]
    transport = HTTPTransport(server, retries=2, backoff=0.0)

    with pytest.raises(OSError, match="503"):
        transport.publish(make_events(1))
    assert len(Recorder.posts) == 3


def test_a_rejected_request_is_not_retried(server: str) -> None:
    # A 400 will be just as wrong the second time.
    Recorder.statuses = [400]
    transport = HTTPTransport(server, retries=3, backoff=0.0)

    with pytest.raises(OSError, match="400"):
        transport.publish(make_events(1))
    assert len(Recorder.posts) == 1


def test_an_unreachable_endpoint_raises_after_retrying() -> None:
    transport = HTTPTransport(
        "http://127.0.0.1:1/nowhere", retries=1, backoff=0.0, timeout=0.5
    )
    with pytest.raises(OSError, match=r"refused|unreachable|Errno"):
        transport.publish(make_events(1))


def test_custom_headers_are_sent(server: str) -> None:
    HTTPTransport(server, headers={"authorization": "Bearer x"}).publish(
        make_events(1)
    )
    assert Recorder.posts


def test_negative_retries_are_rejected() -> None:
    with pytest.raises(ValueError, match="negative"):
        HTTPTransport("http://example.invalid", retries=-1)

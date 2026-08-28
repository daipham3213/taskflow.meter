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

"""The WSGI callable."""

from __future__ import annotations

import pytest

from taskflow_meter import states
from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.api.wsgi import build_request
from taskflow_meter.api.wsgi import request_headers
from taskflow_meter.api.wsgi import status_line
from taskflow_meter.meter import Meter
from tests import wsgi_client
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource


@pytest.fixture
def source() -> FakeSource:
    return FakeSource(
        make_flow(
            state=states.RUNNING,
            atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
        )
    )


@pytest.fixture
def app(source: FakeSource) -> WSGIApp:
    meter = Meter(source, interval=0.01)
    built = WSGIApp(meter, stream_interval=0.01, heartbeat=0.02)
    meter.poll_once()
    return built


# -- the environ ---------------------------------------------------------


def test_status_lines_carry_their_reason() -> None:
    assert status_line(200) == "200 OK"
    assert status_line(404) == "404 Not Found"
    assert status_line(204) == "204 No Content"


def test_headers_are_recovered_from_cgi_keys() -> None:
    headers = request_headers(
        {
            "HTTP_LAST_EVENT_ID": "7",
            "HTTP_X_FORWARDED_PREFIX": "/svc",
            # These two lose their HTTP_ prefix on the way into WSGI.
            "CONTENT_TYPE": "application/json",
            "CONTENT_LENGTH": "0",
            "SERVER_NAME": "ignored",
        }
    )
    assert headers == {
        "last-event-id": "7",
        "x-forwarded-prefix": "/svc",
        "content-type": "application/json",
        "content-length": "0",
    }


def test_the_mount_comes_from_script_name() -> None:
    # WSGI has always split these, so there is one convention here
    # rather than the three ASGI has accumulated.
    request = build_request(
        {
            "REQUEST_METHOD": "GET",
            "SCRIPT_NAME": "/meter",
            "PATH_INFO": "/api/v1/flows",
            "QUERY_STRING": "limit=2",
        }
    )
    assert request.path == "/api/v1/flows"
    assert request.prefix == "/meter"
    assert request.get("limit") == "2"


def test_a_request_landing_on_the_mount_point_itself() -> None:
    # Servers report PATH_INFO as empty, which is not a valid path.
    request = build_request({"SCRIPT_NAME": "/meter", "PATH_INFO": ""})
    assert request.path == "/"


def test_a_proxy_prefix_is_prepended_to_the_mount() -> None:
    request = build_request(
        {
            "SCRIPT_NAME": "/meter",
            "PATH_INFO": "/healthz",
            "HTTP_X_FORWARDED_PREFIX": "/svc",
        }
    )
    assert request.prefix == "/svc/meter"


# -- routing -------------------------------------------------------------


def test_health(app: WSGIApp) -> None:
    response = wsgi_client.request(app, "/healthz")
    assert response.status == 200
    assert response.reason == "OK"
    assert response.json()["status"] == "ok"


def test_listing_flows(app: WSGIApp) -> None:
    response = wsgi_client.request(app, "/api/v1/flows")
    assert [f["run_id"] for f in response.json()["flows"]] == ["run-1"]


def test_a_flow_detail(app: WSGIApp) -> None:
    response = wsgi_client.request(app, "/api/v1/flows/run-1")
    assert [a["name"] for a in response.json()["atoms"]] == ["a"]


def test_an_unknown_flow_is_a_json_404(app: WSGIApp) -> None:
    response = wsgi_client.request(app, "/api/v1/flows/nope")
    assert response.status == 404
    assert response.json()["error"]["title"] == "Not Found"


def test_the_wrong_verb(app: WSGIApp) -> None:
    response = wsgi_client.request(app, "/api/v1/flows", method="DELETE")
    assert response.status == 405
    assert response.header("allow") == "GET, HEAD, OPTIONS"


def test_head_and_options(app: WSGIApp) -> None:
    head = wsgi_client.request(app, "/healthz", method="HEAD")
    assert head.body == b""
    assert head.header("content-length") is not None

    options = wsgi_client.request(app, "/healthz", method="OPTIONS")
    assert options.status == 204


def test_links_are_relative_to_the_mount(app: WSGIApp) -> None:
    response = wsgi_client.request(
        app, "/api/v1/flows", script_name="/deep/prefix"
    )
    links = response.json()["flows"][0]["links"]
    assert links["self"] == "/deep/prefix/api/v1/flows/run-1"


def test_the_meter_starts_on_the_first_request(
    source: FakeSource,
) -> None:
    # WSGI has no lifespan at all, so this is the only chance.
    meter = Meter(source, interval=0.01)
    app = WSGIApp(meter)
    assert not meter.running
    try:
        assert wsgi_client.request(app, "/healthz").status == 200
        assert meter.running
    finally:
        meter.stop()


# -- streaming -----------------------------------------------------------


def test_a_stream_of_a_finished_flow_ends_itself(
    app: WSGIApp, source: FakeSource
) -> None:
    source.set(
        make_flow(
            state=states.SUCCESS,
            atoms=(make_atom("a", state=states.SUCCESS, progress=1.0),),
        )
    )
    app.meter.poll_once()

    response = wsgi_client.request(app, "/api/v1/flows/run-1/stream")
    assert response.status == 200
    content_type = response.header("content-type")
    assert content_type is not None
    assert content_type.startswith("text/event-stream")
    assert "event: end" in response.text


def test_a_stream_stops_when_the_server_closes_it(app: WSGIApp) -> None:
    # A generator, so closing the iterable lands a GeneratorExit inside
    # the loop rather than leaving it polling for nobody.
    response = wsgi_client.request(
        app, "/api/v1/flows/run-1/stream", max_chunks=2
    )
    assert len(response.chunks) == 2
    assert "event: end" not in response.text


def test_a_quiet_stream_sends_heartbeats(app: WSGIApp) -> None:
    response = wsgi_client.request(
        app,
        "/api/v1/flows/run-1/stream",
        query_string="since_seq=99",
        max_chunks=3,
    )
    assert ": keep-alive" in response.text


def test_a_stream_of_an_unknown_flow_is_a_404(app: WSGIApp) -> None:
    response = wsgi_client.request(app, "/api/v1/flows/nope/stream")
    assert response.status == 404

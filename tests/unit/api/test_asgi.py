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

"""The ASGI callable, driven the way a server would drive it."""

from __future__ import annotations

import asyncio

import pytest

from taskflow_meter import states
from taskflow_meter.api.asgi import ASGIApp
from taskflow_meter.api.asgi import build_request
from taskflow_meter.meter import Meter
from tests import asgi_client
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
def app(source: FakeSource) -> ASGIApp:
    meter = Meter(source, interval=0.01)
    built = ASGIApp(meter, stream_interval=0.01, heartbeat=0.02)
    meter.poll_once()
    return built


# -- scopes --------------------------------------------------------------


def test_the_scope_becomes_a_neutral_request() -> None:
    request = build_request(
        {
            "type": "http",
            "method": "GET",
            "path": "/meter/api/v1/flows",
            "root_path": "/meter",
            "query_string": b"limit=2",
            "headers": [(b"Last-Event-ID", b"7")],
        }
    )
    assert request.path == "/api/v1/flows"
    assert request.prefix == "/meter"
    assert request.get("limit") == "2"
    # Header names arrive in whatever case the client sent.
    assert request.headers["last-event-id"] == "7"


def test_a_proxy_prefix_is_prepended_to_the_mount() -> None:
    # The proxy stripped a prefix this app will never see in its scope.
    request = build_request(
        {
            "type": "http",
            "method": "GET",
            "path": "/meter/healthz",
            "root_path": "/meter",
            "headers": [(b"x-forwarded-prefix", b"/svc")],
        }
    )
    assert request.prefix == "/svc/meter"


def test_an_unknown_scope_type_is_refused(app: ASGIApp) -> None:
    async def run() -> None:
        await app({"type": "ftp"}, None, None)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="unsupported ASGI scope"):
        asyncio.run(run())


def test_a_websocket_is_declined_cleanly(app: ASGIApp) -> None:
    # Leaving the handshake hanging would be worse than saying no.
    sent: list[dict[str, object]] = []

    async def run() -> None:
        await app(
            {"type": "websocket"},
            None,  # type: ignore[arg-type]
            lambda message: _record(sent, message),
        )

    asyncio.run(run())
    assert sent == [{"type": "websocket.close", "code": 1000}]


async def _record(sink: list[dict[str, object]], message: object) -> None:
    sink.append(message)  # type: ignore[arg-type]


# -- routing -------------------------------------------------------------


def test_health(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/healthz")
    assert response.status == 200
    assert response.json()["status"] == "ok"
    assert response.header("content-type") == "application/json"


def test_listing_flows(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/api/v1/flows")
    assert response.status == 200
    assert [f["run_id"] for f in response.json()["flows"]] == ["run-1"]


def test_query_parameters_reach_the_service(app: ASGIApp) -> None:
    response = asgi_client.request(
        app, "/api/v1/flows", query_string=f"state={states.SUCCESS}"
    )
    assert response.json()["flows"] == []


def test_a_flow_detail(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/api/v1/flows/run-1")
    assert [a["name"] for a in response.json()["atoms"]] == ["a"]


def test_an_unknown_flow_is_a_json_404(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/api/v1/flows/nope")
    assert response.status == 404
    assert response.json()["error"]["title"] == "Not Found"


def test_an_unknown_endpoint_under_our_prefix_is_ours_to_answer(
    app: ASGIApp,
) -> None:
    # Raising into the host application would turn a wrong URL into
    # somebody else's 500.
    response = asgi_client.request(app, "/api/v1/nonsense")
    assert response.status == 404
    assert "no such endpoint" in response.json()["error"]["detail"]


def test_the_wrong_verb_gets_an_allow_header(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/api/v1/flows", method="DELETE")
    assert response.status == 405
    assert response.header("allow") == "GET, HEAD, OPTIONS"


def test_head_returns_the_headers_of_the_get_without_its_body(
    app: ASGIApp,
) -> None:
    head = asgi_client.request(app, "/healthz", method="HEAD")
    get = asgi_client.request(app, "/healthz")
    assert head.status == 200
    assert head.body == b""
    # Reporting zero here would misdescribe the GET.
    assert head.header("content-length") == get.header("content-length")


def test_options_advertises_what_the_path_accepts(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/api/v1/flows", method="OPTIONS")
    assert response.status == 204
    assert response.header("allow") == "GET, HEAD, OPTIONS"


def test_options_on_an_unknown_path_is_still_a_404(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/nope", method="OPTIONS")
    assert response.status == 404


def test_a_bad_query_value_is_a_json_400(app: ASGIApp) -> None:
    response = asgi_client.request(
        app, "/api/v1/flows", query_string="limit=lots"
    )
    assert response.status == 400
    assert "must be an integer" in response.json()["error"]["detail"]


# -- mounting ------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "root_path"),
    [
        ("/healthz", ""),
        # Modern Starlette leaves the path whole and extends root_path.
        ("/meter/healthz", "/meter"),
        # Older Starlette stripped the path itself.
        ("/healthz", "/meter"),
        ("/deep/nested/mount/healthz", "/deep/nested/mount"),
    ],
)
def test_the_app_answers_at_any_mount_point(
    app: ASGIApp, path: str, root_path: str
) -> None:
    response = asgi_client.request(app, path, root_path=root_path)
    assert response.status == 200


def test_links_are_relative_to_the_mount(app: ASGIApp) -> None:
    response = asgi_client.request(
        app, "/deep/prefix/api/v1/flows", root_path="/deep/prefix"
    )
    links = response.json()["flows"][0]["links"]
    assert links["self"] == "/deep/prefix/api/v1/flows/run-1"


def test_a_link_from_a_mount_routes_back_to_a_real_endpoint(
    app: ASGIApp,
) -> None:
    # The property that keeps a mounted deployment navigable.
    listing = asgi_client.request(
        app, "/meter/api/v1/flows", root_path="/meter"
    )
    self_link = listing.json()["flows"][0]["links"]["self"]

    followed = asgi_client.request(app, self_link, root_path="/meter")
    assert followed.status == 200
    assert followed.json()["run_id"] == "run-1"


# -- lifespan ------------------------------------------------------------


def test_lifespan_runs_the_meter(source: FakeSource) -> None:
    meter = Meter(source, interval=0.01)
    app = ASGIApp(meter)

    async def run() -> list[dict[str, object]]:
        return await asgi_client.lifespan_cycle(app)

    sent = asyncio.run(run())
    assert [message["type"] for message in sent] == [
        "lifespan.startup.complete",
        "lifespan.shutdown.complete",
    ]
    assert not meter.running


def test_an_unrecognised_lifespan_message_is_ignored(
    source: FakeSource,
) -> None:
    # Servers send startup and shutdown; anything else is not ours to
    # interpret, and crashing on it would take the host down with us.
    meter = Meter(source, interval=0.01)
    app = ASGIApp(meter)
    sent: list[dict[str, object]] = []
    queue: list[dict[str, object]] = [
        {"type": "lifespan.something.new"},
        {"type": "lifespan.startup"},
        {"type": "lifespan.shutdown"},
    ]

    async def run() -> None:
        await app(
            {"type": "lifespan"},
            lambda: _pop(queue),
            lambda message: _record(sent, message),
        )

    asyncio.run(run())
    assert [message["type"] for message in sent] == [
        "lifespan.startup.complete",
        "lifespan.shutdown.complete",
    ]


async def _pop(queue: list[dict[str, object]]) -> dict[str, object]:
    return queue.pop(0)


def test_a_meter_that_cannot_start_fails_the_lifespan(
    source: FakeSource,
) -> None:
    meter = Meter(source, interval=0.01)

    def explode() -> None:
        msg = "no database"
        raise RuntimeError(msg)

    meter.start = explode  # type: ignore[method-assign]

    async def run() -> list[dict[str, object]]:
        return await asgi_client.lifespan_cycle(ASGIApp(meter))

    sent = asyncio.run(run())
    assert sent[0]["type"] == "lifespan.startup.failed"
    assert "no database" in str(sent[0]["message"])


def test_a_mounted_app_starts_the_meter_on_the_first_request(
    source: FakeSource,
) -> None:
    # A mount never receives the lifespan scope, so the first request is
    # the only chance to notice nobody started us.
    meter = Meter(source, interval=0.01)
    app = ASGIApp(meter)
    assert not meter.running
    try:
        assert asgi_client.request(app, "/healthz").status == 200
        assert meter.running
    finally:
        meter.stop()


# -- streaming -----------------------------------------------------------


def test_a_stream_sends_frames_then_ends(app: ASGIApp) -> None:
    source = app.meter.source
    assert isinstance(source, FakeSource)
    source.set(
        make_flow(
            state=states.SUCCESS,
            atoms=(make_atom("a", state=states.SUCCESS, progress=1.0),),
        )
    )
    app.meter.poll_once()

    response = asgi_client.request(app, "/api/v1/flows/run-1/stream")
    assert response.status == 200
    content_type = response.header("content-type")
    assert content_type is not None
    assert content_type.startswith("text/event-stream")

    text = response.text
    assert "retry: " in text
    assert "event: flow_state" in text
    assert "event: atom_progress" in text
    # A finished flow closes the stream rather than idling forever.
    assert text.rstrip().endswith("data: {}")
    assert "event: end" in text


def test_a_stream_stops_when_the_client_disconnects(
    app: ASGIApp,
) -> None:
    # Delivered through receive(), which is how a server reports it.
    # The exact chunk count is not the guarantee -- the loop only learns
    # of the disconnect at an await point -- but terminating is.
    response = asgi_client.request(
        app,
        "/api/v1/flows/run-1/stream",
        incoming=[
            {"type": "http.request", "body": b""},
            {"type": "http.disconnect"},
        ],
        max_chunks=50,
    )
    assert response.status == 200
    assert "event: end" not in response.text


def test_a_stream_does_not_hang_when_the_client_stops_reading(
    app: ASGIApp,
) -> None:
    # asgi_client.request() times out rather than blocking forever, so
    # returning at all is the assertion here.
    response = asgi_client.request(
        app, "/api/v1/flows/run-1/stream", max_chunks=2, timeout=10.0
    )
    assert len(response.chunks) >= 2


def test_a_running_flow_keeps_the_stream_open(app: ASGIApp) -> None:
    # The flow never finishes, so only the disconnect ends this.
    response = asgi_client.request(
        app, "/api/v1/flows/run-1/stream", max_chunks=4
    )
    assert "event: end" not in response.text


def test_a_quiet_stream_sends_heartbeats(app: ASGIApp) -> None:
    # Everything is already drained, so what arrives next is keep-alive.
    response = asgi_client.request(
        app,
        "/api/v1/flows/run-1/stream",
        query_string="since_seq=99",
        max_chunks=3,
    )
    assert ": keep-alive" in response.text


def test_a_stream_of_an_unknown_flow_is_a_404(app: ASGIApp) -> None:
    response = asgi_client.request(app, "/api/v1/flows/nope/stream")
    assert response.status == 404

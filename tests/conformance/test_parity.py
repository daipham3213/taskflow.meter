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

"""The two callables must answer identically, byte for byte.

M4's whole point.  Both adapters wrap the same service through the same
dispatcher, so a difference here means one of them grew logic of its
own -- which is exactly how two adapters drift until the less-tested one
is quietly wrong.

Lives outside the mirrored unit tree because its subject is the pair,
not either module.
"""

from __future__ import annotations

import pytest

from taskflow_meter import states
from taskflow_meter.api.asgi import ASGIApp
from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.meter import Meter
from tests import asgi_client
from tests import wsgi_client
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource

#: Every shared route, at a mount and at the root.
PATHS = [
    "/healthz",
    "/api/v1/flows",
    "/api/v1/flows/run-1",
    "/api/v1/flows/run-1/atoms",
    "/api/v1/flows/run-1/events",
    "/api/v1/flows/nope",
    "/api/v1/flows/nope/atoms",
    "/api/v1/nonsense",
]

PREFIXES = ["", "/meter", "/deep/nested/prefix"]


@pytest.fixture
def meter() -> Meter:
    """One meter behind both apps, so any difference is the adapter's."""
    source = FakeSource(
        make_flow(
            state=states.SUCCESS,
            book_id="book-1",
            book_name="nightly",
            atoms=(
                make_atom("alpha", state=states.SUCCESS, progress=1.0),
                make_atom("beta", state=states.SUCCESS, progress=1.0),
            ),
        )
    )
    built = Meter(source)
    built.poll_once()
    return built


@pytest.fixture
def apps(meter: Meter) -> tuple[ASGIApp, WSGIApp]:
    return (
        ASGIApp(meter, stream_interval=0.01, heartbeat=0.05),
        WSGIApp(meter, stream_interval=0.01, heartbeat=0.05),
    )


def compare_headers(left: dict[str, str], right: dict[str, str]) -> None:
    # Both should describe the payload the same way; neither adds
    # transport-specific noise of its own.
    interesting = {"content-type", "content-length", "allow"}
    assert {k: v for k, v in left.items() if k in interesting} == {
        k: v for k, v in right.items() if k in interesting
    }


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("prefix", PREFIXES)
def test_the_two_callables_agree(
    apps: tuple[ASGIApp, WSGIApp], path: str, prefix: str
) -> None:
    asgi, wsgi = apps
    from_asgi = asgi_client.request(asgi, f"{prefix}{path}", root_path=prefix)
    from_wsgi = wsgi_client.request(wsgi, path, script_name=prefix)

    assert from_asgi.status == from_wsgi.status
    assert from_asgi.body == from_wsgi.body
    compare_headers(from_asgi.headers, from_wsgi.headers)


@pytest.mark.parametrize(
    "query",
    [
        "",
        "limit=1",
        "state=SUCCESS",
        "state=RUNNING",
        "limit=lots",
        "limit=0",
        "marker=gone",
    ],
)
def test_they_agree_on_query_handling_including_the_errors(
    apps: tuple[ASGIApp, WSGIApp], query: str
) -> None:
    asgi, wsgi = apps
    from_asgi = asgi_client.request(asgi, "/api/v1/flows", query_string=query)
    from_wsgi = wsgi_client.request(wsgi, "/api/v1/flows", query_string=query)

    assert from_asgi.status == from_wsgi.status
    assert from_asgi.body == from_wsgi.body


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS", "DELETE"])
def test_they_agree_on_every_verb(
    apps: tuple[ASGIApp, WSGIApp], method: str
) -> None:
    asgi, wsgi = apps
    from_asgi = asgi_client.request(asgi, "/api/v1/flows", method=method)
    from_wsgi = wsgi_client.request(wsgi, "/api/v1/flows", method=method)

    assert from_asgi.status == from_wsgi.status
    assert from_asgi.body == from_wsgi.body
    compare_headers(from_asgi.headers, from_wsgi.headers)


def test_they_agree_on_a_resumed_event_page(
    apps: tuple[ASGIApp, WSGIApp],
) -> None:
    asgi, wsgi = apps
    path = "/api/v1/flows/run-1/events"
    from_asgi = asgi_client.request(asgi, path, query_string="since_seq=2")
    from_wsgi = wsgi_client.request(wsgi, path, query_string="since_seq=2")
    assert from_asgi.body == from_wsgi.body


def test_they_agree_on_a_whole_stream(
    apps: tuple[ASGIApp, WSGIApp],
) -> None:
    """A finished flow makes the stream deterministic end to end.

    Same opening bytes, same event frames in the same order, same
    terminating event -- the framing is shared, so any difference would
    be one adapter mangling it on the way out.
    """
    asgi, wsgi = apps
    path = "/api/v1/flows/run-1/stream"
    from_asgi = asgi_client.request(asgi, path)
    from_wsgi = wsgi_client.request(wsgi, path)

    assert from_asgi.status == from_wsgi.status
    assert from_asgi.body == from_wsgi.body
    assert b"event: end" in from_asgi.body
    compare_headers(from_asgi.headers, from_wsgi.headers)


def test_they_agree_when_the_source_keeps_no_history() -> None:
    source = FakeSource(make_flow(state=states.RUNNING))
    source.supports_events = False
    meter = Meter(source, poll=False)
    asgi, wsgi = ASGIApp(meter), WSGIApp(meter)

    for path in ("/api/v1/flows", "/api/v1/flows/run-1/events"):
        from_asgi = asgi_client.request(asgi, path)
        from_wsgi = wsgi_client.request(wsgi, path)
        assert from_asgi.status == from_wsgi.status
        assert from_asgi.body == from_wsgi.body


def test_a_link_from_one_is_followable_on_the_other(
    apps: tuple[ASGIApp, WSGIApp],
) -> None:
    # Mounted deployments stay navigable whichever callable is serving.
    asgi, wsgi = apps
    listing = asgi_client.request(
        asgi, "/meter/api/v1/flows", root_path="/meter"
    )
    link = listing.json()["flows"][0]["links"]["self"]
    assert link.startswith("/meter")

    followed = wsgi_client.request(
        wsgi, link[len("/meter") :], script_name="/meter"
    )
    assert followed.status == 200
    assert followed.json()["run_id"] == "run-1"

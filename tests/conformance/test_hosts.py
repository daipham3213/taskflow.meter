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

"""Every host framework must answer identically.

The promise this project makes is that it plugs into whatever you
already run.  This is the suite that keeps it honest: the same meter is
built into the bare callables, FastAPI, Flask, Pecan and Django, at the
root and at two mount depths, and every one of them has to return the
same bytes.

Anything host-specific -- extra response headers, how a wrong verb is
rejected, whether OPTIONS is answered by the host or by us -- is
deliberately outside the comparison.  Registering our routes in a
host's router means its behaviour applies to them, which is the whole
point of doing it that way.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit

import pytest

from taskflow_meter import states
from taskflow_meter.api.router import Outcome
from taskflow_meter.api.router import Router
from taskflow_meter.api.routes import build_routes
from taskflow_meter.api.service import MeterService
from taskflow_meter.meter import Meter
from tests import hosts
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.hosts import HOSTS
from tests.hosts import Host
from tests.unit.test_poller import FakeSource

PREFIXES = ["", "/meter", "/deep/nested/prefix"]

PATHS = [
    "/healthz",
    "/api/v1/flows",
    "/api/v1/flows/run-1",
    "/api/v1/flows/run-1/atoms",
    "/api/v1/flows/run-1/events",
    "/api/v1/flows/nope",
]


def build_meter() -> Meter:
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
    meter = Meter(source)
    meter.poll_once()
    return meter


def fetch(host: Host, path: str, prefix: str, **kwargs: Any) -> Any:
    app = host.build(build_meter(), prefix)
    return hosts.call(host, app, path, prefix, **kwargs)


def reference(path: str, prefix: str, **kwargs: Any) -> Any:
    """What the bare WSGI callable says, which everything must match."""
    bare = next(h for h in HOSTS if h.name == "bare-wsgi")
    return fetch(bare, path, prefix, **kwargs)


@pytest.mark.parametrize("host", HOSTS, ids=lambda h: h.name)
@pytest.mark.parametrize("prefix", PREFIXES)
@pytest.mark.parametrize("path", PATHS)
def test_every_host_returns_the_same_bytes(
    host: Host, prefix: str, path: str
) -> None:
    expected = reference(path, prefix)
    actual = fetch(host, path, prefix)

    assert actual.status == expected.status, host.name
    assert actual.body == expected.body, host.name


@pytest.mark.parametrize("host", HOSTS, ids=lambda h: h.name)
@pytest.mark.parametrize("prefix", PREFIXES)
def test_every_host_builds_links_under_its_own_mount(
    host: Host, prefix: str
) -> None:
    """A link that ignores the mount point is a link to nowhere.

    Each host is told where it lives differently -- root_path, a
    SCRIPT_NAME, a router prefix, a url_prefix, an include() -- and all
    of them have to arrive at the same answer.
    """
    payload = json.loads(fetch(host, "/api/v1/flows", prefix).body)
    links = payload["flows"][0]["links"]

    router = Router(build_routes(MeterService(build_meter())))
    for name, link in links.items():
        assert link.startswith(prefix), f"{host.name}: {name} -> {link}"
        route_path = urlsplit(link[len(prefix) :]).path
        match = router.match("GET", route_path)
        assert match.outcome is Outcome.MATCHED, (
            f"{host.name}: {name} -> {link} matches no route"
        )


@pytest.mark.parametrize("host", HOSTS, ids=lambda h: h.name)
@pytest.mark.parametrize("prefix", PREFIXES)
def test_an_advertised_link_is_followable_on_its_own_host(
    host: Host, prefix: str
) -> None:
    app = host.build(build_meter(), prefix)
    listing = hosts.call(host, app, "/api/v1/flows", prefix)
    link = json.loads(listing.body)["flows"][0]["links"]["self"]

    followed = hosts.call(
        host, app, link[len(prefix) :] if prefix else link, prefix
    )
    assert followed.status == 200, host.name
    assert json.loads(followed.body)["run_id"] == "run-1"


@pytest.mark.parametrize("host", HOSTS, ids=lambda h: h.name)
@pytest.mark.parametrize(
    "query", ["limit=1", "state=SUCCESS", "state=RUNNING", "limit=lots"]
)
def test_every_host_handles_queries_the_same_way(
    host: Host, query: str
) -> None:
    expected = reference("/api/v1/flows", "", query_string=query)
    actual = fetch(host, "/api/v1/flows", "", query_string=query)

    assert actual.status == expected.status, host.name
    assert actual.body == expected.body, host.name


@pytest.mark.parametrize("host", HOSTS, ids=lambda h: h.name)
def test_every_host_streams_the_same_frames(host: Host) -> None:
    # A finished flow makes the stream deterministic end to end.
    expected = reference("/api/v1/flows/run-1/stream", "")
    actual = fetch(host, "/api/v1/flows/run-1/stream", "")

    assert actual.status == expected.status, host.name
    assert actual.body == expected.body, host.name
    assert b"event: end" in actual.body, host.name

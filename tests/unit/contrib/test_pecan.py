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

"""Hosting the meter in a real Pecan controller tree.

Built with ``pecan.testing.load_test_app`` rather than mocked, because
the whole question is what Pecan does to the path on the way in.
"""

from __future__ import annotations

import json
from typing import Any

import pecan
import pytest
from pecan import make_app
from webtest import TestApp

from taskflow_meter import states
from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.contrib.pecan import MeterController
from taskflow_meter.contrib.pecan import _split
from taskflow_meter.meter import Meter
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource

MOUNT = "/taskflow-meter"


@pytest.fixture
def meter() -> Meter:
    source = FakeSource(
        make_flow(
            state=states.RUNNING,
            atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
        )
    )
    built = Meter(source)
    built.poll_once()
    return built


@pytest.fixture
def app(meter: Meter) -> Any:
    """A Pecan application with the meter mounted inside it."""
    controller = MeterController(WSGIApp(meter))

    class V1Controller:
        @pecan.expose()
        def index(self) -> str:
            return "the host's own API"

    class RootController:
        pass

    # Assigned after definition so the controller instance is shared.
    RootController.v1 = V1Controller()  # type: ignore[attr-defined]
    setattr(RootController, MOUNT.strip("/"), controller)

    return TestApp(make_app(RootController()))


def body(response: Any) -> Any:
    return json.loads(response.body)


# -- path arithmetic -----------------------------------------------------


@pytest.mark.parametrize(
    ("path_info", "remainder", "expected"),
    [
        ("/taskflow-meter", (), ("/taskflow-meter", "/")),
        ("/taskflow-meter/", (), ("/taskflow-meter", "/")),
        (
            "/taskflow-meter/healthz",
            ("healthz",),
            ("/taskflow-meter", "/healthz"),
        ),
        (
            "/a/b/taskflow-meter/api/v1/flows",
            ("api", "v1", "flows"),
            ("/a/b/taskflow-meter", "/api/v1/flows"),
        ),
    ],
)
def test_the_consumed_prefix_is_recovered_from_the_remainder(
    path_info: str, remainder: tuple[str, ...], expected: tuple[str, str]
) -> None:
    # Pecan does not rewrite PATH_INFO as it routes, so the mount point
    # has to be taken off the end.
    assert _split(path_info, remainder) == expected


def test_a_remainder_that_does_not_fit_is_not_guessed_at() -> None:
    # The two can disagree when a segment's encoding differs between the
    # raw path and the decoded remainder.  Route on what Pecan says is
    # left for us, and report no prefix: guessing one would produce
    # links pointing somewhere that does not exist.
    assert _split("/somewhere/else", ("api", "v1")) == ("", "/api/v1")


# -- through a real Pecan app --------------------------------------------


def test_the_hosts_own_routes_still_work(app: Any) -> None:
    assert b"the host's own API" in app.get("/v1/").body


def test_health_is_served_from_inside_pecan(app: Any) -> None:
    response = app.get(f"{MOUNT}/healthz")
    assert response.status_int == 200
    assert body(response)["status"] == "ok"


def test_a_flow_listing_is_served(app: Any) -> None:
    response = app.get(f"{MOUNT}/api/v1/flows")
    assert [f["run_id"] for f in body(response)["flows"]] == ["run-1"]


def test_a_nested_path_is_served(app: Any) -> None:
    response = app.get(f"{MOUNT}/api/v1/flows/run-1/atoms")
    assert [a["name"] for a in body(response)["atoms"]] == ["a"]


def test_query_parameters_survive_the_handoff(app: Any) -> None:
    response = app.get(f"{MOUNT}/api/v1/flows?state={states.SUCCESS}")
    assert body(response)["flows"] == []


def test_links_point_back_through_the_mount(app: Any) -> None:
    response = app.get(f"{MOUNT}/api/v1/flows")
    link = body(response)["flows"][0]["links"]["self"]
    assert link == f"{MOUNT}/api/v1/flows/run-1"


def test_an_advertised_link_is_followable(app: Any) -> None:
    listing = app.get(f"{MOUNT}/api/v1/flows")
    link = body(listing)["flows"][0]["links"]["self"]

    followed = app.get(link)
    assert followed.status_int == 200
    assert body(followed)["run_id"] == "run-1"


def test_our_404s_stay_ours(app: Any) -> None:
    # Pecan should not turn a wrong URL under our mount into its own
    # error page.
    response = app.get(f"{MOUNT}/api/v1/nonsense", expect_errors=True)
    assert response.status_int == 404
    assert body(response)["error"]["title"] == "Not Found"


def test_an_unknown_flow_is_our_404_too(app: Any) -> None:
    response = app.get(f"{MOUNT}/api/v1/flows/nope", expect_errors=True)
    assert response.status_int == 404
    assert "no flow with run id" in body(response)["error"]["detail"]


def test_the_wrong_verb_is_reported_with_allow(app: Any) -> None:
    response = app.delete(f"{MOUNT}/api/v1/flows", expect_errors=True)
    assert response.status_int == 405
    assert response.headers["Allow"] == "GET, HEAD, OPTIONS"


def test_the_event_history_is_served(app: Any) -> None:
    response = app.get(f"{MOUNT}/api/v1/flows/run-1/events")
    assert [e["seq"] for e in body(response)["events"]] == [1, 2, 3]


def test_the_mount_point_itself_is_our_404(app: Any) -> None:
    # Pecan normalises the bare mount point to a trailing slash first,
    # which is its business.  What matters is that the answer then comes
    # from us rather than from its error page: there is no route at our
    # root, so it is a 404 in our shape.
    assert app.get(MOUNT, expect_errors=True).status_int == 302

    response = app.get(f"{MOUNT}/", expect_errors=True)
    assert response.status_int == 404
    assert body(response)["error"]["title"] == "Not Found"

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

"""Routes registered in a Django project's URL conf."""

from __future__ import annotations

from typing import Any

import pytest

from taskflow_meter import states
from taskflow_meter.meter import Meter
from tests import hosts
from tests import wsgi_client
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource

hosts.configure_django()

from taskflow_meter.contrib.django import meter_urlpatterns  # noqa: E402
from taskflow_meter.contrib.django import to_django_route  # noqa: E402

MOUNT = "/taskflow"


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
    return hosts.django_host(meter, MOUNT)


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        # Relative: Django patterns hang off wherever they are included.
        ("/healthz", "healthz"),
        ("/api/v1/flows/{run_id}", "api/v1/flows/<str:run_id>"),
        (
            "/api/v1/flows/{run_id}/atoms",
            "api/v1/flows/<str:run_id>/atoms",
        ),
    ],
)
def test_our_templates_become_django_routes(
    template: str, expected: str
) -> None:
    assert to_django_route(template) == expected


def test_the_patterns_are_named_after_the_routes(meter: Meter) -> None:
    names = {pattern.name for pattern in meter_urlpatterns(meter)}
    assert {"health", "flows", "flow", "atoms", "events", "stream"} == names


def test_a_flow_listing_is_served(app: Any) -> None:
    response = wsgi_client.request(app, f"{MOUNT}/api/v1/flows")
    assert response.status == 200
    assert response.json()["flows"][0]["links"]["self"] == (
        f"{MOUNT}/api/v1/flows/run-1"
    )


def test_our_errors_are_still_ours(app: Any) -> None:
    response = wsgi_client.request(app, f"{MOUNT}/api/v1/flows/nope")
    assert response.status == 404
    assert response.json()["error"]["title"] == "Not Found"


def test_content_length_is_left_to_django(app: Any) -> None:
    # Setting our own would duplicate the header Django computes, and
    # some servers reject a response carrying two.
    response = wsgi_client.request(app, f"{MOUNT}/api/v1/flows")
    assert response.header("content-length") == str(len(response.body))


def test_the_meter_starts_on_the_first_request(meter: Meter) -> None:
    app = hosts.django_host(meter, MOUNT)
    assert not meter.running
    try:
        assert wsgi_client.request(app, f"{MOUNT}/healthz").status == 200
        assert meter.running
    finally:
        meter.stop()


def test_a_stream_is_served(meter: Meter) -> None:
    source = meter.source
    assert isinstance(source, FakeSource)
    source.set(
        make_flow(
            state=states.SUCCESS,
            atoms=(make_atom("a", state=states.SUCCESS, progress=1.0),),
        )
    )
    meter.poll_once()
    app = hosts.django_host(meter, MOUNT)

    response = wsgi_client.request(app, f"{MOUNT}/api/v1/flows/run-1/stream")
    assert response.status == 200
    content_type = response.header("content-type")
    assert content_type is not None
    assert content_type.startswith("text/event-stream")
    assert b"event: end" in response.body


def test_patterns_included_at_the_root(meter: Meter) -> None:
    app = hosts.django_host(meter, "")
    response = wsgi_client.request(app, "/api/v1/flows")
    assert response.json()["flows"][0]["links"]["self"] == (
        "/api/v1/flows/run-1"
    )


def test_a_script_name_is_included_in_links(meter: Meter) -> None:
    app = hosts.django_host(meter, MOUNT)
    response = wsgi_client.request(
        app, f"{MOUNT}/api/v1/flows", script_name="/svc"
    )
    assert response.json()["flows"][0]["links"]["self"] == (
        f"/svc{MOUNT}/api/v1/flows/run-1"
    )

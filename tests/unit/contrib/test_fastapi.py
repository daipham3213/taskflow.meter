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

"""Routes registered in a FastAPI application's own router."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException

from taskflow_meter import states
from taskflow_meter.contrib.fastapi import meter_router
from taskflow_meter.meter import Meter
from tests import asgi_client
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource

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
def app(meter: Meter) -> FastAPI:
    built = FastAPI()
    built.include_router(meter_router(meter), prefix=MOUNT)
    return built


def test_the_routes_appear_in_the_hosts_openapi_schema(
    app: FastAPI,
) -> None:
    # The reason to use a router rather than a mount: the host's schema
    # documents these endpoints alongside its own.
    paths = asgi_client.request(app, "/openapi.json").json()["paths"]
    assert f"{MOUNT}/api/v1/flows" in paths
    assert f"{MOUNT}/api/v1/flows/{{run_id}}" in paths


def test_the_routes_are_named(meter: Meter) -> None:
    router = meter_router(meter)
    names = {getattr(route, "name", None) for route in router.routes}
    assert {"health", "flows", "flow", "stream"} <= names


def test_the_hosts_dependencies_apply(meter: Meter) -> None:
    """The other reason: the host's auth guards our routes too."""

    def forbid() -> None:
        raise HTTPException(status_code=403, detail="nope")

    app = FastAPI()
    app.include_router(
        meter_router(meter), prefix=MOUNT, dependencies=[Depends(forbid)]
    )

    response = asgi_client.request(app, f"{MOUNT}/api/v1/flows")
    assert response.status == 403


def test_our_errors_are_still_ours(app: FastAPI) -> None:
    # The host matched the route, but an unknown flow is our 404 to
    # shape -- letting it escape would make it the host's 500.
    response = asgi_client.request(app, f"{MOUNT}/api/v1/flows/nope")
    assert response.status == 404
    assert response.json()["error"]["detail"].startswith("no flow")


def test_the_meter_starts_on_the_first_request(meter: Meter) -> None:
    app = FastAPI()
    app.include_router(meter_router(meter), prefix=MOUNT)
    assert not meter.running
    try:
        assert asgi_client.request(app, f"{MOUNT}/healthz").status == 200
        assert meter.running
    finally:
        meter.stop()


def test_a_stream_is_served(meter: Meter, app: FastAPI) -> None:
    source = meter.source
    assert isinstance(source, FakeSource)
    source.set(
        make_flow(
            state=states.SUCCESS,
            atoms=(make_atom("a", state=states.SUCCESS, progress=1.0),),
        )
    )
    meter.poll_once()

    response = asgi_client.request(app, f"{MOUNT}/api/v1/flows/run-1/stream")
    assert response.status == 200
    content_type = response.header("content-type")
    assert content_type is not None
    assert content_type.startswith("text/event-stream")
    assert "event: end" in response.text


def test_query_parameters_with_repeats_survive(app: FastAPI) -> None:
    response = asgi_client.request(
        app, f"{MOUNT}/api/v1/flows", query_string="limit=1&limit=2"
    )
    # The first wins, as everywhere else; what matters is that the
    # adapter passes a list rather than collapsing it early.
    assert response.status == 200


def test_a_router_without_a_prefix_works_at_the_root(
    meter: Meter,
) -> None:
    app = FastAPI()
    app.include_router(meter_router(meter))
    response = asgi_client.request(app, "/api/v1/flows")
    assert response.json()["flows"][0]["links"]["self"] == (
        "/api/v1/flows/run-1"
    )


def test_a_custom_service_is_used(meter: Meter) -> None:
    from taskflow_meter.api.service import MeterService

    class Loud(MeterService):
        def health(self, request: Any) -> Any:
            from taskflow_meter.api.http import MeterResponse

            return MeterResponse.json({"status": "custom"})

    app = FastAPI()
    app.include_router(meter_router(meter, service=Loud(meter)))
    assert asgi_client.request(app, "/healthz").json() == {"status": "custom"}

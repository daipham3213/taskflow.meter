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

"""The route table, and the links that have to agree with it."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit

import pytest

from taskflow_meter import states
from taskflow_meter.api import routes
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.router import Outcome
from taskflow_meter.api.router import Router
from taskflow_meter.api.service import MeterService
from taskflow_meter.meter import Meter
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource

PREFIX = "/deep/prefix"


@pytest.fixture
def service() -> MeterService:
    source = FakeSource(
        make_flow(
            state=states.RUNNING,
            atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
        )
    )
    meter = Meter(source)
    meter.poll_once()
    return MeterService(meter)


@pytest.fixture
def router(service: MeterService) -> Router:
    return Router(routes.build_routes(service))


def test_every_template_is_bound_to_a_handler(
    router: Router,
) -> None:
    assert {route.template for route in router.routes} == set(
        routes.templates()
    )
    assert all(callable(route.handler) for route in router.routes)


def test_route_names_are_unique(router: Router) -> None:
    names = [route.name for route in router.routes]
    assert len(names) == len(set(names))


def collect_links(payload: Any) -> list[str]:
    """Every string under any "links" object, at any depth."""
    found: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "links" and isinstance(value, dict):
                found.extend(str(item) for item in value.values())
            else:
                found.extend(collect_links(value))
    elif isinstance(payload, list):
        for item in payload:
            found.extend(collect_links(item))
    return found


@pytest.mark.parametrize(
    ("handler_name", "path_params"),
    [
        ("list_flows", {}),
        ("get_flow", {"run_id": "run-1"}),
        ("get_atoms", {"run_id": "run-1"}),
        ("get_events", {"run_id": "run-1"}),
    ],
)
def test_every_link_a_payload_emits_resolves_to_a_route(
    service: MeterService,
    router: Router,
    handler_name: str,
    path_params: dict[str, str],
) -> None:
    """The property that keeps a mounted deployment navigable.

    A renamed route that left the serialisers pointing at the old path
    would produce payloads full of links to nowhere, and nothing else in
    the suite would notice.
    """
    request = MeterRequest(
        path="/api/v1/flows", prefix=PREFIX, path_params=path_params
    )
    response = getattr(service, handler_name)(request)
    links = collect_links(json.loads(response.body))
    assert links, "the payload advertised no links at all"

    for link in links:
        assert link.startswith(PREFIX), link
        path = urlsplit(link[len(PREFIX) :]).path
        match = router.match("GET", path)
        assert match.outcome is Outcome.MATCHED, f"{link} matches nothing"

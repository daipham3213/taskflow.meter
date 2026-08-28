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

"""Path templates, in one place so links and routes cannot drift apart.

The serialisers build links from the same constants the router matches
on, so a renamed route cannot leave the payloads pointing somewhere that
no longer exists.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from taskflow_meter.api.router import Route

if TYPE_CHECKING:
    # Imported for typing only: service imports serializers, which
    # imports these templates, so a real import would close the loop.
    from taskflow_meter.api.service import MeterService

HEALTH = "/healthz"
FLOWS = "/api/v1/flows"
FLOW = "/api/v1/flows/{run_id}"
ATOMS = "/api/v1/flows/{run_id}/atoms"
EVENTS = "/api/v1/flows/{run_id}/events"
STREAM = "/api/v1/flows/{run_id}/stream"


def build_routes(service: MeterService) -> tuple[Route, ...]:
    """Bind the templates to a service's handlers."""
    return (
        Route("GET", HEALTH, service.health, name="health"),
        Route("GET", FLOWS, service.list_flows, name="flows"),
        Route("GET", FLOW, service.get_flow, name="flow"),
        Route("GET", ATOMS, service.get_atoms, name="atoms"),
        Route("GET", EVENTS, service.get_events, name="events"),
        Route("GET", STREAM, service.stream, name="stream"),
    )


def templates() -> Iterable[str]:
    return (HEALTH, FLOWS, FLOW, ATOMS, EVENTS, STREAM)

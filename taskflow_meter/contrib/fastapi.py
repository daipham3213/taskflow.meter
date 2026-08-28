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

"""Register the meter's routes in a FastAPI application's own router.

Mounting the raw ASGI callable is simpler and works everywhere, so
reach for this when the routes need to be *inside* the host app: its
authentication dependencies, its middleware, its exception handlers and
its OpenAPI schema all apply to a router, and none of them apply to a
mount::

    from taskflow_meter.contrib.fastapi import meter_router

    app.include_router(
        meter_router(meter),
        prefix="/taskflow",
        dependencies=[Depends(require_admin)],
    )

Our path templates already use FastAPI's ``{name}`` syntax, so they are
registered verbatim and the host's ``prefix`` is recovered from the
request rather than configured twice.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence

from fastapi import APIRouter
from fastapi import Request
from fastapi import Response
from fastapi.responses import StreamingResponse

from taskflow_meter.api import routes as route_table
from taskflow_meter.api.dispatch import Dispatcher
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import MeterResponse
from taskflow_meter.api.http import mount_prefix
from taskflow_meter.api.http import split_path
from taskflow_meter.api.router import Route
from taskflow_meter.api.service import MeterService
from taskflow_meter.api.sse import StreamResponse
from taskflow_meter.api.sse import aiter_frames
from taskflow_meter.meter import Meter

DEFAULT_STREAM_INTERVAL = 1.0
DEFAULT_HEARTBEAT = 15.0


def meter_router(
    meter: Meter,
    *,
    service: MeterService | None = None,
    stream_interval: float = DEFAULT_STREAM_INTERVAL,
    heartbeat: float = DEFAULT_HEARTBEAT,
    tags: Sequence[str] | None = None,
) -> APIRouter:
    """Build a router serving every endpoint the callables serve."""
    resolved = service or MeterService(meter)
    dispatcher = Dispatcher(resolved)
    router = APIRouter(tags=list(tags or ["taskflow-meter"]))

    for route in route_table.build_routes(resolved):
        router.add_api_route(
            route.template,
            _endpoint(meter, dispatcher, route, stream_interval, heartbeat),
            methods=[route.method],
            name=route.name,
            # The payloads are plain dictionaries built by our own
            # serialisers; letting FastAPI infer a response model would
            # only give it something to validate them against twice.
            response_class=Response,
        )
    return router


def _endpoint(
    meter: Meter,
    dispatcher: Dispatcher,
    route: Route,
    stream_interval: float,
    heartbeat: float,
) -> Callable[[Request], Awaitable[Response]]:
    async def endpoint(request: Request) -> Response:
        # A mounted app is never told the server started, and an
        # included router is no different.
        await asyncio.to_thread(meter.ensure_started)

        meter_request = _build_request(request, route)
        result = await asyncio.to_thread(dispatcher.run, route, meter_request)

        if isinstance(result, StreamResponse):
            return StreamingResponse(
                aiter_frames(
                    result.cursor,
                    interval=stream_interval,
                    heartbeat=heartbeat,
                ),
                status_code=result.status,
                headers=dict(result.headers),
            )
        return _response(result)

    return endpoint


def _build_request(request: Request, route: Route) -> MeterRequest:
    """Translate a Starlette request, recovering the router's prefix.

    ``include_router(prefix=...)`` puts the prefix in the path rather
    than in ``root_path``, so it is found by taking our own template off
    the end of what arrived.
    """
    params = {key: str(value) for key, value in request.path_params.items()}
    root_path = request.scope.get("root_path", "")
    path = split_path(request.scope.get("path", "/"), root_path)
    sub_path = route.template.format(**params)

    return MeterRequest(
        method=request.method,
        path=sub_path,
        prefix=root_path + mount_prefix(path, sub_path),
        query={
            key: request.query_params.getlist(key)
            for key in request.query_params
        },
        headers={key.lower(): value for key, value in request.headers.items()},
        path_params=params,
    )


def _response(result: MeterResponse) -> Response:
    return Response(
        content=result.body,
        status_code=result.status,
        headers=dict(result.headers),
    )

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

"""Register the meter's routes in a Django project's URL conf.

Mounting the WSGI callable beside Django works too; reach for this when
the routes need to be *inside* the project, so its middleware,
authentication and permission decorators apply to them::

    from django.urls import include, path
    from taskflow_meter.contrib.django import meter_urlpatterns

    urlpatterns = [
        path("taskflow/", include(meter_urlpatterns(meter))),
    ]

Django's converter syntax differs from ours -- ``<str:run_id>`` rather
than ``{run_id}`` -- so the templates are translated on the way in,
from the same table the router matches on.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from django.http import HttpRequest
from django.http import HttpResponse
from django.http import StreamingHttpResponse
from django.urls import URLPattern
from django.urls import path as django_path

from taskflow_meter.api import routes as route_table
from taskflow_meter.api.dispatch import Dispatcher
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import MeterResponse
from taskflow_meter.api.http import mount_prefix
from taskflow_meter.api.router import Route
from taskflow_meter.api.service import MeterService
from taskflow_meter.api.sse import StreamResponse
from taskflow_meter.api.sse import iter_frames
from taskflow_meter.meter import Meter

DEFAULT_STREAM_INTERVAL = 1.0
DEFAULT_HEARTBEAT = 15.0

_PARAM = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def meter_urlpatterns(
    meter: Meter,
    *,
    service: MeterService | None = None,
    stream_interval: float = DEFAULT_STREAM_INTERVAL,
    heartbeat: float = DEFAULT_HEARTBEAT,
) -> list[URLPattern]:
    """Build URL patterns serving every endpoint the callables serve."""
    resolved = service or MeterService(meter)
    dispatcher = Dispatcher(resolved)

    return [
        django_path(
            to_django_route(route.template),
            _view(meter, dispatcher, route, stream_interval, heartbeat),
            name=route.name,
        )
        for route in route_table.build_routes(resolved)
    ]


def to_django_route(template: str) -> str:
    """``/flows/{run_id}`` -> ``flows/<str:run_id>``.

    Django patterns are relative to wherever they are included, so the
    leading slash goes too.
    """
    return _PARAM.sub(r"<str:\1>", template).lstrip("/")


def _view(
    meter: Meter,
    dispatcher: Dispatcher,
    route: Route,
    stream_interval: float,
    heartbeat: float,
) -> Callable[..., HttpResponse | StreamingHttpResponse]:
    def view(
        request: HttpRequest, **params: Any
    ) -> HttpResponse | StreamingHttpResponse:
        meter.ensure_started()

        result = dispatcher.run(route, _build_request(request, route, params))
        if isinstance(result, StreamResponse):
            streaming = StreamingHttpResponse(
                iter_frames(
                    result.cursor,
                    interval=stream_interval,
                    heartbeat=heartbeat,
                ),
                status=result.status,
            )
            _apply_headers(streaming, result.headers)
            return streaming
        return _response(result)

    return view


def _build_request(
    request: HttpRequest, route: Route, params: dict[str, Any]
) -> MeterRequest:
    """Translate a Django request, recovering where it was included.

    ``path_info`` excludes ``SCRIPT_NAME`` but includes the prefix the
    patterns were included under, so the prefix is what is left when
    our own template is taken off the end.
    """
    text_params = {key: str(value) for key, value in params.items()}
    sub_path = route.template.format(**text_params)
    script_name = request.META.get("SCRIPT_NAME", "")

    return MeterRequest(
        method=request.method or "GET",
        path=sub_path,
        prefix=script_name + mount_prefix(request.path_info, sub_path),
        query=dict(request.GET.lists()),
        headers={key.lower(): value for key, value in request.headers.items()},
        path_params=text_params,
    )


def _response(result: MeterResponse) -> HttpResponse:
    response = HttpResponse(result.body, status=result.status)
    _apply_headers(response, result.headers)
    return response


def _apply_headers(
    response: HttpResponse | StreamingHttpResponse,
    headers: tuple[tuple[str, str], ...],
) -> None:
    """Copy our headers onto a Django response.

    Content-Length included: Django does not compute one itself, and
    its header mapping overwrites rather than appends, so there is no
    duplicate for a later middleware to trip over.  A streaming
    response never carries one to begin with.
    """
    for key, value in headers:
        response[key] = value

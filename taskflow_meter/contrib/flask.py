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

"""Register the meter's routes in a Flask application's own blueprint.

Mounting the WSGI callable with ``DispatcherMiddleware`` is simpler and
works everywhere; reach for this when the routes need to be *inside*
the host app, so its ``before_request`` hooks, error handlers and
authentication apply to them::

    from taskflow_meter.contrib.flask import meter_blueprint

    app.register_blueprint(meter_blueprint(meter), url_prefix="/taskflow")

Flask's rule syntax differs from ours -- ``<run_id>`` rather than
``{run_id}`` -- so the templates are translated on the way in, from the
same table the router matches on.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import flask
from flask import Blueprint
from flask import Response

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


def meter_blueprint(
    meter: Meter,
    *,
    service: MeterService | None = None,
    name: str = "taskflow_meter",
    stream_interval: float = DEFAULT_STREAM_INTERVAL,
    heartbeat: float = DEFAULT_HEARTBEAT,
) -> Blueprint:
    """Build a blueprint serving every endpoint the callables serve."""
    resolved = service or MeterService(meter)
    dispatcher = Dispatcher(resolved)
    blueprint = Blueprint(name, __name__)

    for route in route_table.build_routes(resolved):
        blueprint.add_url_rule(
            to_flask_rule(route.template),
            endpoint=route.name,
            view_func=_view(
                meter, dispatcher, route, stream_interval, heartbeat
            ),
            methods=[route.method],
        )
    return blueprint


def to_flask_rule(template: str) -> str:
    """``/flows/{run_id}`` -> ``/flows/<run_id>``."""
    return _PARAM.sub(r"<\1>", template)


def _view(
    meter: Meter,
    dispatcher: Dispatcher,
    route: Route,
    stream_interval: float,
    heartbeat: float,
) -> Callable[..., Response]:
    def view(**params: Any) -> Response:
        # Flask has no lifespan either, so the first request is where a
        # meter nobody started gets started.
        meter.ensure_started()

        result = dispatcher.run(route, _build_request(route, params))
        if isinstance(result, StreamResponse):
            return Response(
                iter_frames(
                    result.cursor,
                    interval=stream_interval,
                    heartbeat=heartbeat,
                ),
                status=result.status,
                headers=list(result.headers),
            )
        return _response(result)

    return view


def _build_request(route: Route, params: dict[str, Any]) -> MeterRequest:
    """Translate the active Flask request, recovering the url_prefix.

    ``request.path`` excludes ``SCRIPT_NAME`` but includes whatever
    prefix the blueprint was registered under, so the prefix is what is
    left when our own template is taken off the end.
    """
    request = flask.request
    text_params = {key: str(value) for key, value in params.items()}
    sub_path = route.template.format(**text_params)

    return MeterRequest(
        method=request.method,
        path=sub_path,
        prefix=request.script_root + mount_prefix(request.path, sub_path),
        query={key: request.args.getlist(key) for key in request.args},
        headers={key.lower(): value for key, value in request.headers.items()},
        path_params=text_params,
    )


def _response(result: MeterResponse) -> Response:
    return Response(
        result.body,
        status=result.status,
        headers=list(result.headers),
    )

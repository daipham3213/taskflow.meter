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

"""Routing and error rendering, shared by every adapter.

Whatever carries the bytes -- ASGI, WSGI, or a host framework's own
router -- the decision of which handler runs, what a wrong verb gets
back, and how an error is rendered belongs here.  Two adapters that each
made those decisions themselves would drift apart, and the difference
would only show up in whichever one had fewer tests.
"""

from __future__ import annotations

from dataclasses import replace

from taskflow_meter.api import routes as route_table
from taskflow_meter.api.http import ApiError
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import MeterResponse
from taskflow_meter.api.http import MethodNotAllowedError
from taskflow_meter.api.http import NotFoundError
from taskflow_meter.api.router import Outcome
from taskflow_meter.api.router import Route
from taskflow_meter.api.router import Router
from taskflow_meter.api.service import MeterService
from taskflow_meter.api.sse import StreamResponse

Result = MeterResponse | StreamResponse


class Dispatcher:
    """Turns a request into a response, or an error into one."""

    def __init__(
        self, service: MeterService, *, router: Router | None = None
    ) -> None:
        self.service = service
        self.router = router or Router(route_table.build_routes(service))

    def dispatch(self, request: MeterRequest) -> Result:
        """Route and run, rendering our own errors as JSON.

        Only :class:`ApiError` is caught.  Anything else is a bug, and
        belongs to whoever is hosting us -- swallowing it here would
        turn our crash into a puzzling 500 with no traceback.
        """
        try:
            return self._dispatch(request)
        except ApiError as error:
            return MeterResponse.from_error(error)

    def run(self, route: Route, request: MeterRequest) -> Result:
        """Invoke a handler the *host* has already matched.

        The contrib adapters register our routes in the host's router,
        so the host does the matching -- but a 404 for an unknown flow
        is still ours to shape.  Without this each adapter would
        re-implement the error rendering, and each would get it subtly
        different.
        """
        try:
            return self._invoke(route, request)
        except ApiError as error:
            return MeterResponse.from_error(error)

    def _invoke(self, route: Route, request: MeterRequest) -> Result:
        result: Result = route.handler(request)
        if request.method == "HEAD" and isinstance(result, MeterResponse):
            # Same headers, including content-length: a HEAD that
            # reported zero would misdescribe the GET.
            return replace(result, body=b"")
        return result

    def _dispatch(self, request: MeterRequest) -> Result:
        if request.method == "OPTIONS":
            return self._options(request)

        # HEAD is a GET whose body is thrown away, which is what makes a
        # health check with curl -I report the truth.
        wanted = "GET" if request.method == "HEAD" else request.method
        match = self.router.match(wanted, request.path)

        if match.outcome is Outcome.MATCHED:
            assert match.route is not None
            return self._invoke(
                match.route, replace(request, path_params=match.params)
            )

        if match.outcome is Outcome.METHOD_NOT_ALLOWED:
            raise MethodNotAllowedError(
                f"{request.method} is not allowed here",
                headers=(("allow", self._allow(match.allowed)),),
            )

        # Unknown paths under our prefix are ours to answer.  Raising
        # into the host application would turn a wrong URL into someone
        # else's 500.
        raise NotFoundError(f"no such endpoint: {request.path}")

    def _options(self, request: MeterRequest) -> MeterResponse:
        allowed: list[str] = []
        for route in self.router.routes:
            if self.router.match(route.method, request.path).outcome is (
                Outcome.MATCHED
            ):
                allowed.append(route.method)
        if not allowed:
            raise NotFoundError(f"no such endpoint: {request.path}")
        return MeterResponse(
            status=204,
            headers=(("allow", self._allow(tuple(allowed))),),
        )

    @staticmethod
    def _allow(methods: tuple[str, ...]) -> str:
        # HEAD and OPTIONS are handled here rather than by a route, so
        # they would otherwise be missing from what we advertise.
        return ", ".join(sorted({*methods, "HEAD", "OPTIONS"}))

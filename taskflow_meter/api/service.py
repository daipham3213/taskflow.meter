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

"""Every query the API can answer, with no HTTP machinery attached.

This is the source of truth.  The ASGI and WSGI callables are adapters
over it, and a host framework's own router can call the same handlers,
so the three can never disagree about what an endpoint returns.
"""

from __future__ import annotations

from taskflow_meter import __version__
from taskflow_meter.api import serializers
from taskflow_meter.api.http import BadRequestError
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import MeterResponse
from taskflow_meter.api.http import NotFoundError
from taskflow_meter.api.http import UnsupportedError
from taskflow_meter.api.sse import EventCursor
from taskflow_meter.api.sse import StreamResponse
from taskflow_meter.datasource.base import UnknownMarkerError
from taskflow_meter.meter import Meter
from taskflow_meter.models import FlowSnapshot

#: Refuse to serve more than this in one page, however much is asked
#: for: an unbounded limit is a full table scan someone can request.
MAX_LIMIT = 500

DEFAULT_FLOW_LIMIT = 50
DEFAULT_EVENT_LIMIT = 200


class MeterService:
    """Answers requests from a :class:`~taskflow_meter.meter.Meter`."""

    def __init__(self, meter: Meter, *, max_limit: int = MAX_LIMIT) -> None:
        self.meter = meter
        self.max_limit = max_limit

    # -- endpoints -------------------------------------------------------

    def health(
        self,
        request: MeterRequest,  # noqa: ARG002 - every handler takes one
    ) -> MeterResponse:
        poller = self.meter.poller
        return MeterResponse.json(
            {
                "status": "ok",
                "version": __version__,
                "running": self.meter.running,
                "supports_events": self.meter.supports_events,
                "poller": None
                if poller is None
                else {
                    "polls": poller.stats.polls,
                    "events": poller.stats.events,
                    "errors": poller.stats.errors,
                    "flows_seen": poller.stats.flows_seen,
                    "last_error": poller.stats.last_error,
                },
            }
        )

    def list_flows(self, request: MeterRequest) -> MeterResponse:
        limit = self._limit(request, DEFAULT_FLOW_LIMIT)
        try:
            page = self.meter.list_flows(
                state=request.get("state"),
                book_id=request.get("book_id"),
                limit=limit,
                marker=request.get("marker"),
            )
        except UnknownMarkerError as exc:
            # The run it named expired between pages.  A 400 tells the
            # client to restart the walk; silently starting over would
            # loop it through the first page forever.
            raise BadRequestError(str(exc)) from exc

        return MeterResponse.json(
            serializers.flow_page(
                page, request, with_events=self.meter.supports_events
            )
        )

    def get_flow(self, request: MeterRequest) -> MeterResponse:
        run_id = request.param("run_id")
        snapshot = self._flow_or_404(run_id)
        return MeterResponse.json(
            serializers.flow(
                snapshot,
                request,
                with_atoms=True,
                with_events=self.meter.supports_events,
            )
        )

    def get_atoms(self, request: MeterRequest) -> MeterResponse:
        run_id = request.param("run_id")
        atoms = self.meter.get_atoms(run_id)
        if atoms is None:
            raise NotFoundError(f"no flow with run id {run_id!r}")
        return MeterResponse.json(
            {
                "atoms": [serializers.atom(item) for item in atoms],
                "links": {"self": request.url(request.path)},
            }
        )

    def get_events(self, request: MeterRequest) -> MeterResponse:
        run_id = request.param("run_id")
        self._require_events()
        self._flow_or_404(run_id)
        page = self.meter.events_since(
            run_id,
            since_seq=request.get_int("since_seq", 0),
            limit=self._limit(request, DEFAULT_EVENT_LIMIT),
        )
        return MeterResponse.json(
            serializers.event_page(page, run_id, request)
        )

    def stream(self, request: MeterRequest) -> StreamResponse:
        run_id = request.param("run_id")
        self._require_events()
        self._flow_or_404(run_id)
        return StreamResponse(
            cursor=EventCursor(
                reader=self.meter.reader,
                run_id=run_id,
                since_seq=self._resume_point(request),
                batch_limit=self._limit(request, DEFAULT_EVENT_LIMIT),
            )
        )

    # -- shared checks ---------------------------------------------------

    def _flow_or_404(self, run_id: str) -> FlowSnapshot:
        snapshot = self.meter.get_flow(run_id)
        if snapshot is None:
            raise NotFoundError(f"no flow with run id {run_id!r}")
        return snapshot

    def _require_events(self) -> None:
        if not self.meter.supports_events:
            msg = (
                "this datasource keeps current state, not a history; "
                "pair it with a poller feeding a writable datasource "
                "to stream events"
            )
            raise UnsupportedError(msg)

    def _limit(self, request: MeterRequest, default: int) -> int:
        limit = request.get_int("limit", default)
        if limit < 1:
            msg = f"limit must be at least 1, got {limit}"
            raise BadRequestError(msg)
        return min(limit, self.max_limit)

    def _resume_point(self, request: MeterRequest) -> int:
        """Where to resume a stream from.

        ``Last-Event-ID`` is what a browser's EventSource sends by itself
        on reconnect, so honouring it is what makes a dropped connection
        recoverable rather than a hole in the client's history.  An
        explicit query parameter wins, for clients driving it by hand.
        """
        explicit = request.get("since_seq")
        if explicit is not None:
            return request.get_int("since_seq", 0)

        header = request.headers.get("last-event-id")
        if header is None:
            return 0
        try:
            return int(header)
        except ValueError:
            msg = f"Last-Event-ID must be an integer, got {header!r}"
            raise BadRequestError(msg) from None

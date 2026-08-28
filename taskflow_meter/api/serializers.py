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

"""Snapshots and events, rendered as JSON-ready dictionaries.

Every payload carries the links that reach the rest of the API, built
from the request's mount prefix rather than a hard-coded path, so the
same handler is correct at ``/`` and at ``/deep/prefix``.
"""

from __future__ import annotations

from typing import Any

from taskflow_meter.api import routes
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.events import Event
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot


def atom(snapshot: AtomSnapshot) -> dict[str, Any]:
    return {
        "name": snapshot.name,
        "uuid": snapshot.uuid,
        "type": snapshot.atom_type,
        "state": snapshot.state,
        "intention": snapshot.intention,
        "progress": snapshot.progress,
        "progress_details": snapshot.progress_details,
        "completion": snapshot.completion,
        "finished": snapshot.is_finished,
        "running": snapshot.is_running,
        "has_result": snapshot.has_result,
        "failure": snapshot.failure,
        "revert_failure": snapshot.revert_failure,
    }


def flow(
    snapshot: FlowSnapshot,
    request: MeterRequest,
    *,
    with_atoms: bool = False,
    with_events: bool = True,
) -> dict[str, Any]:
    """Render a flow.

    ``with_events`` is false when the datasource keeps no history, so
    the payload does not advertise a stream that would answer every
    request with silence.
    """
    run_id = snapshot.run_id
    links = {
        "self": request.url(routes.FLOW.format(run_id=run_id)),
        "atoms": request.url(routes.ATOMS.format(run_id=run_id)),
    }
    if with_events:
        links["events"] = request.url(routes.EVENTS.format(run_id=run_id))
        links["stream"] = request.url(routes.STREAM.format(run_id=run_id))

    payload: dict[str, Any] = {
        "run_id": run_id,
        "name": snapshot.name,
        "state": snapshot.state,
        "book_id": snapshot.book_id,
        "book_name": snapshot.book_name,
        "observed_at": snapshot.observed_at,
        "finished": snapshot.is_finished,
        # An unweighted mean of the atoms: taskflow offers nothing to
        # weight them by, so this indicates rather than estimates.
        "completion": snapshot.completion,
        "atom_count": len(snapshot.atoms),
        "state_counts": snapshot.state_counts,
        "meta": snapshot.meta,
        "links": links,
    }
    if with_atoms:
        payload["atoms"] = [
            atom(snapshot.atoms[name]) for name in snapshot.atom_names
        ]
    return payload


def flow_page(
    page: FlowPage, request: MeterRequest, *, with_events: bool = True
) -> dict[str, Any]:
    links = {"self": request.url(routes.FLOWS)}
    if page.next_marker is not None:
        links["next"] = request.url(routes.FLOWS, marker=page.next_marker)
    return {
        "flows": [
            flow(item, request, with_events=with_events) for item in page.items
        ],
        "next_marker": page.next_marker,
        "links": links,
    }


def event(item: Event) -> dict[str, Any]:
    return item.to_dict()


def event_page(
    page: EventPage, run_id: str, request: MeterRequest
) -> dict[str, Any]:
    template = routes.EVENTS.format(run_id=run_id)
    return {
        "events": [event(item) for item in page.events],
        "next_seq": page.next_seq,
        "oldest_seq": page.oldest_seq,
        # True means the caller's next expected event was already
        # evicted: there is a hole, and the snapshot must be re-read.
        "truncated": page.truncated,
        "links": {
            "self": request.url(template),
            "next": request.url(template, since_seq=page.next_seq),
        },
    }

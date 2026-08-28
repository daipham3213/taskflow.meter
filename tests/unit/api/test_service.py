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

"""Every query the API answers, without any HTTP machinery."""

from __future__ import annotations

import json
from typing import Any

import pytest

from taskflow_meter import states
from taskflow_meter.api.http import BadRequestError
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import NotFoundError
from taskflow_meter.api.http import UnsupportedError
from taskflow_meter.api.service import MAX_LIMIT
from taskflow_meter.api.service import MeterService
from taskflow_meter.api.sse import StreamResponse
from taskflow_meter.meter import Meter
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource


@pytest.fixture
def meter() -> Meter:
    source = FakeSource(
        make_flow(
            state=states.RUNNING,
            book_id="book-1",
            book_name="nightly",
            atoms=(
                make_atom("alpha", state=states.SUCCESS, progress=1.0),
                make_atom("beta", state=states.RUNNING, progress=0.5),
            ),
        )
    )
    built = Meter(source)
    built.poll_once()
    return built


@pytest.fixture
def service(meter: Meter) -> MeterService:
    return MeterService(meter)


def payload(response: Any) -> Any:
    return json.loads(response.body)


def get(path: str = "/", **kwargs: Any) -> MeterRequest:
    query = kwargs.pop("query_string", "")
    return MeterRequest.from_query_string(query, path=path, **kwargs)


# -- health --------------------------------------------------------------


def test_health_reports_the_poller(service: MeterService) -> None:
    body = payload(service.health(get("/healthz")))
    assert body["status"] == "ok"
    assert body["supports_events"] is True
    assert body["poller"]["polls"] == 1
    assert body["poller"]["errors"] == 0


def test_health_without_a_poller() -> None:
    meter = Meter(FakeSource(), poll=False)
    assert payload(MeterService(meter).health(get()))["poller"] is None


# -- listing -------------------------------------------------------------


def test_listing_flows(service: MeterService) -> None:
    body = payload(service.list_flows(get()))
    (flow,) = body["flows"]
    assert flow["run_id"] == "run-1"
    assert flow["state"] == states.RUNNING
    assert flow["book_name"] == "nightly"
    assert flow["atom_count"] == 2
    assert flow["completion"] == pytest.approx(0.75)
    assert flow["state_counts"] == {states.SUCCESS: 1, states.RUNNING: 1}
    # A summary carries no atoms; the detail endpoint does.
    assert "atoms" not in flow


def test_listing_links_reach_the_rest_of_the_api(
    service: MeterService,
) -> None:
    body = payload(service.list_flows(get(prefix="/meter")))
    links = body["flows"][0]["links"]
    assert links["self"] == "/meter/api/v1/flows/run-1"
    assert links["atoms"] == "/meter/api/v1/flows/run-1/atoms"
    assert links["stream"] == "/meter/api/v1/flows/run-1/stream"


def test_listing_filters_are_passed_through(service: MeterService) -> None:
    body = payload(
        service.list_flows(get(query_string=f"state={states.SUCCESS}"))
    )
    assert body["flows"] == []


def test_an_expired_marker_is_a_bad_request(
    service: MeterService,
) -> None:
    with pytest.raises(BadRequestError, match="marker"):
        service.list_flows(get(query_string="marker=gone"))


@pytest.mark.parametrize("limit", ["0", "-3"])
def test_a_useless_limit_is_rejected(
    service: MeterService, limit: str
) -> None:
    with pytest.raises(BadRequestError, match="at least 1"):
        service.list_flows(get(query_string=f"limit={limit}"))


def test_an_enormous_limit_is_capped_not_obeyed(
    service: MeterService,
) -> None:
    # An unbounded limit is a full scan any client could ask for.
    captured: dict[str, Any] = {}
    original = service.meter.list_flows

    def spy(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return original(**kwargs)

    service.meter.list_flows = spy  # type: ignore[method-assign]
    service.list_flows(get(query_string="limit=100000"))
    assert captured["limit"] == MAX_LIMIT


# -- one flow ------------------------------------------------------------


def test_a_flow_detail_includes_its_atoms(service: MeterService) -> None:
    request = get(path_params={"run_id": "run-1"})
    body = payload(service.get_flow(request))
    assert [atom["name"] for atom in body["atoms"]] == ["alpha", "beta"]
    beta = body["atoms"][1]
    assert beta["progress"] == pytest.approx(0.5)
    assert beta["completion"] == pytest.approx(0.5)
    assert beta["running"] is True


def test_an_unknown_flow_is_not_found(service: MeterService) -> None:
    with pytest.raises(NotFoundError, match="no flow with run id"):
        service.get_flow(get(path_params={"run_id": "nope"}))


def test_atoms_on_their_own(service: MeterService) -> None:
    body = payload(service.get_atoms(get(path_params={"run_id": "run-1"})))
    assert [atom["name"] for atom in body["atoms"]] == ["alpha", "beta"]


def test_atoms_of_an_unknown_flow(service: MeterService) -> None:
    with pytest.raises(NotFoundError):
        service.get_atoms(get(path_params={"run_id": "nope"}))


# -- events --------------------------------------------------------------


def test_events_are_returned_with_a_resume_point(
    service: MeterService,
) -> None:
    body = payload(service.get_events(get(path_params={"run_id": "run-1"})))
    assert [event["seq"] for event in body["events"]] == [1, 2, 3, 4, 5]
    assert body["next_seq"] == 5
    assert body["truncated"] is False
    assert "since_seq=5" in body["links"]["next"]


def test_events_can_be_resumed(service: MeterService) -> None:
    body = payload(
        service.get_events(
            get(query_string="since_seq=3", path_params={"run_id": "run-1"})
        )
    )
    assert [event["seq"] for event in body["events"]] == [4, 5]


def test_events_for_an_unknown_flow(service: MeterService) -> None:
    with pytest.raises(NotFoundError):
        service.get_events(get(path_params={"run_id": "nope"}))


def test_a_source_with_no_history_says_so_rather_than_returning_none() -> None:
    # An empty stream is indistinguishable from silence, so the API
    # declines the endpoint instead of pretending to serve it.
    source = FakeSource(make_flow(state=states.RUNNING))
    source.supports_events = False
    service = MeterService(Meter(source, poll=False))
    request = get(path_params={"run_id": "run-1"})

    with pytest.raises(UnsupportedError, match="keeps current state"):
        service.get_events(request)
    with pytest.raises(UnsupportedError):
        service.stream(request)


def test_a_flow_from_a_historyless_source_advertises_no_stream() -> None:
    source = FakeSource(make_flow(state=states.RUNNING))
    source.supports_events = False
    service = MeterService(Meter(source, poll=False))

    body = payload(service.list_flows(get()))
    links = body["flows"][0]["links"]
    assert "self" in links
    assert "events" not in links
    assert "stream" not in links


# -- streaming -----------------------------------------------------------


def test_a_stream_starts_at_the_beginning_by_default(
    service: MeterService,
) -> None:
    response = service.stream(get(path_params={"run_id": "run-1"}))
    assert isinstance(response, StreamResponse)
    assert response.cursor.since_seq == 0
    assert response.cursor.run_id == "run-1"


def test_a_stream_resumes_from_last_event_id(
    service: MeterService,
) -> None:
    # What a browser's EventSource sends by itself after a drop, which
    # is what makes a dropped connection recoverable.
    response = service.stream(
        get(
            path_params={"run_id": "run-1"},
            headers={"last-event-id": "4"},
        )
    )
    assert response.cursor.since_seq == 4


def test_an_explicit_resume_point_beats_the_header(
    service: MeterService,
) -> None:
    response = service.stream(
        get(
            query_string="since_seq=2",
            path_params={"run_id": "run-1"},
            headers={"last-event-id": "4"},
        )
    )
    assert response.cursor.since_seq == 2


def test_a_nonsense_last_event_id_is_a_bad_request(
    service: MeterService,
) -> None:
    with pytest.raises(BadRequestError, match="Last-Event-ID"):
        service.stream(
            get(
                path_params={"run_id": "run-1"},
                headers={"last-event-id": "banana"},
            )
        )


def test_streaming_an_unknown_flow(service: MeterService) -> None:
    with pytest.raises(NotFoundError):
        service.stream(get(path_params={"run_id": "nope"}))

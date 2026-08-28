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

"""Rendering snapshots and events as JSON-ready dictionaries."""

from __future__ import annotations

from taskflow_meter import states
from taskflow_meter.api import serializers
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from tests.conftest import make_atom
from tests.conftest import make_flow


def test_an_atom_reports_state_and_derived_completion() -> None:
    payload = serializers.atom(
        make_atom(
            "a",
            uuid="u-1",
            state=states.RUNNING,
            intention=states.EXECUTE,
            progress=0.25,
            progress_details={"at_progress": 0.25},
        )
    )
    assert payload["name"] == "a"
    assert payload["type"] == "task"
    assert payload["progress"] == 0.25
    assert payload["completion"] == 0.25
    assert payload["running"] is True
    assert payload["finished"] is False


def test_a_reverted_atom_is_finished_without_being_complete() -> None:
    # taskflow leaves its progress at 1.0, which is why the payload
    # carries both numbers rather than just the raw one.
    payload = serializers.atom(
        make_atom("a", state=states.REVERTED, progress=1.0)
    )
    assert payload["progress"] == 1.0
    assert payload["completion"] == 0.0
    assert payload["finished"] is True


def test_a_flow_summary_omits_its_atoms() -> None:
    snapshot = make_flow(
        state=states.RUNNING, atoms=(make_atom("a"), make_atom("b"))
    )
    payload = serializers.flow(snapshot, MeterRequest())
    assert payload["atom_count"] == 2
    assert "atoms" not in payload


def test_a_flow_detail_lists_atoms_in_name_order() -> None:
    snapshot = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("zeta"), make_atom("alpha")),
    )
    payload = serializers.flow(snapshot, MeterRequest(), with_atoms=True)
    assert [atom["name"] for atom in payload["atoms"]] == ["alpha", "zeta"]


def test_a_flow_without_event_support_advertises_no_stream() -> None:
    payload = serializers.flow(make_flow(), MeterRequest(), with_events=False)
    assert set(payload["links"]) == {"self", "atoms"}


def test_a_page_links_to_the_next_one_only_when_there_is_one() -> None:
    snapshot = make_flow()
    without = serializers.flow_page(
        FlowPage(items=(snapshot,)), MeterRequest()
    )
    assert "next" not in without["links"]

    with_more = serializers.flow_page(
        FlowPage(items=(snapshot,), next_marker="run-9"), MeterRequest()
    )
    assert with_more["links"]["next"].endswith("marker=run-9")


def test_an_empty_page_is_still_a_page() -> None:
    payload = serializers.flow_page(FlowPage(), MeterRequest())
    assert payload["flows"] == []
    assert payload["next_marker"] is None


def test_an_event_page_carries_its_resume_point() -> None:
    event = Event(run_id="run-1", seq=4, ts=1.0, kind=EventKind.HEARTBEAT)
    payload = serializers.event_page(
        EventPage(events=(event,), next_seq=4, oldest_seq=2, truncated=True),
        "run-1",
        MeterRequest(prefix="/m"),
    )
    assert payload["events"][0]["seq"] == 4
    assert payload["next_seq"] == 4
    assert payload["oldest_seq"] == 2
    assert payload["truncated"] is True
    assert payload["links"]["next"] == (
        "/m/api/v1/flows/run-1/events?since_seq=4"
    )

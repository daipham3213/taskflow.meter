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

"""Watching an engine's notifiers."""

from __future__ import annotations

import contextlib
import logging
from typing import Any

import pytest
from taskflow import engines
from taskflow import retry
from taskflow.patterns import linear_flow

from taskflow_meter import states
from taskflow_meter.collect.listener import MeterListener
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from tests.conftest import ExplodingTask
from tests.conftest import ProgressingTask


def collect(flow: Any, **kwargs: Any) -> tuple[list[Event], Any]:
    """Run a flow with a listener attached, and return what it saw."""
    seen: list[Event] = []
    engine = engines.load(flow)
    listener = MeterListener(engine, seen.append, **kwargs)
    with listener, contextlib.suppress(Exception):
        # Some of these flows fail on purpose; the listener is what is
        # under test, not the outcome.
        engine.run()
    return seen, listener


def test_a_flow_reports_its_own_states() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    seen, _listener = collect(flow)

    flow_states = [
        event.state for event in seen if event.kind is EventKind.FLOW_STATE
    ]
    assert flow_states[0] == states.RUNNING
    assert flow_states[-1] == states.SUCCESS


def test_atoms_report_their_transitions() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    seen, _listener = collect(flow)

    atom = [event for event in seen if event.kind is EventKind.ATOM_STATE]
    assert [event.state for event in atom] == [
        states.RUNNING,
        states.SUCCESS,
    ]
    assert all(event.atom_name == "only" for event in atom)
    assert all(event.atom_type == "task" for event in atom)
    assert atom[-1].old_state == states.RUNNING


def test_every_event_carries_the_run_id() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    seen, listener = collect(flow)
    assert {event.run_id for event in seen} == {listener.run_id}


def test_sequence_numbers_are_gap_free() -> None:
    flow = linear_flow.Flow("demo").add(
        ProgressingTask("first"), ProgressingTask("second")
    )
    seen, _listener = collect(flow)
    assert [event.seq for event in seen] == list(range(1, len(seen) + 1))


def test_the_timestamp_is_when_it_happened() -> None:
    # Unlike the poller's, which is only as good as its interval.
    ticks = iter(range(1000))
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    seen, _listener = collect(flow, clock=lambda: float(next(ticks)))

    assert [event.ts for event in seen] == sorted(event.ts for event in seen)


def test_a_result_is_flagged_not_carried() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    seen, _listener = collect(flow)

    success = next(
        event
        for event in seen
        if event.kind is EventKind.ATOM_STATE and event.state == states.SUCCESS
    )
    assert success.details == {"has_result": True}


def test_a_failure_is_reported() -> None:
    flow = linear_flow.Flow("demo").add(ExplodingTask("boom"))
    seen, _listener = collect(flow)

    atom_states = [
        event.state for event in seen if event.kind is EventKind.ATOM_STATE
    ]
    assert states.FAILURE in atom_states
    assert states.REVERTED in atom_states


def test_the_book_id_is_carried_when_given() -> None:
    # Nothing on the engine knows it: storage holds a flow detail, and
    # a flow detail has no reference back to its logbook.
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    seen, _listener = collect(flow, book_id="book-1")
    assert {event.book_id for event in seen} == {"book-1"}


def test_a_broken_emit_never_reaches_the_flow(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The rule the whole emit side rests on."""

    def explode(_event: Event) -> None:
        msg = "monitoring is broken"
        raise RuntimeError(msg)

    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    logger = "taskflow_meter.collect.listener"

    with (
        caplog.at_level(logging.ERROR, logger=logger),
        MeterListener(engine, explode),
    ):
        engine.run()

    # The flow finished normally regardless.
    assert engine.storage.get_flow_state() == states.SUCCESS
    assert "could not emit" in caplog.text


def test_the_listener_deregisters_on_the_way_out() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    seen: list[Event] = []

    with MeterListener(engine, seen.append):
        engine.run()
    during = len(seen)

    engine.reset()
    engine.run()
    assert len(seen) == during, "still listening after the block"


class FlakyTask(ProgressingTask):
    """Fails once, so the retry controller actually does something."""

    attempts = 0

    def execute(self) -> str:
        type(self).attempts += 1
        if type(self).attempts == 1:
            msg = "not this time"
            raise RuntimeError(msg)
        return "done"


def test_a_retry_controller_reports_its_own_transitions() -> None:
    FlakyTask.attempts = 0
    flow = linear_flow.Flow("demo", retry=retry.Times(3, "retrier")).add(
        FlakyTask("work")
    )
    seen, _listener = collect(flow)

    retries = [event for event in seen if event.atom_name == "retrier"]
    assert retries, "the retry controller was never reported"
    assert {event.atom_type for event in retries} == {"retry"}

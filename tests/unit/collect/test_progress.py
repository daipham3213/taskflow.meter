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

"""Capturing the progress a task reports about itself."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from taskflow import engines
from taskflow import retry
from taskflow import task
from taskflow.patterns import linear_flow

from taskflow_meter.collect.listener import MeterListener
from taskflow_meter.collect.progress import ProgressTap
from taskflow_meter.collect.progress import atoms_of
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from tests.conftest import ProgressingTask


class Detailed(task.Task):
    """Reports progress with details attached."""

    def execute(self) -> str:
        self.update_progress(0.5)
        return "done"


def run_with_tap(flow: Any, **kwargs: Any) -> list[Event]:
    seen: list[Event] = []
    engine = engines.load(flow)
    with ProgressTap(engine, seen.append, **kwargs):
        engine.run()
    return seen


def test_the_progress_a_task_reports_is_captured() -> None:
    """The gap a listener cannot close.

    taskflow writes update_progress() to storage and never re-emits it
    on the engine's atom notifier, so without this tap these numbers
    are invisible in-process.
    """
    flow = linear_flow.Flow("demo").add(
        ProgressingTask("only", steps=(0.25, 0.5, 0.75))
    )
    seen = run_with_tap(flow)

    reported = [event.progress for event in seen if event.atom_name == "only"]
    # The engine itself contributes 0.0 on start and 1.0 on success.
    assert reported == [0.0, 0.25, 0.5, 0.75, 1.0]
    assert all(event.kind is EventKind.ATOM_PROGRESS for event in seen)


def test_every_task_in_the_flow_is_bound() -> None:
    flow = linear_flow.Flow("demo").add(
        ProgressingTask("first"), ProgressingTask("second")
    )
    engine = engines.load(flow)
    with ProgressTap(engine, lambda _event: None) as tap:
        assert tap.bound == 2


def test_a_retry_controller_is_skipped() -> None:
    # It has no progress to report and no notifier to bind to.
    flow = linear_flow.Flow("demo", retry=retry.Times(2)).add(
        ProgressingTask("work")
    )
    engine = engines.load(flow)
    with ProgressTap(engine, lambda _event: None) as tap:
        assert tap.bound == 1


def test_atoms_are_found_by_compiling_the_flow() -> None:
    # Compiling here is what lets the tap bind before the first task
    # starts, and it is idempotent.
    flow = linear_flow.Flow("demo").add(
        ProgressingTask("a"), ProgressingTask("b")
    )
    engine = engines.load(flow)
    names = {atom.name for atom in atoms_of(engine)}
    assert names == {"a", "b"}
    assert {atom.name for atom in atoms_of(engine)} == names


def test_registering_twice_binds_once() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    tap = ProgressTap(engine, lambda _event: None)
    tap.register()
    tap.register()
    try:
        assert tap.bound == 1
    finally:
        tap.deregister()


def test_deregistering_leaves_the_task_alone() -> None:
    """A callback left on somebody's task outlives the monitoring."""
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    seen: list[Event] = []

    tap = ProgressTap(engine, seen.append)
    tap.register()
    tap.deregister()
    assert tap.bound == 0

    engine.run()
    assert seen == []


def test_deregistering_twice_is_harmless() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    tap = ProgressTap(engine, lambda _event: None)
    tap.register()
    tap.deregister()
    tap.deregister()


def test_progress_details_are_carried() -> None:
    flow = linear_flow.Flow("demo").add(Detailed("only"))
    seen = run_with_tap(flow)
    assert all("progress" not in event.details for event in seen), (
        "the number belongs in its own field"
    )


def test_the_atom_uuid_is_resolved() -> None:
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    seen = run_with_tap(flow)
    assert all(event.atom_uuid for event in seen)


def test_a_broken_emit_never_reaches_the_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def explode(_event: Event) -> None:
        msg = "monitoring is broken"
        raise RuntimeError(msg)

    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    logger = "taskflow_meter.collect.progress"

    with (
        caplog.at_level(logging.ERROR, logger=logger),
        ProgressTap(engine, explode),
    ):
        engine.run()

    from taskflow_meter import states

    assert engine.storage.get_flow_state() == states.SUCCESS
    assert "could not emit progress" in caplog.text


def test_the_tap_and_the_listener_share_a_sequence() -> None:
    # Two producers, one numbering: a client resuming from since_seq
    # must not see the same number twice.
    flow = linear_flow.Flow("demo").add(ProgressingTask("only", steps=(0.5,)))
    engine = engines.load(flow)
    seen: list[Event] = []
    listener = MeterListener(engine, seen.append)

    with (
        listener,
        ProgressTap(engine, seen.append, allocator=listener.allocator),
    ):
        engine.run()

    seqs = sorted(event.seq for event in seen)
    assert seqs == list(range(1, len(seen) + 1))


class Silent(task.Task):
    """A task whose notifier accepts no events at all."""

    TASK_EVENTS = ()

    def execute(self) -> str:
        return "done"


def test_a_task_that_accepts_no_listeners_is_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    flow = linear_flow.Flow("demo").add(Silent("quiet"))
    engine = engines.load(flow)
    logger = "taskflow_meter.collect.progress"

    with caplog.at_level(logging.DEBUG, logger=logger):
        tap = ProgressTap(engine, lambda _event: None)
        tap.register()

    assert tap.bound == 0
    assert "does not accept progress listeners" in caplog.text


def test_a_task_that_will_not_let_go_is_reported(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Leaving the callback is the lesser evil; failing the teardown
    # would take the rest of it down too.
    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    tap = ProgressTap(engine, lambda _event: None)
    tap.register()

    atom, _callback = tap._bound[0]

    def refuse(*_args: Any, **_kwargs: Any) -> None:
        msg = "not letting go"
        raise RuntimeError(msg)

    atom.notifier.deregister = refuse
    logger = "taskflow_meter.collect.progress"
    with caplog.at_level(logging.WARNING, logger=logger):
        tap.deregister()

    assert tap.bound == 0
    assert "could not deregister" in caplog.text


def test_an_atom_with_no_detail_yet_has_no_uuid() -> None:
    # Before the flow is prepared there is nothing to look up.
    from taskflow_meter.collect.progress import _atom_uuid

    flow = linear_flow.Flow("demo").add(ProgressingTask("only"))
    engine = engines.load(flow)
    assert _atom_uuid(engine, "only") is None

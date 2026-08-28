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

"""The barrier between a running flow and everything downstream."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Sequence

import pytest

from taskflow_meter.collect.pipeline import EventPipeline
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.transports.base import Publisher
from taskflow_meter.transports.memory import MemoryTransport


class Hostile(Publisher):
    """Raises on every delivery."""

    name = "hostile"

    def __init__(self) -> None:
        self.attempts = 0

    def publish(self, events: Sequence[Event]) -> None:
        self.attempts += 1
        msg = "no"
        raise RuntimeError(msg)


class Slow(Publisher):
    """Blocks until released, so the queue can be made to back up."""

    name = "slow"

    def __init__(self) -> None:
        self.release = threading.Event()
        self.entered = threading.Event()
        self.received: list[Event] = []

    def publish(self, events: Sequence[Event]) -> None:
        self.entered.set()
        self.release.wait(30)
        self.received.extend(events)


def event(seq: int) -> Event:
    return Event(
        run_id="run-1", seq=seq, ts=float(seq), kind=EventKind.HEARTBEAT
    )


def wait_for(predicate: object, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return True
        time.sleep(0.005)
    return False


@pytest.mark.parametrize(
    ("max_queue", "batch_size", "match"),
    [(0, 10, "max_queue"), (10, 0, "batch_size")],
)
def test_useless_settings_are_rejected(
    max_queue: int, batch_size: int, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        EventPipeline([], max_queue=max_queue, batch_size=batch_size)


def test_events_reach_the_publisher() -> None:
    transport = MemoryTransport()
    pipeline = EventPipeline([transport])
    pipeline.start()
    try:
        for seq in range(1, 4):
            assert pipeline.submit(event(seq))
        assert pipeline.flush()
    finally:
        pipeline.stop()

    assert [item.seq for item in transport.drain()] == [1, 2, 3]
    assert pipeline.stats.delivered == 3


def test_delivery_happens_off_the_submitting_thread() -> None:
    # The whole point: nothing downstream runs on the thread executing
    # somebody's task.
    seen: list[int] = []

    class Recording(Publisher):
        name = "recording"

        def publish(self, events: Sequence[Event]) -> None:
            seen.append(threading.get_ident())

    pipeline = EventPipeline([Recording()])
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert pipeline.flush()
    finally:
        pipeline.stop()

    assert seen, "the publisher was never called"
    assert seen[0] != threading.get_ident()


def test_submitting_never_blocks_on_a_stuck_publisher() -> None:
    slow = Slow()
    pipeline = EventPipeline([slow], max_queue=2)
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)

        # The sender is wedged; submitting must still return promptly.
        started = time.monotonic()
        for seq in range(2, 12):
            pipeline.submit(event(seq))
        assert time.monotonic() - started < 1.0
        assert pipeline.stats.dropped > 0
    finally:
        slow.release.set()
        pipeline.stop()


def test_a_dropped_event_is_not_reported_as_kept() -> None:
    # submit() answers about the event handed to it; stats.dropped is
    # what says something older was lost to make room for it.
    slow = Slow()
    pipeline = EventPipeline([slow], max_queue=1)
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)
        assert pipeline.submit(event(2)) is True
        assert pipeline.submit(event(3)) is True
        assert pipeline.stats.dropped >= 1
    finally:
        slow.release.set()
        pipeline.stop()


def test_a_full_queue_drops_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    slow = Slow()
    pipeline = EventPipeline([slow], max_queue=1)
    logger = "taskflow_meter.collect.pipeline"
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)
        with caplog.at_level(logging.WARNING, logger=logger):
            for seq in range(2, 6):
                pipeline.submit(event(seq))
    finally:
        slow.release.set()
        pipeline.stop()

    assert "queue is full" in caplog.text
    assert pipeline.stats.dropped >= 3


def test_the_drop_handler_is_told() -> None:
    dropped: list[Event] = []
    slow = Slow()
    pipeline = EventPipeline([slow], max_queue=1, on_drop=dropped.append)
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)
        pipeline.submit(event(2))
        pipeline.submit(event(3))
    finally:
        slow.release.set()
        pipeline.stop()

    assert dropped


def test_a_failing_drop_handler_does_not_reach_the_flow(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[Event] = []

    def unhelpful(dropped: Event) -> None:
        calls.append(dropped)
        msg = "handler exploded"
        raise RuntimeError(msg)

    slow = Slow()
    pipeline = EventPipeline([slow], max_queue=1, on_drop=unhelpful)
    logger = "taskflow_meter.collect.pipeline"
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)
        pipeline.submit(event(2))
        with caplog.at_level(logging.ERROR, logger=logger):
            # Must not raise, however badly the handler behaves -- and
            # the event itself is still queued, in place of the oldest.
            assert pipeline.submit(event(3)) is True
    finally:
        slow.release.set()
        pipeline.stop()

    assert calls, "the drop handler was never reached"
    assert "drop handler failed" in caplog.text


def test_the_sender_stops_even_when_the_sentinel_arrives_mid_batch() -> None:
    """A producer racing shutdown puts events behind the sentinel.

    Taking it as part of a batch and not noticing would leave the
    sender running for the life of the process.
    """
    slow = Slow()
    pipeline = EventPipeline([slow], max_queue=50, batch_size=50)
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)
        for seq in range(2, 10):
            pipeline.submit(event(seq))

        stopper = threading.Thread(
            target=pipeline.stop, kwargs={"drain": False, "timeout": 10}
        )
        stopper.start()
        slow.release.set()
        stopper.join(15)
        assert not stopper.is_alive()
    finally:
        slow.release.set()

    assert not pipeline.running


def test_a_failing_publisher_is_counted_not_fatal() -> None:
    hostile = Hostile()
    good = MemoryTransport()
    pipeline = EventPipeline([hostile, good])
    pipeline.start()
    try:
        for seq in range(1, 4):
            pipeline.submit(event(seq))
        assert pipeline.flush()
    finally:
        pipeline.stop()

    assert pipeline.stats.errors >= 1
    assert "no" in (pipeline.stats.last_error or "")
    # One bad publisher must not deprive the others.
    assert len(good.drain()) == 3


def test_events_are_delivered_in_batches() -> None:
    batches: list[int] = []

    class Counting(Publisher):
        name = "counting"

        def publish(self, events: Sequence[Event]) -> None:
            batches.append(len(events))

    pipeline = EventPipeline([Counting()], batch_size=50)
    # Queue before starting, so the sender finds a full queue at once.
    for seq in range(1, 21):
        pipeline.submit(event(seq))
    pipeline.start()
    try:
        assert pipeline.flush()
    finally:
        pipeline.stop()

    assert sum(batches) == 20
    assert max(batches) > 1, "nothing was batched"


def test_a_batch_never_exceeds_its_size() -> None:
    batches: list[int] = []

    class Counting(Publisher):
        name = "counting"

        def publish(self, events: Sequence[Event]) -> None:
            batches.append(len(events))

    pipeline = EventPipeline([Counting()], batch_size=3, max_queue=50)
    for seq in range(1, 11):
        pipeline.submit(event(seq))
    pipeline.start()
    try:
        assert pipeline.flush()
    finally:
        pipeline.stop()

    assert sum(batches) == 10
    assert max(batches) <= 3


def test_stopping_drains_what_is_queued() -> None:
    transport = MemoryTransport()
    pipeline = EventPipeline([transport])
    for seq in range(1, 6):
        pipeline.submit(event(seq))
    pipeline.start()
    pipeline.stop()

    assert len(transport.drain()) == 5


def test_publishers_are_started_and_stopped() -> None:
    calls: list[str] = []

    class Noting(MemoryTransport):
        def start(self) -> None:
            calls.append("start")

        def stop(self) -> None:
            calls.append("stop")

    pipeline = EventPipeline([Noting()])
    pipeline.start()
    pipeline.stop()
    assert calls == ["start", "stop"]


def test_a_publisher_that_will_not_stop_is_not_fatal() -> None:
    class Awkward(MemoryTransport):
        def stop(self) -> None:
            msg = "will not close"
            raise RuntimeError(msg)

    pipeline = EventPipeline([Awkward()])
    pipeline.start()
    pipeline.stop()
    assert not pipeline.running


def test_starting_twice_does_not_start_two_senders() -> None:
    pipeline = EventPipeline([MemoryTransport()])
    before = threading.active_count()
    pipeline.start()
    pipeline.start()
    try:
        assert threading.active_count() == before + 1
    finally:
        pipeline.stop()


def test_stopping_an_unstarted_pipeline_is_harmless() -> None:
    pipeline = EventPipeline([MemoryTransport()])
    pipeline.stop()
    pipeline.stop()
    assert not pipeline.running


def test_a_stubborn_sender_is_reported(
    caplog: pytest.LogCaptureFixture,
) -> None:
    slow = Slow()
    pipeline = EventPipeline([slow])
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)
        logger = "taskflow_meter.collect.pipeline"
        with caplog.at_level(logging.WARNING, logger=logger):
            pipeline.stop(drain=False, timeout=0.05)
        assert "did not stop" in caplog.text
    finally:
        slow.release.set()


def test_flush_reports_when_it_gave_up() -> None:
    slow = Slow()
    pipeline = EventPipeline([slow])
    pipeline.start()
    try:
        pipeline.submit(event(1))
        assert slow.entered.wait(5)
        pipeline.submit(event(2))
        assert pipeline.flush(0.05) is False
    finally:
        slow.release.set()
        pipeline.stop()

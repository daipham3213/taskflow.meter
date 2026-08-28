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

"""In-process transports."""

from __future__ import annotations

import pytest

from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.transports.memory import DataSourcePublisher
from taskflow_meter.transports.memory import MemoryTransport


def make_events(count: int) -> list[Event]:
    return [
        Event(
            run_id="run-1",
            seq=seq,
            ts=float(seq),
            kind=EventKind.HEARTBEAT,
        )
        for seq in range(1, count + 1)
    ]


def test_published_events_can_be_drained() -> None:
    transport = MemoryTransport()
    transport.publish(make_events(3))

    assert [event.seq for event in transport.drain()] == [1, 2, 3]
    # Draining takes them; a second drain is empty.
    assert transport.drain() == ()


def test_peeking_leaves_them_alone() -> None:
    transport = MemoryTransport()
    transport.publish(make_events(2))
    assert len(transport.peek()) == 2
    assert len(transport) == 2


def test_an_undrained_buffer_is_bounded_and_counted() -> None:
    # An unbounded buffer is a leak with a slower fuse than a crash.
    transport = MemoryTransport(max_events=3)
    transport.publish(make_events(5))

    assert [event.seq for event in transport.peek()] == [3, 4, 5]
    assert transport.dropped == 2


def test_a_useless_bound_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        MemoryTransport(max_events=0)


def test_the_datasource_publisher_folds_events_into_a_store() -> None:
    store = MemoryDataSource()
    DataSourcePublisher(store).publish(make_events(2))

    page = store.events_since("run-1")
    assert [event.seq for event in page.events] == [1, 2]


def test_a_publisher_is_a_context_manager() -> None:
    with MemoryTransport() as transport:
        transport.publish(make_events(1))
    assert len(transport.peek()) == 1

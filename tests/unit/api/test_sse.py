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

"""SSE framing and the cursor that drives a stream."""

from __future__ import annotations

import json

import pytest

from taskflow_meter import states
from taskflow_meter.api.sse import DEFAULT_RETRY_MS
from taskflow_meter.api.sse import SSE_HEADERS
from taskflow_meter.api.sse import EventCursor
from taskflow_meter.api.sse import StreamResponse
from taskflow_meter.api.sse import comment
from taskflow_meter.api.sse import frame
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.diff import diff_flow
from taskflow_meter.events import SequenceAllocator
from tests.conftest import make_atom
from tests.conftest import make_flow


def test_a_minimal_frame() -> None:
    assert frame("hello") == b"data: hello\n\n"


def test_a_frame_with_everything() -> None:
    assert frame("x", event="atom_state", event_id=7, retry=1000) == (
        b"event: atom_state\nid: 7\nretry: 1000\ndata: x\n\n"
    )


def test_multi_line_data_is_split_across_data_lines() -> None:
    # A raw newline would end the frame early and truncate the rest.
    assert frame("one\ntwo") == b"data: one\ndata: two\n\n"


def test_a_comment_is_a_frame_clients_ignore() -> None:
    assert comment("keep-alive") == b": keep-alive\n\n"
    assert comment() == b": \n\n"


def test_the_headers_defeat_proxy_buffering() -> None:
    headers = dict(SSE_HEADERS)
    assert headers["content-type"].startswith("text/event-stream")
    assert headers["cache-control"] == "no-cache"
    # nginx buffers proxied responses by default, which would turn a
    # live stream into one long silence.
    assert headers["x-accel-buffering"] == "no"


def test_no_hop_by_hop_headers_are_emitted() -> None:
    # PEP 3333 forbids an application from sending these, and wsgiref
    # refuses outright -- so one here breaks every WSGI deployment.
    hop_by_hop = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
    assert not {name for name, _ in SSE_HEADERS} & hop_by_hop


@pytest.fixture
def store() -> MemoryDataSource:
    return MemoryDataSource()


def fill(store: MemoryDataSource, *, finished: bool = False) -> None:
    allocator = SequenceAllocator()
    running = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
    )
    store.apply_many(diff_flow(None, running, allocator=allocator))
    if finished:
        done = make_flow(
            state=states.SUCCESS,
            atoms=(make_atom("a", state=states.SUCCESS, progress=1.0),),
        )
        store.apply_many(diff_flow(running, done, allocator=allocator))


def test_the_opening_bytes_include_a_retry_hint(
    store: MemoryDataSource,
) -> None:
    fill(store)
    opening = EventCursor(store, "run-1").opening()
    assert f"retry: {DEFAULT_RETRY_MS}".encode() in opening
    # Something on the wire immediately, so proxies release the headers.
    assert opening.endswith(b"\n\n")


def test_polling_frames_each_new_event(store: MemoryDataSource) -> None:
    fill(store)
    cursor = EventCursor(store, "run-1")
    frames = cursor.poll()

    assert len(frames) == 3
    assert b"event: flow_state" in frames[0]
    assert b"id: 1" in frames[0]
    payload = json.loads(frames[0].split(b"data: ", 1)[1])
    assert payload["kind"] == "flow_state"
    # The cursor advanced, so the next poll starts after these.
    assert cursor.since_seq == 3


def test_a_second_poll_with_nothing_new_is_empty(
    store: MemoryDataSource,
) -> None:
    fill(store)
    cursor = EventCursor(store, "run-1")
    cursor.poll()
    assert cursor.poll() == []
    assert not cursor.complete


def test_resuming_skips_what_the_client_already_has(
    store: MemoryDataSource,
) -> None:
    fill(store)
    cursor = EventCursor(store, "run-1", since_seq=2)
    frames = cursor.poll()
    assert len(frames) == 1
    assert b"id: 3" in frames[0]


def test_a_finished_flow_ends_the_stream(
    store: MemoryDataSource,
) -> None:
    fill(store, finished=True)
    cursor = EventCursor(store, "run-1")
    cursor.poll()

    frames = cursor.poll()
    assert cursor.complete
    assert frames[-1].startswith(b"event: end")


def test_an_evicted_history_is_reported_as_a_gap(
    store: MemoryDataSource,
) -> None:
    # The client's next event was dropped before it arrived.  Saying so
    # beats letting it stitch a hole into a history it believes is whole.
    small = MemoryDataSource(max_events_per_run=2)
    fill(small)
    cursor = EventCursor(small, "run-1")
    frames = cursor.poll()

    assert frames[0].startswith(b"event: gap")
    payload = json.loads(frames[0].split(b"data: ", 1)[1])
    assert payload == {"reason": "events_evicted", "resume_from": 2}


def test_the_heartbeat_is_a_comment(store: MemoryDataSource) -> None:
    assert EventCursor(store, "run-1").heartbeat().startswith(b": ")


def test_a_stream_response_carries_the_cursor_and_headers(
    store: MemoryDataSource,
) -> None:
    cursor = EventCursor(store, "run-1")
    response = StreamResponse(cursor=cursor)
    assert response.status == 200
    assert response.cursor is cursor
    assert response.headers == SSE_HEADERS

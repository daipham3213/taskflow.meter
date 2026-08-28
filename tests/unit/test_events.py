"""Event envelope and sequence allocation."""

from __future__ import annotations

import json
import threading

import pytest

from taskflow_meter.events import Event, EventKind, SequenceAllocator


def make_event(**overrides: object) -> Event:
    defaults: dict[str, object] = {
        "run_id": "run-1",
        "seq": 1,
        "ts": 100.0,
        "kind": EventKind.ATOM_STATE,
    }
    defaults.update(overrides)
    return Event(**defaults)  # type: ignore[arg-type]


def test_kind_is_its_own_string_value() -> None:
    assert EventKind.ATOM_PROGRESS.value == "atom_progress"
    assert str(EventKind.ATOM_PROGRESS) == "atom_progress"
    assert f"{EventKind.ATOM_PROGRESS}" == "atom_progress"


def test_to_dict_round_trips() -> None:
    event = make_event(
        atom_name="build",
        progress=0.5,
        details={"progress_details": {"at_progress": 0.5}},
    )
    assert Event.from_dict(event.to_dict()) == event


def test_to_dict_is_json_serialisable() -> None:
    payload = json.dumps(make_event(atom_name="build").to_dict())
    assert json.loads(payload)["kind"] == "atom_state"


def test_to_dict_copies_nested_details() -> None:
    event = make_event(details={"failure": {"exc_type_names": ["IOError"]}})
    data = event.to_dict()
    data["details"]["failure"]["exc_type_names"].append("mutated")
    assert event.details["failure"]["exc_type_names"] == ["IOError"]


def test_from_dict_rejects_unknown_keys() -> None:
    data = make_event().to_dict()
    data["speculative_field"] = 1
    with pytest.raises(TypeError):
        Event.from_dict(data)


def test_events_are_immutable() -> None:
    with pytest.raises(AttributeError):
        make_event().seq = 2  # type: ignore[misc]


def test_allocator_is_gap_free_and_per_run() -> None:
    allocator = SequenceAllocator()
    first = [allocator.allocate("run-1") for _ in range(3)]
    second = [allocator.allocate("run-2") for _ in range(2)]
    assert first == [1, 2, 3]
    assert second == [1, 2]


def test_allocator_peek_does_not_consume() -> None:
    allocator = SequenceAllocator()
    assert allocator.peek("run-1") == 0
    allocator.allocate("run-1")
    assert allocator.peek("run-1") == 1
    assert allocator.peek("run-1") == 1


def test_resume_from_never_renumbers_seen_events() -> None:
    allocator = SequenceAllocator()
    allocator.resume_from("run-1", 41)
    assert allocator.allocate("run-1") == 42
    # A lower resume point must not rewind and hand out duplicates.
    allocator.resume_from("run-1", 7)
    assert allocator.allocate("run-1") == 43


def test_forget_resets_a_run() -> None:
    allocator = SequenceAllocator()
    allocator.allocate("run-1")
    allocator.forget("run-1")
    assert allocator.allocate("run-1") == 1


def test_allocation_is_thread_safe() -> None:
    # The parallel engine fires callbacks from executor threads, so a
    # duplicate sequence number here would corrupt a client's since_seq.
    allocator = SequenceAllocator()
    seen: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        mine = [allocator.allocate("run-1") for _ in range(200)]
        with lock:
            seen.extend(mine)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(seen) == list(range(1, 1601))

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

"""The datasource with a schema of its own."""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa

from taskflow_meter import states
from taskflow_meter.datasource.base import UnknownMarkerError
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.datasource.sqlalchemy.source import SQLADataSource
from taskflow_meter.datasource.sqlalchemy.source import upgrade
from taskflow_meter.diff import diff_flow
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.events import SequenceAllocator
from tests.conftest import make_atom
from tests.conftest import make_flow


@pytest.fixture
def store(tmp_path: Path) -> SQLADataSource:
    return SQLADataSource(
        f"sqlite:///{tmp_path / 'meter.db'}", create_schema=True
    )


def heartbeat(seq: int, run_id: str = "run-1", ts: float = 0.0) -> Event:
    return Event(
        run_id=run_id,
        seq=seq,
        ts=ts or float(seq),
        kind=EventKind.HEARTBEAT,
    )


# -- construction --------------------------------------------------------


def test_needs_exactly_one_of_url_or_engine() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        SQLADataSource()
    with pytest.raises(ValueError, match="exactly one"):
        SQLADataSource("sqlite://", engine=object())


def test_an_engine_can_be_shared(tmp_path: Path) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'shared.db'}")
    from taskflow_meter.datasource.sqlalchemy.models import metadata

    metadata.create_all(engine)
    store = SQLADataSource(engine=engine)
    store.apply(heartbeat(1))
    # Borrowed, so stopping us must not dispose somebody else's engine.
    store.stop()
    assert engine.connect() is not None


# -- writing and reading -------------------------------------------------


def test_an_unknown_run_reads_as_absent(store: SQLADataSource) -> None:
    assert store.get_flow("nope") is None
    assert store.get_atoms("nope") is None
    assert store.events_since("nope").events == ()


def test_a_flow_round_trips(store: SQLADataSource) -> None:
    allocator = SequenceAllocator()
    snapshot = make_flow(
        state=states.RUNNING,
        book_id="book-1",
        book_name="nightly",
        atoms=(
            make_atom(
                "a",
                state=states.RUNNING,
                progress=0.5,
                uuid="u-1",
                intention=states.EXECUTE,
                progress_details={"at_progress": 0.5},
            ),
        ),
    )
    store.apply_many(diff_flow(None, snapshot, allocator=allocator))

    stored = store.get_flow("run-1")
    assert stored is not None
    assert stored.state == states.RUNNING
    assert stored.book_name == "nightly"

    atom = stored.atoms["a"]
    assert atom.uuid == "u-1"
    assert atom.progress == pytest.approx(0.5)
    assert atom.progress_details == {"at_progress": 0.5}


def test_it_agrees_with_the_in_memory_store() -> None:
    """Both fold with the same code, and this is what proves it."""
    allocator = SequenceAllocator()
    memory = MemoryDataSource()
    sql = SQLADataSource("sqlite://", create_schema=True)

    previous = None
    for state, progress in (
        (states.PENDING, 0.0),
        (states.RUNNING, 0.5),
        (states.SUCCESS, 1.0),
    ):
        snapshot = make_flow(
            state=state,
            atoms=(make_atom("a", state=state, progress=progress),),
        )
        events = diff_flow(previous, snapshot, allocator=allocator)
        memory.apply_many(events)
        sql.apply_many(events)
        previous = snapshot

    from_memory = memory.get_flow("run-1")
    from_sql = sql.get_flow("run-1")
    assert from_memory is not None
    assert from_sql is not None
    assert from_sql == from_memory
    assert [e.seq for e in sql.events_since("run-1").events] == [
        e.seq for e in memory.events_since("run-1").events
    ]


def test_replaying_an_event_is_a_no_op(store: SQLADataSource) -> None:
    # A collector that reconnects redelivers; that must not double up.
    events = [heartbeat(seq) for seq in (1, 2, 3)]
    store.apply_many(events)
    store.apply_many(events)

    assert [e.seq for e in store.events_since("run-1").events] == [
        1,
        2,
        3,
    ]


def test_a_batch_touching_several_runs(store: SQLADataSource) -> None:
    store.apply_many(
        [heartbeat(1, "run-1"), heartbeat(1, "run-2"), heartbeat(2, "run-1")]
    )
    assert store.get_flow("run-1") is not None
    assert store.get_flow("run-2") is not None
    assert len(store.events_since("run-1").events) == 2


def test_an_empty_batch_does_nothing(store: SQLADataSource) -> None:
    store.apply_many([])
    assert store.list_flows().items == ()


# -- listing -------------------------------------------------------------


def populate(store: SQLADataSource) -> None:
    for index, (run_id, state) in enumerate(
        [
            ("run-a", states.SUCCESS),
            ("run-b", states.RUNNING),
            ("run-c", states.RUNNING),
        ]
    ):
        store.apply(
            Event(
                run_id=run_id,
                seq=1,
                ts=float(index),
                kind=EventKind.FLOW_STATE,
                book_id="book-1" if run_id != "run-c" else "book-2",
                state=state,
            )
        )


def test_listing_is_newest_first(store: SQLADataSource) -> None:
    populate(store)
    assert [flow.run_id for flow in store.list_flows().items] == [
        "run-c",
        "run-b",
        "run-a",
    ]


@pytest.mark.parametrize(
    ("state", "book_id", "expected"),
    [
        (states.RUNNING, None, ["run-c", "run-b"]),
        (None, "book-1", ["run-b", "run-a"]),
        (states.RUNNING, "book-1", ["run-b"]),
        (states.PENDING, None, []),
    ],
)
def test_filtering_happens_in_sql(
    store: SQLADataSource,
    state: str | None,
    book_id: str | None,
    expected: list[str],
) -> None:
    # The point of this datasource over the persistence one, which can
    # only filter after reading everything.
    populate(store)
    page = store.list_flows(state=state, book_id=book_id)
    assert [flow.run_id for flow in page.items] == expected


def test_paging_walks_every_flow_once(store: SQLADataSource) -> None:
    populate(store)
    seen: list[str] = []
    marker: str | None = None
    while True:
        page = store.list_flows(limit=2, marker=marker)
        seen.extend(flow.run_id for flow in page.items)
        if not page.has_more:
            break
        marker = page.next_marker
    assert seen == ["run-c", "run-b", "run-a"]


def test_flows_observed_together_page_deterministically(
    store: SQLADataSource,
) -> None:
    for run_id in ("run-b", "run-a", "run-c"):
        store.apply(
            Event(
                run_id=run_id,
                seq=1,
                ts=5.0,
                kind=EventKind.FLOW_STATE,
                state=states.RUNNING,
            )
        )
    first = store.list_flows(limit=2)
    second = store.list_flows(limit=2, marker=first.next_marker)
    assert [flow.run_id for flow in first.items] == ["run-a", "run-b"]
    assert [flow.run_id for flow in second.items] == ["run-c"]


def test_an_unknown_marker_is_rejected(store: SQLADataSource) -> None:
    populate(store)
    with pytest.raises(UnknownMarkerError):
        store.list_flows(marker="run-gone")


@pytest.mark.parametrize("limit", [0, -1])
def test_non_positive_limits_are_rejected(
    store: SQLADataSource, limit: int
) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        store.list_flows(limit=limit)
    with pytest.raises(ValueError, match="at least 1"):
        store.events_since("run-1", limit=limit)


# -- events --------------------------------------------------------------


def test_events_resume_from_a_sequence(store: SQLADataSource) -> None:
    store.apply_many([heartbeat(seq) for seq in range(1, 6)])
    page = store.events_since("run-1", since_seq=3)
    assert [event.seq for event in page.events] == [4, 5]
    assert page.next_seq == 5
    assert page.truncated is False


def test_events_beyond_a_gap_are_held_back(
    store: SQLADataSource,
) -> None:
    # Concurrent producers do not arrive in order; advancing a caller
    # past an event still in flight would lose it for good.
    store.apply_many([heartbeat(1), heartbeat(3)])
    page = store.events_since("run-1")
    assert [event.seq for event in page.events] == [1]

    store.apply(heartbeat(2))
    assert [
        event.seq for event in store.events_since("run-1", since_seq=1).events
    ] == [2, 3]


def test_a_pruned_history_is_reported_as_truncated(
    store: SQLADataSource,
) -> None:
    store.apply_many([heartbeat(seq) for seq in range(1, 6)])
    with store.engine.begin() as conn:
        from taskflow_meter.datasource.sqlalchemy.models import events

        conn.execute(sa.delete(events).where(events.c.seq < 3))

    page = store.events_since("run-1")
    assert page.oldest_seq == 3
    assert page.truncated is True
    assert [event.seq for event in page.events] == [3, 4, 5]


def test_an_event_round_trips_exactly(store: SQLADataSource) -> None:
    original = Event(
        run_id="run-1",
        seq=1,
        ts=1.5,
        kind=EventKind.ATOM_PROGRESS,
        book_id="book-1",
        atom_name="a",
        atom_uuid="u-1",
        atom_type="task",
        state=states.RUNNING,
        old_state=states.PENDING,
        intention=states.EXECUTE,
        progress=0.25,
        details={"progress_details": {"at_progress": 0.25}},
    )
    store.apply(original)
    (stored,) = store.events_since("run-1").events
    assert stored == original


# -- retention -----------------------------------------------------------


def test_pruning_drops_old_runs_and_their_events(
    store: SQLADataSource,
) -> None:
    store.apply(heartbeat(1, "old", ts=10.0))
    store.apply(heartbeat(1, "new", ts=100.0))

    assert store.prune(before=50.0) == 1
    assert store.get_flow("old") is None
    assert store.events_since("old").events == ()
    assert store.get_flow("new") is not None


def test_pruning_nothing_is_not_an_error(store: SQLADataSource) -> None:
    assert store.prune(before=0.0) == 0


def test_forgetting_a_run(store: SQLADataSource) -> None:
    store.apply(heartbeat(1))
    assert store.forget("run-1") is True
    assert store.forget("run-1") is False
    assert store.get_flow("run-1") is None


# -- migrations ----------------------------------------------------------


def test_upgrade_creates_a_usable_schema(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'migrated.db'}"
    upgrade(url)
    store = SQLADataSource(url)
    store.apply(heartbeat(1))
    assert store.get_flow("run-1") is not None


def test_upgrade_is_idempotent(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'twice.db'}"
    upgrade(url)
    upgrade(url)


def test_upgrade_can_be_told_where_to_keep_its_revision(
    tmp_path: Path,
) -> None:
    # Sharing a database with a host service that migrates its own schema
    # means sharing ``alembic_version`` unless this is passed, and each
    # tree then reads the other's revision as one it has never heard of.
    url = f"sqlite:///{tmp_path / 'shared.db'}"
    upgrade(url, version_table="taskflow_meter_version")

    store = SQLADataSource(url)
    with store.engine.begin() as conn:
        tables = set(sa.inspect(conn).get_table_names())

    assert "taskflow_meter_version" in tables
    assert "alembic_version" not in tables

    # And it stays idempotent against the table it was given.
    upgrade(url, version_table="taskflow_meter_version")


def test_the_source_can_be_stopped_and_reused(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'reuse.db'}"
    upgrade(url)
    store = SQLADataSource(url)
    store.apply(heartbeat(1))
    store.stop()
    # Reopens rather than failing.
    assert store.get_flow("run-1") is not None


def test_it_is_a_context_manager(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'ctx.db'}"
    upgrade(url)
    with SQLADataSource(url) as store:
        store.apply(heartbeat(1))
    assert isinstance(store, SQLADataSource)


def test_atoms_are_returned_in_name_order(store: SQLADataSource) -> None:
    allocator = SequenceAllocator()
    snapshot = make_flow(
        state=states.RUNNING,
        atoms=(make_atom("zeta"), make_atom("alpha")),
    )
    store.apply_many(diff_flow(None, snapshot, allocator=allocator))

    atoms = store.get_atoms("run-1")
    assert atoms is not None
    assert [atom.name for atom in atoms] == ["alpha", "zeta"]


def test_a_flow_with_no_events_yet(store: SQLADataSource) -> None:
    # apply() with an atom event creates the flow row even though no
    # identity event ever arrived.
    store.apply(
        Event(
            run_id="run-1",
            seq=7,
            ts=1.0,
            kind=EventKind.ATOM_STATE,
            atom_name="a",
            state=states.RUNNING,
        )
    )
    flow = store.get_flow("run-1")
    assert flow is not None
    assert flow.name == ""
    assert flow.atoms["a"].state == states.RUNNING


def test_metadata_describes_both_tables() -> None:
    from taskflow_meter.datasource.sqlalchemy.models import metadata

    assert set(metadata.tables) == {
        "taskflow_meter_flows",
        "taskflow_meter_events",
    }


def test_the_details_column_survives_a_dict(
    store: SQLADataSource, tmp_path: Path
) -> None:
    store.apply(
        Event(
            run_id="run-1",
            seq=1,
            ts=1.0,
            kind=EventKind.FLOW_STRUCTURE,
            details={"nodes": [{"name": "a", "kind": "task"}]},
        )
    )
    (stored,) = store.events_since("run-1").events
    assert stored.details["nodes"][0]["name"] == "a"


def test_an_unusable_url_fails_when_used(tmp_path: Path) -> None:
    store = SQLADataSource("sqlite:////nonexistent-dir/meter.db")
    with pytest.raises(Exception, match=r"unable to open|no such"):
        store.get_flow("run-1")


def test_the_listing_index_exists(store: SQLADataSource) -> None:
    # Listing is the hot query; without it this is a scan per page.
    inspector = sa.inspect(store.engine)
    names = {
        index["name"]
        for index in inspector.get_indexes("taskflow_meter_flows")
    }
    assert "ix_taskflow_meter_flows_listing" in names


def test_unknown_atoms_dict_defaults(store: SQLADataSource) -> None:
    from taskflow_meter.datasource.sqlalchemy.models import flows

    with store.engine.begin() as conn:
        conn.execute(
            sa.insert(flows).values(
                run_id="bare",
                name="",
                observed_at=1.0,
                meta={},
                atoms={},
            )
        )
    flow = store.get_flow("bare")
    assert flow is not None
    assert flow.atoms == {}


def test_the_plugin_name(store: SQLADataSource) -> None:
    assert SQLADataSource.name == "sqlalchemy"
    assert store.supports_events is True


def test_untouched_runs_are_not_pruned(store: SQLADataSource) -> None:
    store.apply(heartbeat(1, "keep", ts=100.0))
    assert store.prune(before=100.0) == 0
    assert store.get_flow("keep") is not None


def test_apply_single_event(store: SQLADataSource) -> None:
    store.apply(heartbeat(1))
    assert len(store.events_since("run-1").events) == 1


def test_meta_survives(store: SQLADataSource) -> None:
    allocator = SequenceAllocator()
    snapshot = make_flow(state=states.RUNNING)
    store.apply_many(diff_flow(None, snapshot, allocator=allocator))
    stored = store.get_flow("run-1")
    assert stored is not None
    assert stored.meta == {}


def test_engine_disposal_only_when_owned(tmp_path: Path) -> None:
    store = SQLADataSource(
        f"sqlite:///{tmp_path / 'own.db'}", create_schema=True
    )
    store.apply(heartbeat(1))
    store.stop()
    store.stop()
    assert store.get_flow("run-1") is not None


def test_events_limit_is_respected(store: SQLADataSource) -> None:
    store.apply_many([heartbeat(seq) for seq in range(1, 11)])
    page = store.events_since("run-1", limit=4)
    assert [event.seq for event in page.events] == [1, 2, 3, 4]
    assert page.next_seq == 4

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

"""Builders for the snapshot fixtures the unit tests compare against."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from oslo_utils import uuidutils
from taskflow import engines
from taskflow import task
from taskflow.persistence import backends as tf_backends
from taskflow.persistence import models as tf_models

from taskflow_meter.events import SequenceAllocator
from taskflow_meter.models import AtomSnapshot
from taskflow_meter.models import FlowSnapshot


def make_atom(name: str, **overrides: Any) -> AtomSnapshot:
    return AtomSnapshot(name=name, **overrides)


def make_flow(
    run_id: str = "run-1",
    *,
    atoms: tuple[AtomSnapshot, ...] = (),
    **overrides: Any,
) -> FlowSnapshot:
    overrides.setdefault("name", "demo-flow")
    overrides.setdefault("observed_at", 100.0)
    return FlowSnapshot(
        run_id=run_id,
        atoms={atom.name: atom for atom in atoms},
        **overrides,
    )


@pytest.fixture
def allocator() -> SequenceAllocator:
    return SequenceAllocator()


@pytest.fixture
def sqlite_url(tmp_path: Path) -> str:
    """A file-backed sqlite URL, so a second connection can read it."""
    return f"sqlite:///{tmp_path / 'taskflow.db'}"


@pytest.fixture
def backend(sqlite_url: str) -> Iterator[Any]:
    """A taskflow persistence backend with its schema in place."""
    created = tf_backends.fetch({"connection": sqlite_url})
    with contextlib.closing(created.get_connection()) as conn:
        conn.upgrade()
    try:
        yield created
    finally:
        created.close()


def make_logbook(
    backend: Any,
    *,
    book_name: str = "demo-book",
    flow_name: str = "demo-flow",
) -> tuple[Any, Any]:
    """Persist an empty book and flow detail, and return both."""
    book = tf_models.LogBook(book_name)
    flow_detail = tf_models.FlowDetail(
        flow_name, uuid=uuidutils.generate_uuid()
    )
    book.add(flow_detail)
    with contextlib.closing(backend.get_connection()) as conn:
        conn.save_logbook(book)
    return book, flow_detail


def run_flow(backend: Any, flow: Any, book: Any, flow_detail: Any) -> Any:
    """Run a flow to completion against a persistence backend."""
    engine = engines.load(
        flow, flow_detail=flow_detail, book=book, backend=backend
    )
    engine.run()
    return engine


class ProgressingTask(task.Task):
    """Reports a known progress sequence, so tests can assert on it."""

    def __init__(self, name: str, steps: tuple[float, ...] = (0.5,)) -> None:
        super().__init__(name=name)
        self.steps = steps

    def execute(self) -> str:
        for step in self.steps:
            self.update_progress(step)
        return f"{self.name}-done"


class ExplodingTask(task.Task):
    """Fails on purpose."""

    def execute(self) -> None:
        msg = "boom"
        raise RuntimeError(msg)

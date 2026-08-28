"""Builders for the snapshot fixtures the unit tests compare against."""

from __future__ import annotations

from typing import Any

import pytest

from taskflow_meter.events import SequenceAllocator
from taskflow_meter.models import AtomSnapshot, FlowSnapshot


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

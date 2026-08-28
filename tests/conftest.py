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

from typing import Any

import pytest

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

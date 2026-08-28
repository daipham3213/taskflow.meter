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

"""Attach to a running engine: the whole in-process path in one call.

Named ``attachment`` rather than ``attach`` because the package
re-exports the :func:`attach` function, and a module of the same name
would be shadowed by it -- leaving ``taskflow_meter.collect.attach``
meaning one thing to an importer and another to anything that resolves
attributes.

    with attach(engine) as watched:
        engine.run()
        meter = Meter(watched.store, poll=False)

What that gets you over reading persistence is latency and shape: state
changes arrive as the engine makes them rather than a poll interval
later, per-task progress arrives the moment a task reports it, and the
flow's graph -- which taskflow never persists -- is emitted once up
front so a UI can draw the thing instead of tailing a list.
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from taskflow.engines.action_engine import compiler as tf_compiler

from taskflow_meter.collect.listener import MeterListener
from taskflow_meter.collect.pipeline import EventPipeline
from taskflow_meter.collect.progress import ProgressTap
from taskflow_meter.datasource.base import WritableDataSource
from taskflow_meter.datasource.memory import MemoryDataSource
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind
from taskflow_meter.events import SequenceAllocator
from taskflow_meter.transports.base import Publisher
from taskflow_meter.transports.memory import DataSourcePublisher

LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class Attachment:
    """What is watching one engine, and where its events landed."""

    engine: Any
    store: WritableDataSource | None
    pipeline: EventPipeline
    listener: MeterListener
    tap: ProgressTap
    allocator: SequenceAllocator = field(default_factory=SequenceAllocator)

    @property
    def run_id(self) -> str:
        return self.listener.run_id

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait for queued events to be delivered."""
        return self.pipeline.flush(timeout)


@contextlib.contextmanager
def attach(
    engine: Any,
    *,
    store: WritableDataSource | None = None,
    publishers: Sequence[Publisher] = (),
    book_id: str | None = None,
    emit_structure: bool = True,
    max_queue: int = 1000,
    clock: Callable[[], float] = time.time,
) -> Iterator[Attachment]:
    """Watch ``engine`` for as long as the block runs.

    An in-memory store is created unless one is given, so the simple
    case needs no arguments.  Everything is torn down on the way out,
    including when the flow raises -- a callback left registered on
    somebody's task object outlives the monitoring that wanted it.
    """
    resolved_store = MemoryDataSource() if store is None else store
    sinks: list[Publisher] = [DataSourcePublisher(resolved_store)]
    sinks.extend(publishers)

    pipeline = EventPipeline(sinks, max_queue=max_queue)
    allocator = SequenceAllocator()
    listener = MeterListener(
        engine,
        pipeline.submit,
        allocator=allocator,
        clock=clock,
        book_id=book_id,
    )
    tap = ProgressTap(
        engine,
        pipeline.submit,
        allocator=allocator,
        clock=clock,
        book_id=book_id,
    )

    pipeline.start()
    try:
        with listener:
            tap.register()
            try:
                if emit_structure:
                    _emit_structure(
                        engine, pipeline.submit, allocator, clock, book_id
                    )
                yield Attachment(
                    engine=engine,
                    store=resolved_store,
                    pipeline=pipeline,
                    listener=listener,
                    tap=tap,
                    allocator=allocator,
                )
            finally:
                tap.deregister()
    finally:
        pipeline.stop()


def _emit_structure(
    engine: Any,
    emit: Callable[[Event], Any],
    allocator: SequenceAllocator,
    clock: Callable[[], float],
    book_id: str | None,
) -> None:
    """Emit the flow's graph, once, before anything runs."""
    try:
        graph = describe_graph(engine)
    except Exception:
        # Topology is a bonus; failing to read it must not stop the
        # states and progress that are the point.
        LOG.exception("could not describe the flow's graph")
        return

    run_id = str(engine.storage.flow_uuid)
    emit(
        Event(
            run_id=run_id,
            seq=allocator.allocate(run_id),
            ts=clock(),
            kind=EventKind.FLOW_STRUCTURE,
            book_id=book_id,
            details=graph,
        )
    )


def describe_graph(engine: Any) -> dict[str, Any]:
    """Render the compiled execution graph as plain data.

    This is the thing the persistence datasource can never provide:
    taskflow stores atoms but not the edges between them, so the shape
    of a flow only exists in the process that compiled it.
    """
    engine.compile()
    compilation = engine.compilation
    graph = compilation.execution_graph

    nodes = [
        {"name": _name(node), "kind": str(data.get("kind", ""))}
        for node, data in graph.nodes(data=True)
    ]
    edges = [
        {"from": _name(source), "to": _name(target)}
        for source, target in graph.edges()
    ]
    return {
        "nodes": sorted(nodes, key=lambda item: (item["kind"], item["name"])),
        "edges": sorted(edges, key=lambda item: (item["from"], item["to"])),
        "atom_count": sum(
            1
            for _, data in graph.nodes(data=True)
            if data.get("kind") in tf_compiler.ATOMS
        ),
    }


def _name(node: Any) -> str:
    """The name of a graph node.

    Not ``str(node)``: an atom's repr is its decorated form, so a task
    called "first" renders as '"first==1.0"' and matches nothing a
    client knows it by.
    """
    return str(getattr(node, "name", None) or node)

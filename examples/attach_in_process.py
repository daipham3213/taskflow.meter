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

"""Watch a flow from inside the process running it.

    python examples/attach_in_process.py

Attaches to an engine, runs a flow that reports its own progress, and
prints the event stream as it lands -- no database, nothing on the
wire, and nothing changed in the flow itself.
"""

from __future__ import annotations

import time

from taskflow import engines
from taskflow import task
from taskflow.patterns import linear_flow
from taskflow.patterns import unordered_flow

from taskflow_meter.collect import attach
from taskflow_meter.events import EventKind


class Upload(task.Task):
    """A task that reports how far along it is."""

    def execute(self) -> str:
        files = range(5)
        for index in files:
            time.sleep(0.05)
            self.update_progress((index + 1) / len(files))
        return f"uploaded {len(files)} files"


class Verify(task.Task):
    """A task with nothing to report but its state."""

    def execute(self) -> str:
        time.sleep(0.1)
        return "verified"


def build_flow() -> linear_flow.Flow:
    return linear_flow.Flow("publish").add(
        Upload("upload"),
        unordered_flow.Flow("checks").add(
            Verify("verify-checksums"), Verify("verify-signatures")
        ),
    )


def main() -> int:
    engine = engines.load(build_flow())

    with attach(engine) as watched:
        engine.run()
        watched.flush()

        store = watched.store
        assert store is not None
        page = store.events_since(watched.run_id, limit=1000)

        print(f"run {watched.run_id}\n")
        for event in page.events:
            if event.kind is EventKind.FLOW_STRUCTURE:
                nodes = event.details["atom_count"]
                edges = len(event.details["edges"])
                print(f"  graph: {nodes} atoms, {edges} edges")
            elif event.kind is EventKind.ATOM_PROGRESS:
                print(f"  {event.atom_name:20} {event.progress:>5.0%}")
            elif event.kind is EventKind.ATOM_STATE:
                print(f"  {event.atom_name:20} {event.state}")
            else:
                print(f"  flow                 {event.state}")

        flow = store.get_flow(watched.run_id)
        assert flow is not None
        print(f"\n  completion: {flow.completion:.0%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

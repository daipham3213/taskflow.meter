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

"""Observe a flow running in a different process.

The milestone this project is built around: a flow runs in one process
against a sqlite logbook, and a meter in another process watches its
state and its per-atom progress without the flow knowing anything about
it.  Nothing here imports the flow's code or touches its engine.

The child waits on a file before finishing, so the parent is guaranteed a
chance to observe the flow mid-run rather than racing it.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from taskflow_meter import states
from taskflow_meter.datasource.persistence import PersistenceDataSource
from taskflow_meter.events import EventKind
from taskflow_meter.meter import Meter

TIMEOUT = 60.0

CHILD = '''
import sys
import time

from oslo_utils import uuidutils
from taskflow import engines
from taskflow import task
from taskflow.patterns import linear_flow
from taskflow.persistence import backends
from taskflow.persistence import models

STARTED = {started!r}
GO = {go!r}
CONF = {{"connection": {url!r}}}


class Waiting(task.Task):
    """Reports progress, then blocks until the observer says go."""

    def execute(self):
        self.update_progress(0.25)
        with open(STARTED, "w") as handle:
            handle.write("running")
        deadline = time.monotonic() + {timeout}
        while time.monotonic() < deadline:
            try:
                open(GO).close()
                break
            except OSError:
                time.sleep(0.02)
        else:
            raise RuntimeError("the observer never said go")
        self.update_progress(0.75)
        return "finished"


backend = backends.fetch(CONF)
conn = backend.get_connection()
conn.upgrade()
conn.close()

book = models.LogBook("cross-process-book")
flow_detail = models.FlowDetail(
    "cross-process-flow", uuid=uuidutils.generate_uuid()
)
book.add(flow_detail)
conn = backend.get_connection()
conn.save_logbook(book)
conn.close()

flow = linear_flow.Flow("cross-process-flow").add(Waiting("worker"))
engine = engines.load(
    flow, flow_detail=flow_detail, book=book, backend=backend
)
engine.run()
backend.close()
sys.stdout.write(flow_detail.uuid)
'''


def wait_for_file(path: Path, child: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        if path.exists():
            return
        if child.poll() is not None:
            out, err = child.communicate()
            pytest.fail(f"the child exited early:\n{out}\n{err}")
        time.sleep(0.02)
    pytest.fail(f"{path.name} never appeared")


def poll_until(meter: Meter, predicate: object) -> None:
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        meter.poll_once()
        if predicate(meter):  # type: ignore[operator]
            return
        time.sleep(0.05)
    pytest.fail("the meter never observed what was expected")


def test_a_flow_in_another_process_is_observable_start_to_finish(
    tmp_path: Path,
) -> None:
    database = tmp_path / "taskflow.db"
    started = tmp_path / "started"
    go = tmp_path / "go"
    script = tmp_path / "child.py"
    script.write_text(
        textwrap.dedent(CHILD).format(
            started=str(started),
            go=str(go),
            url=f"sqlite:///{database}",
            timeout=TIMEOUT,
        )
    )

    child = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # Wait until the flow is genuinely mid-task before looking.
        wait_for_file(started, child)

        source = PersistenceDataSource(
            conf={"connection": f"sqlite:///{database}"}
        )
        meter = Meter(source, interval=0.05)

        with meter:
            poll_until(
                meter,
                lambda m: any(
                    flow.state == states.RUNNING
                    for flow in m.list_flows().items
                ),
            )

            (running,) = meter.list_flows().items
            run_id = running.run_id
            assert running.name == "cross-process-flow"
            assert running.book_name == "cross-process-book"

            atom = running.atoms["worker"]
            assert atom.state == states.RUNNING
            # The progress the task reported from inside another process.
            assert atom.progress == pytest.approx(0.25)
            assert not atom.has_result

            # Release the child and watch it through to completion.
            go.write_text("go")
            poll_until(
                meter,
                lambda m: (
                    (m.get_flow(run_id) or running).state == states.SUCCESS
                ),
            )

            finished = meter.get_flow(run_id)
            assert finished is not None
            assert finished.state == states.SUCCESS
            assert finished.completion == pytest.approx(1.0)
            assert finished.atoms["worker"].has_result

            page = meter.events_since(run_id, limit=1000)
            kinds = [event.kind for event in page.events]
            seqs = [event.seq for event in page.events]
    finally:
        # Let it finish on its own -- it exits once released.  Killing a
        # child that already succeeded would report a failure that only
        # the teardown caused.
        try:
            stdout, stderr = child.communicate(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            child.kill()
            stdout, stderr = child.communicate()

    assert child.returncode == 0, f"child failed:\n{stdout}\n{stderr}"
    assert stdout.strip() == run_id

    # The stream is a history, not just a final state: the flow was seen
    # running before it was seen finished.
    assert EventKind.ATOM_PROGRESS in kinds
    states_seen = [
        event.state
        for event in page.events
        if event.kind is EventKind.ATOM_STATE
    ]
    assert states_seen == [states.RUNNING, states.SUCCESS]
    # Gap-free and monotonic, so a client resuming from since_seq cannot
    # silently miss anything.
    assert seqs == list(range(1, len(seqs) + 1))


def test_an_empty_logbook_reports_nothing_rather_than_failing(
    tmp_path: Path,
) -> None:
    # A meter started before any flow has run must not fall over.
    database = tmp_path / "empty.db"
    script = tmp_path / "prepare.py"
    script.write_text(
        textwrap.dedent(
            f"""
            from taskflow.persistence import backends
            backend = backends.fetch({{"connection": "sqlite:///{database}"}})
            conn = backend.get_connection()
            conn.upgrade()
            conn.close()
            backend.close()
            """
        )
    )
    subprocess.run([sys.executable, str(script)], check=True, timeout=TIMEOUT)

    source = PersistenceDataSource(
        conf={"connection": f"sqlite:///{database}"}
    )
    with Meter(source, interval=0.05) as meter:
        assert meter.poll_once() == 0
        assert meter.list_flows().items == ()

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

"""Serve the monitoring API for a flow running in another process.

    python examples/serve_persistence.py

Runs a flow against a sqlite logbook and serves the API over it, the
way a deployment would point the meter at a database its services
already write to.  Nothing here touches the flow's code.
"""

from __future__ import annotations

import contextlib
import tempfile
import threading
import time
from pathlib import Path
from wsgiref.simple_server import make_server

from oslo_utils import uuidutils
from taskflow import engines
from taskflow import task
from taskflow.patterns import linear_flow
from taskflow.persistence import backends
from taskflow.persistence import models

from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.datasource.persistence import PersistenceDataSource
from taskflow_meter.meter import Meter


class Slow(task.Task):
    """Long enough to be watched while it runs."""

    def execute(self) -> str:
        for step in range(10):
            time.sleep(0.3)
            self.update_progress((step + 1) / 10)
        return "done"


def run_flow(url: str) -> None:
    backend = backends.fetch({"connection": url})
    with contextlib.closing(backend.get_connection()) as conn:
        conn.upgrade()

    book = models.LogBook("example-book")
    flow_detail = models.FlowDetail(
        "example-flow", uuid=uuidutils.generate_uuid()
    )
    book.add(flow_detail)
    with contextlib.closing(backend.get_connection()) as conn:
        conn.save_logbook(book)

    flow = linear_flow.Flow("example-flow").add(Slow("first"), Slow("second"))
    engine = engines.load(
        flow, flow_detail=flow_detail, book=book, backend=backend
    )
    engine.run()
    backend.close()


def main() -> int:
    database = Path(tempfile.mkdtemp()) / "taskflow.db"
    url = f"sqlite:///{database}"

    worker = threading.Thread(target=run_flow, args=(url,), daemon=True)
    worker.start()
    time.sleep(1)  # let the schema and the first flow appear

    source = PersistenceDataSource(conf={"connection": url})
    with (
        Meter(source, interval=0.5) as meter,
        make_server("127.0.0.1", 8080, WSGIApp(meter)) as server,
    ):
        print("serving on http://127.0.0.1:8080")
        print("  try: curl -s localhost:8080/api/v1/flows | jq")
        print("  and: curl -N localhost:8080/api/v1/flows/<id>/stream")
        print("ctrl-c to stop")
        with contextlib.suppress(KeyboardInterrupt):
            server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

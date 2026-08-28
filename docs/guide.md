# Using taskflow-meter

How to deploy it into an OpenStack service, and how to answer the two
questions people actually have: *how far along is this flow?* and *what is it
doing right now?*

---

## 1. Point it at a taskflow backend

The meter reads what taskflow already writes. Flow and atom states go into the
persistence backend via the engine, and per-atom progress goes there too --
`storage.set_task_progress()` writes through to the backend on every
`update_progress()` call. Nothing needs to change in your flow code.

The one requirement is a **shared** persistence backend: sqlite, MySQL,
PostgreSQL, a directory, or ZooKeeper. The `memory` backend is per-process and
invisible from anywhere else.

## 2. Configure it in the service's own config file

Add a `[taskflow_meter]` section to whichever config file your service
already reads:

```ini
[taskflow_meter]
# Where the taskflow logbooks live.
connection = mysql+pymysql://user:password@host/taskflow

# Poll the backend and keep an event history (default: true).
poll = true

# Seconds between polls (default: 2.0).  Also the resolution of what can
# be observed -- a state entered and left within one interval was never
# visible.
poll_interval = 2.0

# Events retained per flow before the oldest are dropped (default: 1000).
max_events_per_run = 1000
```

`connection` may be omitted if taskflow shares the service's own database, in
which case `[database] connection` is used. **Set it explicitly if the
logbooks live anywhere else**, or the meter will faithfully report an empty
database.

To generate a sample section:

```bash
oslo-config-generator --namespace taskflow_meter
```

## 3. Deploy it

### In a paste pipeline

If your service composes its WSGI stack from `api-paste.ini`, dispatch a
prefix to the meter. It is then served on the same port, behind the same
middleware, as the API it is monitoring:

```ini
[composite:main]
use = egg:Paste#urlmap
/: your_api
/v1: your_api_v1
/taskflow-meter: taskflow_meter

[app:taskflow_meter]
paste.app_factory = taskflow_meter.contrib.paste:app_factory
```

Keep whichever urlmap factory your composite already uses; only the
`[app:taskflow_meter]` stanza and the one dispatch line are ours.

urlmap sets `SCRIPT_NAME` to the prefix it dispatched on, and the app builds
its links from that, so the mount point needs no configuring. Anything in the
paste stanza overrides the oslo.config values, for deployments that prefer to
keep it all in `api-paste.ini`:

```ini
[app:taskflow_meter]
paste.app_factory = taskflow_meter.contrib.paste:app_factory
connection = mysql+pymysql://user:password@host/taskflow
poll_interval = 5
```

### In a Pecan controller tree

```python
from taskflow_meter.contrib.pecan import MeterController


class RootController:
    v1 = V1Controller()
    taskflow_meter = MeterController()  # built from the service's config
```

Everything under that path is handed to the meter. Note that a mounted
sub-application bypasses the host's own hooks and middleware; if the host's
auth must apply, wire the route table into its router instead --
`taskflow_meter.api.routes.build_routes()` returns it as data.

### Inside FastAPI, Flask, or Django

Mounting the raw callable works in all three and needs nothing from this
package. Reach for a native adapter when the routes need to be *inside* the
host application, so its authentication, middleware and error handling apply
to them -- and, for FastAPI, so they appear in its OpenAPI schema:

```python
from taskflow_meter.contrib.fastapi import meter_router

app.include_router(
    meter_router(meter),
    prefix="/taskflow",
    dependencies=[Depends(require_admin)],
)

from taskflow_meter.contrib.flask import meter_blueprint

app.register_blueprint(meter_blueprint(meter), url_prefix="/taskflow")

from taskflow_meter.contrib.django import meter_urlpatterns

urlpatterns = [path("taskflow/", include(meter_urlpatterns(meter)))]
```

Each host is told where it lives differently -- an ASGI `root_path`, a WSGI
`SCRIPT_NAME`, a router prefix, a `url_prefix`, an `include()` -- and all of
them produce the same links. A conformance suite runs every endpoint through
all six hosts at three mount depths and compares the bytes.

### Behind gunicorn, or standalone

```python
from taskflow_meter.conf import wsgi_app_from_config

application = wsgi_app_from_config()  # gunicorn module:application
```

```bash
taskflow-meter serve --connection sqlite:///taskflow.db
```

The `serve` command runs on `wsgiref` from the standard library and binds
localhost. It is a development server.

### Many API workers

Each worker that polls is another poller on the same database. With N gunicorn
workers, either accept N pollers at a slow interval, or set `poll = false` on
the workers and give them a store somebody else fills.

Reading a taskflow backend with `poll = false` means no event history, so the
event and stream endpoints report 501 rather than serving an empty stream. To
keep those, run the collector deployment below.

### The collector deployment

For a fleet: the flows publish to a broker, one process writes to the meter's
own database, and the workers read it.

```bash
# once
taskflow-meter upgrade --store-url postgresql://user@host/meter

# the collector: consumes the broker, writes the store
taskflow-meter collect   --amqp-url amqp://guest@broker//   --store-url postgresql://user@host/meter

# the API workers: read the store, poll nothing
taskflow-meter serve --store-url postgresql://user@host/meter
```

The flows publish by attaching with an AMQP transport:

```python
from taskflow_meter.collect import attach
from taskflow_meter.transports.amqp import AMQPTransport

publisher = AMQPTransport("amqp://guest@broker//")
with attach(engine, publishers=[publisher]):
    engine.run()
```

The queue is durable and declared on publish, so a flow that starts before the
collector does is not a lost run. Events are keyed on `(run_id, seq)`, so a
collector that reconnects and redelivers writes nothing twice.

Unlike the persistence datasource, which inherits taskflow's retention, this
store keeps what it is told until told otherwise:

```python
store.prune(before=time.time() - 30 * 86400)
```

---

## 3b. Watching from inside the process running the flow

Everything above reads what taskflow persisted. If you control the code that
runs the flow, attaching to the engine gets you two things reading cannot:

```python
from taskflow_meter.collect import attach
from taskflow_meter.meter import Meter

with attach(engine) as watched:
    engine.run()
    meter = Meter(watched.store, poll=False)  # serve it however you like
```

**Latency.** A poller's floor is its interval -- seconds, by default. Attached,
a number a task reports is readable in single-digit milliseconds.

**The graph.** taskflow persists atoms but not the edges between them, so the
shape of a flow only exists in the process that compiled it. `attach` emits it
once, before anything runs, as a `flow_structure` event:

```json
{
  "nodes": [{"name": "first", "kind": "task"}, ...],
  "edges": [{"from": "first", "to": "second"}, ...],
  "atom_count": 3
}
```

Events can also go somewhere else at the same time:

```python
from taskflow_meter.transports.http import HTTPTransport

with attach(engine, publishers=[HTTPTransport("https://collector/events")]):
    engine.run()
```

### What attaching costs the flow

Nothing it can help: delivery happens on its own thread behind a bounded
queue, so a task never waits on a publisher. A full queue drops the oldest
events, counts them and logs -- blocking would stall the flow, and growing
without limit would take the process down later for reasons nobody would
connect back to monitoring. A publisher that raises is counted and logged,
and the next batch is still attempted. None of it can fail a task.

## 4. How far along is this flow?

Every flow payload carries a `completion` between 0 and 1:

```bash
curl -s http://localhost:8080/api/v1/flows/$RUN_ID | jq '{
  state, completion, running_atoms
}'
```

```json
{
  "state": "RUNNING",
  "completion": 0.375,
  "running_atoms": ["also", "working"]
}
```

From Python:

```python
flow = meter.get_flow(run_id)
print(f"{flow.completion:.0%}")  # 38%
print([atom.name for atom in flow.running_atoms])
```

### What the number means, and what it does not

`completion` is the **unweighted mean of the atoms' individual completion**.
Every atom counts the same, because taskflow gives nothing to weight them by:
no durations, no cost hints. A flow whose first atom takes an hour and whose
second takes a second will sit at 50% for an hour. Treat it as an indicator of
progress, not an estimate of time remaining.

Each atom contributes:

| Atom state | Contributes | Why |
| --- | --- | --- |
| `SUCCESS` | 1.0 | Done |
| `IGNORE` | 1.0 | A decider ruled it out; it will never run |
| `RUNNING`, `REVERTING`, `RETRYING` | its reported progress | Trusted only while running |
| `FAILURE`, `REVERT_FAILURE` | 0.0 | taskflow leaves stale progress on failure |
| `REVERTED` | 0.0 | Its revert finished, not its work |
| `PENDING` | 0.0 | Not started |

Those last two rows are why the raw `progress` field cannot be used on its
own. taskflow sets an atom's progress to **1.0 on both `SUCCESS` and
`REVERTED`**, and leaves it untouched on `FAILURE`. A naive mean of `progress`
would report a fully reverted flow as 100% complete. Both numbers are in the
payload -- `progress` is what taskflow recorded, `completion` is what it means.

### Getting better resolution

`completion` only moves when an atom finishes, unless your tasks report
progress themselves:

```python
class Upload(task.Task):
    def execute(self, files):
        for index, item in enumerate(files):
            upload(item)
            self.update_progress((index + 1) / len(files))
```

Each call write-throughs to the persistence backend, so the meter sees it from
another process within one poll interval.

---

## 5. What is it doing right now?

`running_atoms` names the atoms currently executing. It is a list because
unordered and graph flows run several at once, and it is empty between two
atoms or once the flow has finished.

For per-atom detail, ask for the flow itself or its atoms:

```bash
curl -s http://localhost:8080/api/v1/flows/$RUN_ID | jq '.atoms[] | select(.running)'
```

```json
{
  "name": "upload",
  "state": "RUNNING",
  "intention": "EXECUTE",
  "progress": 0.4,
  "completion": 0.4,
  "running": true,
  "finished": false,
  "has_result": false,
  "failure": null
}
```

`intention` is worth watching: `EXECUTE` means forward progress, `REVERT`
means the flow is unwinding.

### Watching it change

`/events` is the history, and `/stream` is the same thing live over SSE:

```bash
curl -N http://localhost:8080/api/v1/flows/$RUN_ID/stream
```

```
retry: 3000
: stream open

event: atom_state
id: 4
data: {"kind":"atom_state","atom_name":"upload","state":"RUNNING",...}

event: atom_progress
id: 5
data: {"kind":"atom_progress","atom_name":"upload","progress":0.4,...}
```

Each frame's `id` is the event's sequence number, gap-free per flow. A browser
`EventSource` sends the last one back as `Last-Event-ID` when it reconnects, so
a dropped connection resumes rather than leaving a hole. Driving it by hand,
poll `/events?since_seq=N` with the `next_seq` from the previous response.

Two events are not flow activity and are worth handling:

- **`event: gap`** -- you fell further behind than `max_events_per_run` and
  some events were dropped. Re-read the flow rather than assuming continuity.
- **`event: end`** -- the flow finished. Close the connection; an
  `EventSource` will otherwise reconnect and be told the same thing again.

---

## 6. What the meter cannot tell you

Worth knowing before you go looking for it:

- **When something happened.** taskflow records no timestamps below the
  logbook, so flow and atom details carry none. `observed_at` is when the
  meter looked, not when the state changed -- accurate to the poll interval,
  no better.
- **The shape of the flow.** Atoms are persisted; the edges between them are
  not. Nothing read from persistence can draw the graph.
- **What a task returned.** Results can be arbitrarily large application
  objects and are not copied into the monitoring path. `has_result` reports
  whether there is one.
- **Anything that happened between two polls.** A state entered and left
  within one interval was never observable.

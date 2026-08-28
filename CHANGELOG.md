# Changelog

Notable changes, newest first. The version comes from the git tag, so an
entry lands here in the release it ships in.

## Unreleased

Nothing yet.

## 1.0.0 - 2026-08-28

First release. Monitoring interfaces for observing
[OpenStack TaskFlow](https://opendev.org/openstack/taskflow) flow execution
progress.

### Reading an existing deployment

- **Read-only persistence datasource.** Reports flow and atom states and
  per-atom progress from whatever taskflow already wrote, with no changes to
  flow code. Works because `update_progress()` write-throughs to the
  persistence backend on every call.
- **Poller and diff engine.** Turns a state-only backend into a resumable
  event stream by comparing successive snapshots.
- **`Meter`.** Owns the source, the store and the poller, with a lifecycle
  that survives being mounted (where ASGI lifespan never arrives).

### Serving it

- **HTTP API**: flows, one flow, its atoms, its event history, and the same
  events live over SSE with `Last-Event-ID` resumption.
- **A hand-rolled ASGI callable and a hand-rolled WSGI callable**, with no web
  framework dependency, mountable at any sub-path. A conformance suite
  compares them byte for byte.
- **Native adapters** for FastAPI, Flask, Django, Pecan and paste pipelines,
  for when the routes should live inside the host application so its auth and
  middleware apply. All six hosts are checked against each other at three
  mount depths.
- **`taskflow-meter serve`**, on stdlib `wsgiref`, so serving costs no
  dependency.
- **oslo.config integration**, so a service configures the meter in the config
  file it already reads.

### Watching a flow as it runs

- **`attach(engine)`**: a listener for state changes, a tap on each task's own
  notifier for the progress taskflow never re-emits, and the flow's graph,
  which taskflow never persists.
- Progress becomes readable in single-digit milliseconds rather than a poll
  interval.
- Delivery is off-thread behind a bounded queue: no publisher failure, however
  bad, can fail a task or slow it measurably.

### Collecting at scale

- **AMQP transport** (kombu) and a **SQLAlchemy datasource** with alembic
  migrations, so flows publish once, one `taskflow-meter collect` process
  writes, and any number of API workers read without polling.
- Events are keyed on `(run_id, seq)`, so a collector that reconnects and
  redelivers writes nothing twice.
- `prune()` gives the deployment a retention policy of its own.

### Known limits

These are properties of what taskflow records, not gaps to be filled later:

- No timestamps below the logbook, so observation time is all a reader has.
- No graph topology in persistence; only the in-process path knows the shape.
- Nothing that happened between two polls was ever observable.

# Changelog

Notable changes, newest first. The version comes from the git tag, so an
entry lands here in the release it ships in.

## Unreleased

### Python 3.10

- **The floor is Python 3.10**, down from 3.11. The reasoning is the one
  behind the dependency floors: a package meant to be co-installed into a
  service does not get to choose the interpreter that service runs on, and
  3.10 is what Ubuntu 22.04 ships, which a good deal of OpenStack still runs
  on. Nothing in the package needed 3.11 -- two spellings did. `typing.Self`
  is a bound `TypeVar`, which is how the same thing was written before 3.11,
  and `enum.StrEnum` falls back to a `str, Enum` mixin whose `str()` is its
  value, that being the only property of it the wire format rests on. CI runs
  the suite on 3.10 alongside every other version, and the `lowest-direct`
  job moved there, since it exists to install the oldest releases on the
  oldest interpreter supported.

### Fixed

- **An SSE stream no longer raises out of its own sleep on Python 3.10.** The
  sleep between polls suppressed a bare `TimeoutError`, which catches nothing
  there: `asyncio.TimeoutError` only became the builtin in 3.11 and is a
  class of its own before that. It is spelled `asyncio.TimeoutError` now,
  which is the same object from 3.11 on. Found by running the suite on 3.10,
  which is the whole argument for testing a floor rather than declaring one.

- **A failed atom now says what it failed of.** The fold layer has always
  understood `failure` and `revert_failure`; the listener never emitted them,
  so watching an engine reported *that* a task failed and reading persistence
  reported *why*. It now renders the `Failure` taskflow hands it, in the same
  shape the persistence datasource produces -- `models.failure_dict`, shared by
  both -- so a client cannot tell which producer it is reading.

- **A flow's name is no longer lost when the graph is emitted first.** The
  in-process producer emits `FLOW_STRUCTURE` before anything runs, so that is
  the event a run gets seeded from -- and it carries no flow name, while the
  state events that follow were dropping theirs. Snapshots from `attach()`
  reported `name = ""` for the whole run. The name is now taken from whichever
  event carries it.

### Sharing a database with the host service

- **`upgrade()` takes `version_table`.** The store's alembic tree kept its
  revision in `alembic_version`, which is also where a host service keeps its
  own -- so the two trees each read the other's revision as one they have never
  heard of. Deployments that put the store in the service's database can now
  give it a table of its own, and nothing changes for those that do not.

### Documentation

- **`docs/PLAN.md` is gone.** It was a pre-build plan, and by 1.0 it was
  describing a package that no longer matched it -- milestones, a packaging
  sketch superseded by the real one, and an ASGI design (`meter.lifespan`,
  `run_in_executor`, WebSocket support) that is not what shipped.
- **`docs/design.md` replaces it** with the parts that were worth keeping and
  are still true, re-verified against the code and against taskflow rather
  than transcribed: what taskflow does and does not record and what follows
  from it, the decisions and what each one costs, the rule that the emit side
  can never hurt the flow, and the mount-safety, lifecycle and good-citizen
  rules an embedded app has to obey.
- The README no longer claims to be pre-alpha at milestone M0, and now has a
  documentation index. The guide's sections are numbered in sequence.

### An oslo.messaging transport

Events can now go out on the notification bus an OpenStack service is
already configured for, instead of over a broker connection of the meter's
own. The operator's existing `transport_url` and
`[oslo_messaging_notifications]` settings apply unchanged, whichever driver
they chose is the one used, and the events land beside everything else the
deployment already collects.

```bash
taskflow-meter collect --transport oslo-messaging \
  --url rabbit://guest@broker// --store-url postgresql://host/meter
```

```python
from taskflow_meter.transports.oslo_messaging import OsloMessagingTransport

publisher = OsloMessagingTransport(conf=CONF)  # no URL: the service decides
```

- **Notifications, not RPC.** RPC is a call with a reply and a server
  expected to be listening; a flow reporting progress wants neither.
- **`collect` grew `--transport`**, defaulting to `amqp`. Its `--amqp-url`
  is now spelled `--url`; the old name still works, so collectors deployed
  against 1.0.0 keep running.
- The wire envelope is shared with the AMQP transport, so a collector parses
  the same thing either way.
- New extra: `taskflow-meter[oslo-messaging]`, floor **oslo.messaging 6.0.0**
  (5.0.0 fails the suite), found the same way as every other floor.

**Pick this one knowing what it gives up.** The AMQP transport declares its
durable queue on every publish, so a flow that runs before the collector ever
has is not a lost run. A notifier cannot do that -- the queue belongs to the
listener, and a broker discards what it has nothing to route to. Start the
collector once before the first flow and the queue is durable from then on;
if flows genuinely run before any collector exists, use the AMQP transport.

### Dependencies lowered

This package is meant to be co-installed into a service whose dependency
versions it does not get to choose, and 1.0.0 asked for far more than it
used. Every floor is now the oldest release the suite actually passes
against, found by running it rather than by reading release notes.

| | 1.0.0 | now |
| --- | --- | --- |
| taskflow | >=6.4.0 | **>=4.2.0** |
| oslo.config | >=9.0.0 | **>=6.9.0** |
| oslo.cache | >=3.0.0 | *dropped* |
| oslo.serialization | >=2.18.0 | *dropped* |
| oslo.utils | >=3.33.0 | *dropped* |
| stevedore | >=1.20.0 | *dropped* |
| SQLAlchemy (extra) | >=2.0 | **>=1.4.0** |
| alembic (extra) | >=1.13 | **>=1.2.0** |
| kombu (extra) | >=5.0 | **>=5.1.0** |

- **Four required dependencies removed.** Nothing in the package imports
  oslo.cache, oslo.serialization, oslo.utils or stevedore. The three that
  taskflow needs arrive with taskflow. oslo.cache was reserved for a caching
  datasource that was deferred; it returns as an extra when that lands.
- **The `memcache`, `etcd` and `prometheus` extras are gone.** All three
  installed libraries no code imported. `pip install taskflow-meter[memcache]`
  now warns that the extra is unknown instead of pulling in dogpile.cache.
- **`kombu` moved up**, not down: 5.0 does not import on Python 3.11.
- **oslo.config compatibility fix.** `resolve_connection()` raised
  `RequiredOptError` with the group *name*; oslo.config only learned to
  accept a string there recently, and older versions raise from `__str__`
  while formatting it -- burying the very message being reported. It now
  passes the `OptGroup`, which every version handles.
- **A `lowest-direct` CI job** installs every floor exactly, on Python 3.11,
  and runs the whole suite. The floors were a claim nobody checked before:
  every other job resolves to the newest release.
- The contrib adapters still declare no dependency on their hosts, and are
  now exercised against Django 3.2, Flask 2.3.3, FastAPI 0.100, Pecan 1.4 and
  PasteDeploy 2.0.

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

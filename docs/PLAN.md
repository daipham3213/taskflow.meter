# taskflow-meter — design plan

Monitoring interfaces (ASGI, WSGI, datasource, transports) for observing
[OpenStack TaskFlow](https://opendev.org/openstack/taskflow) flow execution
progress.

Target: `taskflow >= 6.4.0`, Python >= 3.11 (taskflow 6.4 declares
`Requires-Python >=3.11`, classifiers through 3.14).

---

## 1. What TaskFlow actually gives us

Verified against taskflow 6.4.0 source. These findings drive the whole design.

| Fact | Location | Consequence |
| --- | --- | --- |
| `engine.notifier` fires on flow state transitions; `engine.atom_notifier` on atom state transitions | `engines/base.py:42,47` | A `Listener` sees state changes only |
| `task.update_progress(p)` is handled by `TaskAction._on_update_progress`, which writes to storage and **never re-emits on `atom_notifier`** | `engines/action_engine/actions/task.py:82` | A `Listener` **cannot** see intra-task progress. Need a separate tap on each atom's own `task.notifier` |
| `storage.set_task_progress()` write-throughs to the persistence backend, storing `meta['progress']` and `meta['progress_details']` | `storage.py:616` → `_update_atom_metadata` → `_with_connection(_save_atom_detail)` | Fine-grained progress **is** durable and readable out-of-process |
| `LogBook` has `created_at`/`updated_at`; `FlowDetail` and `AtomDetail` have neither | `persistence/models.py:102,295,479` | No per-atom start/end times. We stamp observation times ourselves |
| Flow DAG edges are **not** persisted; `FlowDetail` holds a flat set of atoms | `persistence/models.py:295` | Graph topology requires the in-process path or the flow definition |
| `Connection` exposes `get_logbooks()`, `get_flows_for_book()`, `get_atoms_for_flow()` with no filtering or paging | `persistence/base.py:98-118` | Listing is a full scan — cache in front of it |
| Only `EVENT_UPDATE_PROGRESS` is registrable on a task notifier (`Task.TASK_EVENTS`) | `task.py:39,55` | The progress tap has exactly one event to bind |
| Progress callbacks are proxied back from remote workers | `engines/worker_based/executor.py:239` | Worker-based engines report progress the same way |
| taskflow discovers backends via stevedore entry points (`taskflow.persistence`, `taskflow.engines`, ...) | package metadata | Mirror this convention for our own plugins |

**Bottom line:** for a deployment already using a shared persistence backend
(sqlite/mysql/postgresql/dir/zookeeper — not `memory`, which is per-process),
we can monitor flows *including sub-task progress* with **zero changes to the
flow code**. The live in-process path is an enhancement (lower latency, DAG
topology, richer failure detail), not a prerequisite.

## 2. Decisions taken

| Decision | Choice | Rationale |
| --- | --- | --- |
| Topology | Library-first, both in-process and out-of-process supported | Transports designed in from day one so the collector is an addition, not a rewrite |
| Web layer | Zero-framework: hand-rolled ASGI and WSGI callables | No Starlette/FastAPI dependency; maximum embeddability into existing OpenStack services |
| Embedding | Mountable into **any** ASGI/WSGI framework at any sub-path, plus optional native adapters in `contrib/` | Standalone `serve` is one deployment mode, not the only one |
| Primary datasource | Read-only adapter over taskflow persistence | Monitors existing deployments untouched |
| Caching / shared state | `oslo.cache` (not a direct redis client) | Operator picks the backend (dict, memcached, pymemcache, redis, etcd3gw) |
| Config | `oslo.config` | Already required by oslo.cache; matches OpenStack operator expectations |
| Build backend | hatchling + hatch-vcs | Per project requirement; replaces the current `uv_build` |
| License | Apache-2.0 | Matches the taskflow/OpenStack ecosystem |

### oslo.cache is a cache, not a bus

It gives shared *state* with TTLs, not pub/sub fanout. So:

- **Cross-process liveness** = the collector polls persistence, diffs, and
  publishes snapshots into the cache region; API workers read the cache.
- **Cross-process push** (optional, M5) = the AMQP transport via `kombu`,
  which taskflow already depends on for its worker-based engine.
- **SSE/WS streams** are fed by the poller's diff stream, not by the cache.

Note also that most oslo.cache backends (memcached especially) cannot enumerate
keys. The flow *list* is therefore always answered from persistence (cached as
a whole rendered page), never by scanning cache keys.

## 3. Architecture

```
 EMIT SIDE (optional, in the app running the flow)   SERVE SIDE (monitoring)
 ─────────────────────────────────────────────────┬──────────────────────────
  collect/                                        │  datasource/
    MeterListener   -> flow + atom state          │    persistence           <-- primary
    ProgressTap     -> atom's own task.notifier   │    memory
    PollingSampler  -> engine.storage gap-fill    │    sqlalchemy (own schema)
          |                                       │        ^
          v  normalized Event                     │        | apply(event)
  transports/ Publisher  ── memory | http | amqp ─┼──> Subscriber
                                                  │  cache/  (oslo.cache region)
                                                  │  api/    asgi + wsgi
```

The read path needs none of the left column: `PersistenceDataSource` polls the
taskflow DB, diffs successive snapshots, and synthesizes the same `Event`
objects the emit side would have produced. One event model, two producers.

## 4. Package layout

```
taskflow_meter/
  __init__.py                  # attach(engine, ...) / serve() one-liners
  _version.py                  # generated by hatch-vcs (gitignored)
  conf.py                      # oslo.config opt registration + list_opts entry point
  events.py                    # Event, EventKind, seq, JSON codec
  models.py                    # FlowSnapshot, AtomSnapshot, Progress (plain dataclasses)
  diff.py                      # snapshot -> snapshot => [Event]  (shared by poller + tests)
  plugins.py                   # stevedore loaders for transports + datasources

  collect/
    listener.py                # MeterListener(taskflow.listeners.base.Listener)
    progress.py                # ProgressTap: registers EVENT_UPDATE_PROGRESS per atom
    attachment.py              # attach(): listener + tap + pipeline, as one call
    pipeline.py                # bounded queue + sender thread; never raises into the flow

  transports/
    base.py                    # Publisher / Subscriber ABCs
    memory.py                  # in-process queue
    http.py                    # webhook POST (batched, retrying)
    amqp.py                    # kombu; extra = "amqp"

  datasource/
    base.py                    # apply/get_flow/list_flows/get_atoms/events_since
    persistence.py             # read-only over LogBook/FlowDetail/AtomDetail
    memory.py
    sqlalchemy/                # own schema + alembic; extra = "sqlalchemy"
    cached.py                  # oslo.cache decorator wrapping any datasource

  meter.py                     # Meter: owns datasource + cache + poller; start/stop; the only handle

  api/
    service.py                 # framework-free query + stream logic (the source of truth)
    http.py                    # MeterRequest / MeterResponse / StreamResponse dataclasses
    routes.py                  # declarative Route table: (method, template, handler, name)
    router.py                  # path-template matcher over routes.py
    serializers.py             # dataclass -> JSON
    sse.py                     # SSE framing, shared
    asgi.py                    # ASGIApp(meter): pure ASGI 3 callable, mount-safe
    wsgi.py                    # WSGIApp(meter): pure WSGI callable, mount-safe

  contrib/                     # optional native adapters; framework imported lazily
    fastapi.py                 # meter_router(meter) -> APIRouter
    flask.py                   # meter_blueprint(meter) -> Blueprint
    django.py                  # meter_urlpatterns(meter) -> [path(...)]
    pecan.py                   # MeterController(): delegates to the WSGI app
    paste.py                   # app_factory for an api-paste.ini pipeline

  cli.py                       # taskflow-meter serve | collect | tail | dump

tests/
  unit/                        # mirrors the package: tests/unit/<pkg>/test_<mod>.py
    datasource/                #   for taskflow_meter/<pkg>/<mod>.py
    api/
  integration/                 # real engines (linear/graph/unordered, serial/parallel)
  functional/                  # ASGI+WSGI over a live sqlite-backed flow; packaging
  conformance/                 # same suite through every host framework + mount prefix
  conftest.py                  # engine fixtures, ProgressingTask, sqlite logbook
tools/check_test_layout.py     # enforces the mirror; run by tox -e pep8
docs/  .github/workflows/  tox.ini
```

## 5. Event model

Single envelope, produced identically by the listener path and the diff engine:

```python
@dataclass(frozen=True, slots=True)
class Event:
    run_id: str  # FlowDetail uuid
    book_id: str | None  # LogBook uuid
    seq: int  # monotonic per run_id
    ts: float  # observation time (our clock — taskflow has none below LogBook)
    kind: EventKind  # flow_state | atom_state | atom_progress
    # | flow_structure | flow_result | heartbeat
    atom_name: str | None
    atom_uuid: str | None
    state: str | None
    old_state: str | None
    intention: str | None
    progress: float | None
    details: dict  # progress_details, failure dict, result summary
```

- Failures serialized via `taskflow.types.failure.Failure.to_dict()` — never pickle.
- `flow_structure` is emitted once at attach time from the compiled graph
  (in-process path only) so a UI can render the DAG instead of a log tail.
- `seq` is assigned by the producer per `run_id`; the diff engine derives it
  from a persisted counter so a restarted collector does not replay.
- `ts` is explicitly *observation* time, documented as such — taskflow records
  no atom-level timestamps, so we must not pretend to know when a state
  actually changed. The in-process path's `ts` is accurate; the poller's is
  bounded by the poll interval.

## 6. Safety rules for the emit side

We run inside somebody else's flow. Non-negotiable:

1. Every callback body is wrapped in `try/except Exception` + log. A monitoring
   bug must never fail a task.
2. Callbacks fire on executor threads under the parallel engine — all shared
   state is lock-guarded or queue-mediated.
3. All I/O happens on a sender thread behind a **bounded** queue with an
   explicit drop policy (`drop_oldest`, counted and logged). A slow webhook
   must never stall a task.
4. `attach()` is a context manager that always deregisters, including on error.
5. The progress tap checks `task.notifier.can_be_registered(...)` before
   binding, and deregisters symmetrically.

## 7. HTTP API

```
GET  /healthz
GET  /api/v1/flows?state=&book_id=&limit=&marker=
GET  /api/v1/flows/{run_id}
GET  /api/v1/flows/{run_id}/atoms
GET  /api/v1/flows/{run_id}/events?since_seq=
GET  /api/v1/flows/{run_id}/stream          # SSE, Last-Event-ID honoured
WS   /api/v1/ws                             # ASGI only; subscribe/unsubscribe frames
GET  /metrics                               # optional, extra = "prometheus"
```

`api/service.py` holds all logic and returns plain dataclasses. `asgi.py` and
`wsgi.py` are thin adapters over it — neither is the source of truth, and the
same route table drives both.

### Embedding into a host framework

This is a first-class requirement, not an afterthought: `taskflow-meter` must
drop into an existing FastAPI/Starlette/Litestar/Quart (ASGI) or
Flask/Django/Pyramid (WSGI) application, at an arbitrary sub-path, without
either side knowing much about the other.

Three integration levels, cheapest first:

```python
# 1. Mount the raw callable — works in any framework that can mount ASGI/WSGI
meter = Meter(datasource=..., cache=...)
app.mount("/taskflow", ASGIApp(meter))  # Starlette / FastAPI
app.wsgi_app = DispatcherMiddleware(
    app.wsgi_app,  # Flask / werkzeug
    {"/taskflow": WSGIApp(meter)},
)

# 2. Native adapter — routes registered in the host's own router, so the host's
#    auth, middleware, error handling and OpenAPI apply to them
app.include_router(meter_router(meter), prefix="/taskflow")  # contrib.fastapi
app.register_blueprint(meter_blueprint(meter), url_prefix="/taskflow")

# 3. Hand-wire — iterate api.routes.ROUTES and register handlers yourself
```

Level 2 exists because mounting a sub-app bypasses the host's middleware
stack; teams with existing auth usually want the routes *inside* their app.
`contrib/` modules import their framework lazily and are never imported by the
core, so the zero-dependency promise holds.

#### Mount-safety rules (verified against Starlette 1.6.0)

- **Never trust `scope["path"]` alone.** Modern Starlette `Mount` does *not*
  rewrite `path`; it only extends `root_path` (`routing.py:421`). Pre-0.33
  Starlette stripped `path` instead. Compute the route path exactly as
  Starlette's own `get_route_path` does (`_utils.py:96`): strip `root_path`
  **only if it is actually a prefix of `path`**, else use `path` unchanged.
  That one rule covers old Starlette, new Starlette, and servers given
  `--root-path` where the prefix never appears in `path`.
- **WSGI is unambiguous**: the server or dispatcher has already split
  `SCRIPT_NAME` / `PATH_INFO`. Route on `PATH_INFO`, build links from
  `SCRIPT_NAME`.
- **Generated links** (`links.self`, the SSE stream URL advertised in a flow
  payload) are built from the mount prefix plus `X-Forwarded-Prefix`, never
  from a hard-coded `/api/v1`.
- **Never mutate the host's `scope`/`environ`** — copy before touching.

#### Lifecycle without lifespan

**Mounted ASGI apps never receive the lifespan scope.** Starlette's `Router`
handles `scope["type"] == "lifespan"` at the root and never forwards it to a
`Mount` (`routing.py:678`). So resource startup *cannot* hang off lifespan.
Instead, `Meter` owns its own lifecycle and offers three ways in:

1. **Explicit** (recommended): `meter.start()` / `meter.stop()`, also a context
   manager. Host apps call it from their own lifespan/`AppConfig.ready()`.
2. **Optional lifespan handler**: `meter.lifespan` for when our ASGI app *is*
   the root app.
3. **Lazy fallback**: first request starts the poller if it is not running,
   with an `atexit` stop. Idempotent and refcounted, so double-starting from
   both a host lifespan and a first request is harmless.

**Multi-worker caveat, documented loudly**: N gunicorn/uvicorn workers each
constructing a polling `Meter` means N pollers hammering the taskflow DB. Two
supported modes — `Meter(poll=True)` for single-process/embedded use, and
`Meter(poll=False)` for API workers reading a cache/datasource that a separate
`taskflow-meter collect` process keeps warm.

#### Being a good citizen in someone else's app

No module-level singletons — every app is a factory over a `Meter` instance, so
two meters can coexist in one process. No CORS headers by default (the host
handles that). No global exception handlers, no logging configuration, no
signal handlers. `HEAD` and `OPTIONS` answered correctly. Unknown paths inside
our prefix return a JSON 404 rather than raising into the host.

Because both native callables ship, no `a2wsgi`-style bridge is needed in
either direction; it stays an escape hatch, not a dependency.

### Hand-rolling notes

- **Sync core, async edge**: handlers are plain sync functions returning
  `MeterResponse`, so WSGI and Django-sync call them directly. The ASGI adapter
  offloads blocking datasource/cache calls with
  `loop.run_in_executor(...)` — stdlib only, no `anyio` dependency — and feeds
  streams from a thread-safe queue via an async generator.
- **Router**: compiled path templates (`/api/v1/flows/{run_id}/atoms`) →
  handler, method map, 404/405 handling. ~80 lines, fully unit-tested.
- **ASGI**: implement the `http` and `websocket` scopes. The server performs
  the WebSocket handshake; we only handle `websocket.connect` / `accept` /
  `receive` / `send` / `disconnect`.
- **SSE**: shared framing module (`id:`, `event:`, `data:`, `retry:`, heartbeat
  comments). ASGI streams from an async generator; WSGI returns an iterable.
- **WSGI caveat, documented up front**: a sync WSGI worker holds one thread per
  open SSE connection. Recommend gevent/eventlet workers for streaming, or
  plain polling of `/events?since_seq=` for the WSGI deployment. WebSocket is
  ASGI-only by nature.

## 8. Packaging

```toml
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"

[project]
name = "taskflow-meter"
dynamic = ["version"]
requires-python = ">=3.11"
license = "Apache-2.0"
dependencies = ["taskflow>=6.4.0", "oslo.config>=9.0.0", "oslo.cache>=3.0.0",
                "oslo.serialization>=2.18.0", "oslo.utils>=3.33.0", "stevedore>=1.20.0"]

[project.optional-dependencies]
memcache    = ["oslo.cache[dogpile]"]
sqlalchemy  = ["SQLAlchemy>=2.0", "alembic>=1.13"]
amqp        = ["kombu>=4.3.0"]
prometheus  = ["prometheus-client>=0.20"]
all         = [...]

[project.scripts]
taskflow-meter = "taskflow_meter.cli:main"

[project.entry-points."taskflow_meter.datasource"]
persistence = "taskflow_meter.datasource.persistence:PersistenceDataSource"
memory      = "taskflow_meter.datasource.memory:MemoryDataSource"
sqlalchemy  = "taskflow_meter.datasource.sqlalchemy:SQLADataSource"

[project.entry-points."taskflow_meter.transport"]
memory = "taskflow_meter.transports.memory:MemoryTransport"
http   = "taskflow_meter.transports.http:HTTPTransport"
amqp   = "taskflow_meter.transports.amqp:AMQPTransport"

[project.entry-points."oslo.config.opts"]
taskflow_meter = "taskflow_meter.conf:list_opts"

[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "taskflow_meter/_version.py"
```

The `oslo.config.opts` entry point makes `oslo-config-generator` emit a sample
config — the thing OpenStack operators expect from a package like this.

`contrib/` adapters declare **no** runtime dependencies: a host importing
`taskflow_meter.contrib.flask` already has Flask. Framework versions are pinned
only in the CI conformance matrix, so we never constrain a host's dependency
resolution.

## 9. CI (GitHub Actions)

- **`ci.yml`** — on push/PR: `ruff check` + `ruff format --check`, `mypy`,
  `pytest` matrix over Python 3.11/3.12/3.13/3.14 on ubuntu-latest, coverage
  uploaded once. Integration tests run real engines against sqlite; the
  memcached-backed cache tests run against a `memcached` service container and
  are skipped locally when absent.
- **`conformance.yml`** — the mount-conformance suite (§10) run against
  FastAPI, Starlette, Flask, Django, and the bare callables under
  uvicorn/gunicorn, each at both `/` and a `/deep/prefix` mount. This is the
  job that keeps the "plugs into anything" promise honest as frameworks move.
- **`release.yml`** — on tag push `v*`: `hatch build`, then PyPI **Trusted
  Publishing** via OIDC (`pypa/gh-action-pypi-publish`, no API token stored),
  plus a GitHub Release with the artifacts attached. hatch-vcs derives the
  version from the tag, so the tag is the single source of version truth.
- **`docs.yml`** (later) — build and publish to GitHub Pages.

Concurrency groups cancel superseded runs; `permissions:` blocks are minimal
(`id-token: write` only in the release job).

## 10. Testing strategy

- **`diff.py` is the highest-value unit under test** — every state transition
  pair, progress monotonicity, resumed flows, atoms appearing mid-run.
- **Integration** uses real taskflow engines, not mocks: linear/graph/unordered
  patterns × serial/parallel engines, with a `ProgressingTask` that emits a
  known progress sequence, asserting the tap captures all of it and the flow
  result is unaffected.
- **Chaos-ish safety test**: a transport that raises on every `emit` must not
  change flow outcome or timing materially.
- **Functional**: run a flow against a sqlite logbook, then drive both the ASGI
  and WSGI callables against the same `PersistenceDataSource` and assert
  byte-identical JSON responses for the shared routes.
- **Mount conformance** is its own suite, parametrised over
  (host framework × mount prefix × integration level). Every combination must
  return byte-identical JSON for the shared routes, and every generated link
  must resolve back to a real route under that prefix. A regression here means
  we silently broke somebody's deployment.
- **Unit tests mirror the package.** `taskflow_meter/<pkg>/<mod>.py` is
  tested by `tests/unit/<pkg>/test_<mod>.py`; anything that does not target a
  single module lives in `tests/functional`, `tests/integration` or
  `tests/conformance`. `tools/check_test_layout.py` enforces this and runs in
  `tox -e pep8` and in CI, because a convention nothing checks is a
  convention that decays.
- No external services required for the default `pytest` run.

## 11. Milestones

| # | Scope | Exit criterion |
| --- | --- | --- |
| M0 **(done)** | Swap `uv_build` -> hatchling + hatch-vcs; ruff, mypy, `ci.yml`, `release.yml`, Apache-2.0, README | Lint, types, tests and build all green locally; wheel ships `py.typed` + a git-derived version |
| M1 **(done)** | `states.py`, `events.py`, `models.py`, `diff.py`, `datasource/base.py` + `memory` | Diff engine fully unit-tested; 100% branch coverage on every module in this milestone |
| M2 **(done)** | `datasource/persistence.py` + `poller.py` + `meter.py` lifecycle | Cross-process test: a flow runs in a subprocess against a sqlite logbook while the meter observes its states and per-atom progress from the parent |
| M3 **(done)** | `api/` core: `service`, `http`, `routes`, `router`, `serializers`, `sse` + `api/asgi.py` | REST + SSE over a plain ASGI 3 callable, no web framework; mount-safe path handling tested against both Starlette conventions, and every link a payload emits is asserted to resolve to a real route |
| M4 **(done)** | `api/wsgi.py`, plus `api/dispatch.py` shared by both callables, and `taskflow-meter serve` on stdlib wsgiref | Parity suite: both callables byte-identical across every shared route, verb, query and mount prefix -- including a whole SSE stream |
| M5 **(done)** | `conf.py` (oslo.config), `contrib/` for paste, Pecan, FastAPI, Flask and Django, `running_atoms`, `docs/guide.md`, `conformance.yml` | Every endpoint runs through all six hosts at three mount depths and returns identical bytes; every link a payload emits resolves to a real route under that host's own prefix |
| M6 *(skipped for now)* | `cache/` via oslo.cache + `oslo-config-generator` sample | Flow list served from cache; documented invalidation and TTL semantics (`conf.py` and the `poll = false` worker mode landed early, in M5) |
| M7 **(done)** | `collect/` (listener, progress tap, pipeline, `attach()`) + `transports/` memory, datasource and http | Progress reported by a task is readable in ~1ms serial and ~17ms parallel, and the compiled graph is emitted as a `flow_structure` event; a broken publisher changes neither the flow's outcome nor its timing |
| M8 **(done)** | `datasource/sqlalchemy` + alembic; `transports/amqp`; `fold.py` shared by both stores; `collect` and `upgrade` commands | A flow publishes to a broker, a collector writes the meter's own schema, and an API worker serves it with no polling -- with replay proven idempotent and the migration checked against the models |
| M9 **(ready)** | `examples/`, `CHANGELOG.md`, `docs/releasing.md`, guide and README complete | Everything but the tag: the release workflow, the checklist and the trusted-publisher setup are documented, and tagging is the maintainer's call |

M0-M5 is the smallest genuinely useful product: point it at an existing
taskflow database, mount it into whatever service you already run, and get a
live progress API without touching the flows.

## 12. Open questions

1. **Poll interval vs. load** — persistence listing is a full scan. Default
   interval, per-run adaptive backoff for finished flows, and whether to add an
   optional `updated_at` index on the sqlalchemy backend.
2. **Retention** — the persistence adapter inherits the deployment's retention.
   The sqlalchemy datasource needs its own pruning policy.
3. **Auth** — none in v1 (assume the API sits behind an existing service or is
   bound to localhost). Keystone middleware is a plausible M9.
4. **Multi-book scoping** — whether the API should expose LogBooks as
   first-class resources or keep `book_id` as a filter only.
5. **Auth on the collector's queue** — the AMQP transport takes a URL and
   nothing else; a deployment wanting per-publisher credentials or TLS
   currently supplies them in the URL.

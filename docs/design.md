# How taskflow-meter works, and why

The [guide](guide.md) covers deploying it. This covers the reasoning
underneath: what taskflow does and does not record, what follows from that,
and the rules the embedding code has to obey. It is here so the next person
to change something knows which parts are load-bearing.

---

## 1. What taskflow gives us

Everything else follows from these. They are properties of taskflow, verified
against it rather than assumed. The suite keeps them honest from two
directions: `tests/functional/test_packaging.py` pins the API names the
package relies on, and `tests/functional/test_cross_process.py` proves the
behaviour that matters most -- progress reported by a task in one process
being readable from another.

| Fact | Consequence |
| --- | --- |
| `engine.notifier` fires on flow state transitions, `engine.atom_notifier` on atom state transitions | A `Listener` sees state changes, and only state changes |
| `task.update_progress()` is handled by `TaskAction._on_update_progress`, which writes to storage and **never re-emits on `atom_notifier`** | A listener cannot see intra-task progress. Reading it live needs a separate tap on each atom's own notifier |
| `Task.TASK_EVENTS` is exactly `('update_progress',)` | That tap has one event to bind, and no others will appear |
| `storage.set_task_progress()` write-throughs to the persistence backend, as `meta['progress']` and `meta['progress_details']` | Fine-grained progress **is** durable, and readable from another process |
| `LogBook` has `created_at`/`updated_at`; `FlowDetail` and `AtomDetail` have neither | No per-atom start or end times exist to report. Observation times are stamped by the meter, and mean only when it looked |
| Flow DAG edges are not persisted -- a `FlowDetail` holds a flat set of atoms | Nothing read from persistence can draw the graph. Topology needs the in-process path |
| `Connection` exposes `get_logbooks()`, `get_flows_for_book()`, `get_atoms_for_flow()`, with no filtering or paging | Listing is a full scan |
| Progress callbacks are proxied back from remote workers | Worker-based engines report progress the same way |
| taskflow discovers backends through stevedore entry points | Our own plugins follow the same convention |

**The bottom line, and the reason this package can exist:** a deployment
already using a shared persistence backend can be monitored *including
per-task progress* with no changes to flow code. The in-process path is an
enhancement -- lower latency, plus the graph -- not a prerequisite.

The `memory` backend is the exception: per-process, and invisible from
anywhere else.

## 2. Decisions, and what they cost

| Decision | Why | What it costs |
| --- | --- | --- |
| Read-only over taskflow persistence as the primary source | Monitors existing deployments untouched | Resolution is the poll interval; anything entered and left between two polls was never observable |
| Hand-rolled ASGI and WSGI callables, no web framework | Maximum embeddability: a service does not inherit Starlette because it wanted monitoring | Two implementations to keep identical, which is what the conformance suite is for |
| Both callables ship, rather than one plus a bridge | No `a2wsgi`-style shim in either direction | -- |
| Mountable at any sub-path, plus native adapters in `contrib/` | Standalone `serve` is one deployment mode, not the only one | Every link has to be built from a discovered prefix |
| oslo.config for configuration | What OpenStack operators expect; the service configures the meter in the file it already reads | -- |
| Completion derived from state, never from raw `progress` | taskflow sets progress to 1.0 on `REVERTED` as well as `SUCCESS`, so a mean of `progress` reports a fully reverted flow as complete | Two numbers in the payload instead of one |

## 3. The emit side must never hurt the flow

The rule the whole in-process path rests on: **nothing monitoring does can
change a flow's outcome or its timing measurably.** Concretely:

- A publisher that raises is counted and logged, and the next batch is still
  attempted. It cannot propagate into the engine.
- Delivery happens on its own thread behind a **bounded** queue, so a task
  never waits on a publisher. Blocking would stall the flow; growing without
  limit would take the process down later, for a reason nobody would connect
  back to monitoring.
- A full queue drops the oldest events, counts the drops, and logs. Losing
  old monitoring data beats losing the process.
- A listener whose callback raises logs and carries on, and the flow finishes
  normally.

The receiving side makes the opposite trade, deliberately: a handler that
fails leaves the batch on the broker rather than acknowledging it. Silently
dropping monitoring data is worse than redelivering it.

## 4. Embedding into somebody else's application

Three ways in, in increasing order of how much the host is involved:

1. **Mount the raw callable.** Works in anything that can mount an ASGI or
   WSGI app. Nothing from this package is needed.
2. **A native adapter** from `contrib/`. The routes are registered in the
   host's own router, so its authentication, middleware, error handling --
   and, for FastAPI, its OpenAPI schema -- apply to them.
3. **Hand-wire it.** `api.routes.build_routes()` returns the route table as
   data; register the handlers yourself.

### Mount-safety

- **Never trust `scope["path"]` alone.** Modern Starlette's `Mount` does not
  rewrite `path`; it extends `root_path`. Older versions stripped `path`
  instead. A server given `--root-path` never puts the prefix in `path` at
  all. One rule covers all three: strip `root_path` from `path` **only when
  it is genuinely a prefix**, otherwise use `path` unchanged. That is what
  `api.http.split_path` does.
- **WSGI is unambiguous.** The server or dispatcher has already split
  `SCRIPT_NAME` from `PATH_INFO`. Route on `PATH_INFO`, build links from
  `SCRIPT_NAME`.
- **Links are built from the discovered prefix**, plus `X-Forwarded-Prefix`
  when a proxy stripped one, never from a hard-coded `/api/v1`.
- **Never mutate the host's `scope` or `environ`.** Copy first.

Pecan is the awkward one: it does not rewrite `PATH_INFO`, so the mount point
is recovered by taking the remainder off the end.

### Lifecycle, without lifespan

**A mounted ASGI app never receives the lifespan scope.** The host router
handles it at the root and does not forward it to a mount. So startup cannot
hang off lifespan alone, and `Meter` owns its own lifecycle instead:

1. **Explicit, and preferred:** `meter.start()` / `meter.stop()`, or use it as
   a context manager. A host with a lifespan of its own should call these
   from it, or from `AppConfig.ready()`.
2. **From lifespan**, when our ASGI app *is* the root app.
3. **Lazily**, on the first request, with an `atexit` stop.

All three are idempotent and reference-counted, so starting from a host
lifespan *and* a first request is harmless.

### Being a good citizen

- No module-level singletons. Every app is a factory over a `Meter`, so two
  meters can coexist in one process.
- No CORS headers, no global exception handlers, no logging configuration, no
  signal handlers. Those are the host's business.
- `HEAD` and `OPTIONS` answered correctly.
- An unknown path inside our own prefix returns a JSON 404 rather than
  raising into the host.

### Sync core, async edge

Handlers are plain synchronous functions returning a `MeterResponse`, so WSGI
and synchronous Django call them directly. The ASGI adapter offloads the
blocking datasource calls with `asyncio.to_thread` -- standard library, no
`anyio` dependency -- and feeds streams from an async generator.

WebSockets are declined with a clean close rather than implemented: SSE
already carries the stream, and it works over WSGI too.

**The WSGI streaming caveat**, which deployments need to know up front: a
synchronous WSGI worker holds one thread for as long as an SSE connection
stays open. Use gevent or eventlet workers for streaming, or let clients poll
`/events?since_seq=` instead.

## 5. Multiple workers

N API workers each constructing a polling `Meter` means N pollers on the same
database. Two supported shapes:

- `Meter(poll=True)` for single-process or embedded use.
- `Meter(poll=False)` for API workers reading a store that a separate
  `taskflow-meter collect` process keeps filled.

With `poll=False` over a taskflow backend there is no event history, so the
event and stream endpoints answer 501 and flow payloads omit their links --
rather than serving an empty stream that cannot be told apart from silence.

## 6. Testing

The suite is built on the principle that a monitoring library which is only
tested against mocks has tested its own assumptions. So it runs real engines
(serial and parallel), a real sqlite logbook read from a second process, real
Pecan, Flask, Django and FastAPI applications, a real HTTP server, a real
kombu broker and a real oslo.messaging bus, and real alembic migrations
checked against the models.

| Tree | Subject |
| --- | --- |
| `tests/unit/<pkg>/test_<mod>.py` | One module, mirroring the package |
| `tests/functional/` | A behaviour across modules, including the cross-process one |
| `tests/integration/` | Real engines, and the collector deployment end to end |
| `tests/conformance/` | The ASGI and WSGI callables, and all six hosts, comparing bytes |

A unit test module must mirror its target or `tox -e pep8` fails on the
test-tree check. Anything that does not target a single module belongs in one
of the sibling trees.

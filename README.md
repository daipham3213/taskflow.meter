# taskflow-meter

Monitoring interfaces — ASGI, WSGI, datasources, transports — for observing
[OpenStack TaskFlow](https://opendev.org/openstack/taskflow) flow execution
progress.

```bash
pip install taskflow-meter
```

## What it is for

TaskFlow knows exactly where a flow is — which atoms have run, which are
running, how far along each task reports itself — but it offers no way to
*look* at that from outside the process running the flow. `taskflow-meter`
provides that view:

- **Read-only over your existing persistence backend.** Task progress is
  written through to the persistence layer on every `update_progress()` call,
  so an existing deployment can be monitored with *no changes to flow code*.
- **Embeddable anywhere.** Ships a hand-rolled ASGI callable and a hand-rolled
  WSGI callable with no web framework dependency, mountable at any sub-path
  inside FastAPI, Starlette, Flask, Django, or served standalone.
- **Live, in-process option.** Attach a listener plus a per-atom progress tap
  to an engine for sub-second latency and full DAG topology.

## Using it

Point a meter at a taskflow persistence backend and serve it:

```python
from taskflow_meter.api.asgi import ASGIApp
from taskflow_meter.datasource.persistence import PersistenceDataSource
from taskflow_meter.meter import Meter

source = PersistenceDataSource(conf={"connection": "sqlite:///taskflow.db"})
meter = Meter(source)  # polls the backend, keeps an event stream
app = ASGIApp(meter)  # a plain ASGI 3 callable
```

`app` runs under any ASGI server (`uvicorn module:app`), or mounts inside an
application you already have:

```python
host.mount("/taskflow", ASGIApp(meter))  # Starlette / FastAPI
```

There is a WSGI callable too, with identical behaviour -- a conformance suite
compares the two byte for byte:

```python
from taskflow_meter.api.wsgi import WSGIApp

application = WSGIApp(meter)  # gunicorn module:application

app.wsgi_app = DispatcherMiddleware(  # Flask / werkzeug
    app.wsgi_app, {"/taskflow": WSGIApp(meter)}
)
```

Or serve it straight away, with no server dependency at all:

```bash
taskflow-meter serve --connection sqlite:///taskflow.db
```

That runs on `wsgiref` from the standard library and binds localhost. It is a
development server -- put the WSGI callable behind gunicorn for anything real.

For a fleet, run the collector: the flows publish to a broker, one process
writes to the meter's own database, and the API workers read it without
polling anything.

```bash
taskflow-meter upgrade --store-url postgresql://host/meter
taskflow-meter collect --url amqp://broker// --store-url postgresql://host/meter
taskflow-meter serve   --store-url postgresql://host/meter
```

Inside OpenStack, `--transport oslo-messaging` puts the events on the
notification bus the service is already configured for, instead of opening a
broker connection of its own.
One caveat for WSGI deployments: a synchronous worker holds a thread for as
long as an SSE stream stays open, so use gevent or eventlet workers for
streaming, or let clients poll `/events?since_seq=` instead.

Mounted apps never receive the ASGI lifespan scope, so the meter also starts
itself on the first request. If your host application has a lifespan of its
own, prefer `meter.start()` / `meter.stop()` from it.

### Inside a service you already run

If your service composes its WSGI stack from `api-paste.ini`, dispatch a
prefix to the meter and configure it in the service's own config file:

```ini
[composite:main]
use = egg:Paste#urlmap
/: your_api
/taskflow-meter: taskflow_meter

[app:taskflow_meter]
paste.app_factory = taskflow_meter.contrib.paste:app_factory
```

```ini
[taskflow_meter]
connection = mysql+pymysql://user:password@host/taskflow
```

Native adapters exist for the frameworks where the routes should live inside
the host application rather than beside it, so its auth and middleware apply
to them:

| Host | Adapter |
| --- | --- |
| FastAPI | `contrib.fastapi.meter_router(meter)` |
| Flask | `contrib.flask.meter_blueprint(meter)` |
| Django | `contrib.django.meter_urlpatterns(meter)` |
| Pecan | `contrib.pecan.MeterController()` |
| paste | `contrib.paste:app_factory` |

See [the guide](docs/guide.md) for all of them, and for how to read completion
and current-task out of the API.

### Watching from inside the process running the flow

If you control the code that runs the flow, attaching to the engine gets you
what reading persistence cannot: progress readable in single-digit
milliseconds instead of a poll interval, and the flow's graph, which taskflow
never persists.

```python
from taskflow_meter.collect import attach

with attach(engine) as watched:
    engine.run()
    meter = Meter(watched.store, poll=False)
```

Delivery happens on its own thread behind a bounded queue, so a task never
waits on a publisher, and no failure downstream -- a broken webhook, a full
queue, a publisher that raises -- can fail a task.

### Endpoints

| Path | What it returns |
| --- | --- |
| `GET /healthz` | Liveness, version, and poller counters |
| `GET /api/v1/flows` | Flows, newest first, filterable and paged |
| `GET /api/v1/flows/{run_id}` | One flow with its atoms |
| `GET /api/v1/flows/{run_id}/atoms` | Just the atoms |
| `GET /api/v1/flows/{run_id}/events` | Event history from `?since_seq=` |
| `GET /api/v1/flows/{run_id}/stream` | The same events as SSE, live |

The stream honours `Last-Event-ID`, so a dropped connection resumes where it
left off rather than leaving a hole. If the datasource keeps no history, the
two event endpoints answer 501 and flow payloads omit their links, rather than
serving an empty stream that cannot be told apart from silence.

## Requirements

- Python 3.10+
- taskflow 4.2.0+
- oslo.config 6.9.0+

Floors are deliberately low: this package is meant to be co-installed into
a service whose dependency versions it does not get to choose. They are the
oldest release of each library the suite actually passes against, not the
oldest that looks plausible -- a `lowest-direct` CI job installs exactly
these and runs the whole suite on them.

Everything else is optional, and only needed by the feature that imports it:

| Extra | Pulls in | Needed for |
| --- | --- | --- |
| `sqlalchemy` | SQLAlchemy 1.4+, alembic 1.2+ | The collector's own store |
| `amqp` | kombu 5.1+ | Publishing events to a broker |
| `oslo-messaging` | oslo.messaging 6.0+ | Publishing onto the service's own notification bus |
| `all` | all three | |

The contrib adapters declare no dependency on their hosts -- a deployment
mounting the meter in Django already has Django. They are tested against
Django 3.2, Flask 2.3.3, FastAPI 0.100, Pecan 1.4 and PasteDeploy 2.0.

## Documentation

| | |
| --- | --- |
| [`docs/guide.md`](docs/guide.md) | Deploying it: configuration, every host, and how to read completion and current-task out of the API |
| [`docs/design.md`](docs/design.md) | How it works and why -- what taskflow does and does not record, and the rules the embedding code obeys |
| [`docs/releasing.md`](docs/releasing.md) | Cutting a release |
| [`CHANGELOG.md`](CHANGELOG.md) | What changed |

## Examples

Runnable, and covered by the test suite so they cannot rot:
[`examples/`](examples/).

## Development

This project builds with [Hatch](https://hatch.pypa.io) (`hatchling` +
`hatch-vcs`, so the version comes from git tags) and is developed with
[uv](https://docs.astral.sh/uv/).

```bash
uv run --group dev pytest          # tests
uv run --group dev ruff check .    # lint
uv run --group dev ruff format .   # format
uv run --group dev mypy            # type check
uv build                           # build sdist + wheel
```

`tox` is available too, and is what CI reproduces:

```bash
uvx tox -e pep8          # ruff, hacking, mypy, and the test-tree check
uvx tox -e py312         # tests on one interpreter
uvx tox                  # the whole matrix
```

CI also runs the suite with every declared dependency floor installed exactly,
which is the only job that checks those floors are real. To reproduce it:

```bash
uv lock --python 3.10 --resolution lowest-direct
uv sync --python 3.10 --group dev --all-extras --resolution lowest-direct
uv run --frozen --no-sync pytest
```

Because the version is derived from git history, a shallow clone or a checkout
with no tags will build as `0.0.0`. CI checks out with full history.

### Where tests go

A unit test module mirrors the module it targets, so finding the tests for a
file is mechanical rather than a search:

| Module | Its tests |
| --- | --- |
| `taskflow_meter/diff.py` | `tests/unit/test_diff.py` |
| `taskflow_meter/datasource/memory.py` | `tests/unit/datasource/test_memory.py` |

Tests that do not target a single module go in a sibling tree instead --
`tests/functional/`, `tests/integration/` or `tests/conformance/`. Anything
misplaced fails `tox -e pep8`.

## License

Apache-2.0. See [`LICENSE`](LICENSE).

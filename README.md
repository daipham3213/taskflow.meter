# taskflow-meter

Monitoring interfaces — ASGI, WSGI, datasources, transports — for observing
[OpenStack TaskFlow](https://opendev.org/openstack/taskflow) flow execution
progress.

> **Status: pre-alpha.** The packaging, tooling and CI skeleton are in place
> (milestone M0). The design is specified in [`docs/PLAN.md`](docs/PLAN.md);
> functionality lands milestone by milestone against it.

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

## Requirements

- Python 3.11+
- taskflow 6.4.0+

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

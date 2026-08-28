"""Guards on the packaging plumbing set up in M0."""

from __future__ import annotations

import subprocess
import sys

import pytest

import taskflow_meter
from taskflow_meter import cli


def test_version_is_a_non_empty_string() -> None:
    assert isinstance(taskflow_meter.__version__, str)
    assert taskflow_meter.__version__


def test_taskflow_is_importable_at_the_required_floor() -> None:
    # The whole package is built against notifier/persistence APIs that are
    # only stable from taskflow 6.x onwards.
    from taskflow import version as taskflow_version

    major = int(taskflow_version.version_string().split(".")[0])
    assert major >= 6


def test_cli_version_exits_zero() -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--version"])
    assert excinfo.value.code == 0


def test_cli_entry_point_runs_as_a_module() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "taskflow_meter.cli", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "taskflow-meter" in result.stdout


def test_declared_datasource_plugins_all_resolve() -> None:
    # An entry point pointing at a missing module breaks discovery for
    # every other plugin in the same group, so load them all.
    from importlib.metadata import entry_points

    from taskflow_meter.datasource.base import DataSource

    found = entry_points(group="taskflow_meter.datasource")
    assert {ep.name for ep in found} == {"memory"}
    for entry_point in found:
        assert issubclass(entry_point.load(), DataSource)

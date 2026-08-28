"""Command line entry point."""

from __future__ import annotations

import subprocess
import sys

import pytest

from taskflow_meter import cli


def test_version_exits_zero() -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--version"])
    assert excinfo.value.code == 0


def test_no_arguments_prints_help_and_succeeds(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.main([]) == 0
    assert "taskflow-meter" in capsys.readouterr().out


def test_unknown_argument_is_rejected() -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--nope"])
    assert excinfo.value.code != 0


def test_runs_as_a_module() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "taskflow_meter.cli", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "taskflow-meter" in result.stdout

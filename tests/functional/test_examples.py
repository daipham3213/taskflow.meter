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

"""The examples have to keep working.

Documentation that does not run is worse than none: it is confidently
wrong.  These run the scripts as a reader would.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent.parent / "examples"
TIMEOUT = 120.0


def test_the_examples_directory_is_documented() -> None:
    scripts = {path.name for path in EXAMPLES.glob("*.py")}
    listed = (EXAMPLES / "README.md").read_text()
    for name in scripts:
        assert name in listed, f"{name} is not mentioned in the README"


def test_attaching_in_process_runs() -> None:
    result = subprocess.run(
        [sys.executable, str(EXAMPLES / "attach_in_process.py")],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    # It should have shown the graph, some progress and a finished flow.
    assert "graph:" in result.stdout
    assert "100%" in result.stdout
    assert "SUCCESS" in result.stdout
    assert "completion: 100%" in result.stdout


@pytest.mark.parametrize(
    "script", sorted(path.name for path in EXAMPLES.glob("*.py"))
)
def test_every_example_at_least_imports(script: str) -> None:
    # The serving example blocks, so it cannot simply be run; importing
    # it still catches the ways an example rots.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import runpy, sys;"
            f"sys.argv=['{script}'];"
            f"runpy.run_path('{EXAMPLES / script}', run_name='__notmain__')",
        ],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        check=False,
    )
    assert result.returncode == 0, result.stderr

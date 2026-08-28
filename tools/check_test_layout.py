#!/usr/bin/env python3
"""Check that unit test modules mirror the package they test.

``taskflow_meter/<pkg>/<mod>.py`` is tested by
``tests/unit/<pkg>/test_<mod>.py``, so that finding the tests for a module
is mechanical rather than a search.  Tests that do not target a single
module -- packaging checks, engine integration, mount conformance -- live
in the sibling trees instead, which this check leaves alone.

Run directly, or via ``tox -e pep8``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PACKAGE = "taskflow_meter"
TESTS = "tests"
UNIT = "unit"

#: Test trees that are not required to mirror anything.
FREE_TREES = frozenset({"functional", "integration", "conformance"})


def check(root: Path) -> list[str]:
    """Return a message for every misplaced test module."""
    problems: list[str] = []
    tests_root = root / TESTS
    if not tests_root.is_dir():
        return [f"{TESTS}/ does not exist"]

    for path in sorted(tests_root.rglob("test_*.py")):
        relative = path.relative_to(tests_root)
        tree = relative.parts[0]

        if len(relative.parts) == 1:
            problems.append(
                f"{path.relative_to(root)}: test modules do not belong at "
                f"the top of {TESTS}/; move it to {TESTS}/{UNIT}/ (mirroring "
                f"its target) or into one of: "
                f"{', '.join(sorted(FREE_TREES))}"
            )
            continue

        if tree in FREE_TREES:
            continue

        if tree != UNIT:
            problems.append(
                f"{path.relative_to(root)}: unknown test tree {tree!r}; "
                f"expected {UNIT}/ or one of: "
                f"{', '.join(sorted(FREE_TREES))}"
            )
            continue

        within_unit = relative.relative_to(UNIT)
        target = (
            root
            / PACKAGE
            / within_unit.parent
            / within_unit.name.removeprefix("test_")
        )
        if not target.is_file():
            expected = target.relative_to(root)
            problems.append(
                f"{path.relative_to(root)}: mirrors nothing -- expected "
                f"{expected} to exist"
            )

    return problems


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    problems = check(root)
    for problem in problems:
        print(f"ERROR: {problem}", file=sys.stderr)
    if problems:
        print(
            f"\n{len(problems)} misplaced test module(s).  A unit test for "
            f"{PACKAGE}/<pkg>/<mod>.py belongs at "
            f"{TESTS}/{UNIT}/<pkg>/test_<mod>.py.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

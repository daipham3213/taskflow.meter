"""The repo's own dev tooling.

Lives outside the mirrored unit tree because its target is under
``tools/`` rather than in the package.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent


def load_checker() -> ModuleType:
    path = ROOT / "tools" / "check_test_layout.py"
    spec = importlib.util.spec_from_file_location("check_test_layout", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def checker() -> ModuleType:
    return load_checker()


def build_tree(root: Path, *test_files: str) -> None:
    for package_file in ("taskflow_meter/models.py", "taskflow_meter/x/y.py"):
        target = root / package_file
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()
    for test_file in test_files:
        target = root / test_file
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()
    (root / "tests").mkdir(exist_ok=True)


def test_this_repository_passes(checker: ModuleType) -> None:
    assert checker.check(ROOT) == []


def test_a_mirrored_module_is_accepted(
    checker: ModuleType, tmp_path: Path
) -> None:
    build_tree(
        tmp_path,
        "tests/unit/test_models.py",
        "tests/unit/x/test_y.py",
    )
    assert checker.check(tmp_path) == []


def test_a_test_mirroring_nothing_is_rejected(
    checker: ModuleType, tmp_path: Path
) -> None:
    build_tree(tmp_path, "tests/unit/test_imaginary.py")
    (problem,) = checker.check(tmp_path)
    assert "mirrors nothing" in problem
    assert "taskflow_meter/imaginary.py" in problem


def test_a_test_in_the_wrong_subpackage_is_rejected(
    checker: ModuleType, tmp_path: Path
) -> None:
    # y.py lives in taskflow_meter/x/, so its test may not sit at the top
    # of the unit tree.
    build_tree(tmp_path, "tests/unit/test_y.py")
    (problem,) = checker.check(tmp_path)
    assert "mirrors nothing" in problem


def test_a_test_at_the_top_of_the_tests_tree_is_rejected(
    checker: ModuleType, tmp_path: Path
) -> None:
    build_tree(tmp_path, "tests/test_models.py")
    (problem,) = checker.check(tmp_path)
    assert "do not belong at the top" in problem


def test_free_trees_are_left_alone(
    checker: ModuleType, tmp_path: Path
) -> None:
    build_tree(
        tmp_path,
        "tests/functional/test_packaging.py",
        "tests/integration/test_a_real_engine.py",
        "tests/conformance/test_mounting.py",
    )
    assert checker.check(tmp_path) == []


def test_an_unrecognised_tree_is_rejected(
    checker: ModuleType, tmp_path: Path
) -> None:
    build_tree(tmp_path, "tests/scratch/test_models.py")
    (problem,) = checker.check(tmp_path)
    assert "unknown test tree" in problem


def test_a_missing_tests_directory_is_reported(
    checker: ModuleType, tmp_path: Path
) -> None:
    assert checker.check(tmp_path) == ["tests/ does not exist"]

"""Command line entry point for ``taskflow-meter``.

Only ``--version`` is wired up so far; ``serve``, ``collect``, ``tail`` and
``dump`` arrive with the API and collector milestones.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from taskflow_meter import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taskflow-meter",
        description=("Monitor OpenStack TaskFlow flow execution progress."),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    parser.parse_args(argv)
    parser.print_help()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

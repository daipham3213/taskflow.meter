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

"""Monitoring interfaces for OpenStack TaskFlow flow execution progress.

See ``docs/PLAN.md`` for the design this package is being built out against.
"""

from __future__ import annotations

__all__ = ["__version__"]


def _detect_version() -> str:
    """Resolve the distribution version.

    ``_version.py`` is generated at build time by hatch-vcs and is therefore
    absent from a source checkout that has never been built.  Fall back to the
    installed distribution metadata, then to a sentinel, so importing the
    package never fails just because of version plumbing.
    """
    try:
        from taskflow_meter._version import __version__ as version
    except ImportError:
        pass
    else:
        return str(version)

    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _version

    try:
        return _version("taskflow-meter")
    except PackageNotFoundError:
        return "0.0.0.dev0"


__version__: str = _detect_version()

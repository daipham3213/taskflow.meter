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

"""Guards on the packaging plumbing, against the installed distribution.

These exercise the built artefact rather than a module, so they live
outside the mirrored unit tree.
"""

from __future__ import annotations

import taskflow_meter


def test_version_is_a_non_empty_string() -> None:
    assert isinstance(taskflow_meter.__version__, str)
    assert taskflow_meter.__version__


def test_taskflow_is_importable_at_the_required_floor() -> None:
    # The whole package is built against notifier/persistence APIs that are
    # only stable from taskflow 6.x onwards.
    from taskflow import version as taskflow_version

    major = int(taskflow_version.version_string().split(".")[0])
    assert major >= 6


def test_declared_datasource_plugins_all_resolve() -> None:
    # An entry point pointing at a missing module breaks discovery for
    # every other plugin in the same group, so load them all.
    from importlib.metadata import entry_points

    from taskflow_meter.datasource.base import DataSource

    found = entry_points(group="taskflow_meter.datasource")
    assert {ep.name for ep in found} == {"memory", "persistence"}
    for entry_point in found:
        assert issubclass(entry_point.load(), DataSource)

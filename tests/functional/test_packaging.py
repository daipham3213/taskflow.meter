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


def test_every_taskflow_api_the_package_uses_exists() -> None:
    """What the declared taskflow floor is actually claiming.

    The floor is not a guess at a major version -- it is the oldest
    release carrying all of this.  Pinning the *names* rather than the
    number is what stops the floor being lowered past something the
    package silently needs.
    """
    from taskflow import exceptions as tf_exc
    from taskflow import states as tf_states
    from taskflow import task as tf_task
    from taskflow.engines.action_engine import compiler as tf_compiler
    from taskflow.listeners import base as tf_listeners
    from taskflow.persistence import backends as tf_backends
    from taskflow.persistence import models as tf_models

    assert hasattr(tf_compiler, "ATOMS")
    assert hasattr(tf_task, "EVENT_UPDATE_PROGRESS")
    assert hasattr(tf_models, "atom_detail_type")
    assert hasattr(tf_exc, "NotFound")
    assert hasattr(tf_listeners, "Listener")
    assert hasattr(tf_backends, "fetch")
    for state in (
        "SUCCESS",
        "FAILURE",
        "PENDING",
        "RUNNING",
        "REVERTING",
        "REVERTED",
        "REVERT_FAILURE",
        "RETRYING",
        "IGNORE",
        "SUSPENDED",
        "EXECUTE",
        "REVERT",
    ):
        assert hasattr(tf_states, state), state


def test_the_declared_floors_are_the_ones_that_were_tested() -> None:
    """Guards the floors against being edited without being re-tested.

    CI runs a ``lowest-direct`` job that installs exactly these, so a
    floor changed here and not verified there fails somewhere obvious
    rather than in somebody else's deployment.
    """
    from importlib.metadata import requires

    declared = {}
    for line in requires("taskflow-meter") or []:
        name, _, rest = line.partition(">=")
        # Names come back normalised, and every extra repeats a floor
        # the extra it belongs to has already stated.
        declared[name.strip()] = rest.partition(";")[0].strip()

    assert declared == {
        "taskflow": "4.2.0",
        "oslo-config": "6.9.0",
        "sqlalchemy": "1.4.0",
        "alembic": "1.2.0",
        "kombu": "5.1.0",
    }


def test_declared_datasource_plugins_all_resolve() -> None:
    # An entry point pointing at a missing module breaks discovery for
    # every other plugin in the same group, so load them all.
    from importlib.metadata import entry_points

    from taskflow_meter.datasource.base import DataSource

    found = entry_points(group="taskflow_meter.datasource")
    assert {ep.name for ep in found} == {
        "memory",
        "persistence",
        "sqlalchemy",
    }
    for entry_point in found:
        assert issubclass(entry_point.load(), DataSource)


def test_the_paste_factory_is_discoverable() -> None:
    # So api-paste.ini can say `use = egg:taskflow-meter#meter`.
    from importlib.metadata import entry_points

    (found,) = entry_points(group="paste.app_factory", name="meter")
    assert callable(found.load())


def test_the_config_generator_hook_is_discoverable() -> None:
    from importlib.metadata import entry_points

    (found,) = entry_points(group="oslo.config.opts", name="taskflow_meter")
    ((group, options),) = found.load()()
    assert group.name == "taskflow_meter"
    assert options


def test_declared_transports_all_resolve() -> None:
    from importlib.metadata import entry_points

    from taskflow_meter.transports.base import Publisher

    found = entry_points(group="taskflow_meter.transport")
    assert {ep.name for ep in found} == {
        "memory",
        "datasource",
        "http",
        "amqp",
    }
    for entry_point in found:
        assert issubclass(entry_point.load(), Publisher)


def test_the_migrations_are_installed_beside_the_code() -> None:
    """They are package data, not source-tree files.

    A wheel without them installs fine and then fails at
    ``taskflow-meter upgrade``, which is the worst place to find out.
    """
    from taskflow_meter.datasource.sqlalchemy.source import MIGRATIONS

    assert (MIGRATIONS / "env.py").is_file()
    assert (MIGRATIONS / "script.py.mako").is_file()
    versions = sorted((MIGRATIONS / "versions").glob("*.py"))
    assert versions, "no migration scripts were installed"


def test_the_package_declares_it_is_typed() -> None:
    from importlib.resources import files

    assert files("taskflow_meter").joinpath("py.typed").is_file()

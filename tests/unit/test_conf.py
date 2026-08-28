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

"""Configuration, and building a meter from it alone."""

from __future__ import annotations

from typing import Any

import pytest
from oslo_config import cfg

from taskflow_meter import conf as meter_conf
from taskflow_meter.datasource.memory import MemoryDataSource


def build_conf(*, host_database: str | None = None) -> cfg.ConfigOpts:
    """A fresh ConfigOpts, optionally with a host service's [database]."""
    conf = cfg.ConfigOpts()
    if host_database is not None:
        conf.register_opts([cfg.StrOpt("connection")], group="database")
    meter_conf.register_opts(conf)
    conf([], project="test", default_config_files=[])
    if host_database is not None:
        conf.set_override("connection", host_database, group="database")
    return conf


@pytest.fixture
def conf() -> cfg.ConfigOpts:
    return build_conf()


# -- registration --------------------------------------------------------


def test_registration_is_repeatable(conf: cfg.ConfigOpts) -> None:
    meter_conf.register_opts(conf)
    meter_conf.register_opts(conf)
    assert conf.taskflow_meter.poll is True


def test_two_configs_do_not_share_values() -> None:
    """oslo.config keeps option state on the OptGroup object.

    Registering one module-level group into two ConfigOpts makes them
    share overrides -- invisible while there is only the global CONF,
    and immediate once there is a second one.
    """
    first, second = build_conf(), build_conf()
    first.set_override(
        "connection", "sqlite:///first.db", group="taskflow_meter"
    )
    assert second.taskflow_meter.connection is None


def test_the_defaults_are_the_documented_ones(
    conf: cfg.ConfigOpts,
) -> None:
    settings = conf.taskflow_meter
    assert settings.poll is True
    assert settings.poll_interval == 2.0
    assert settings.max_events_per_run == 1000


def test_the_connection_is_marked_secret() -> None:
    # It usually carries a database password.
    (option,) = [opt for opt in meter_conf.OPTS if opt.name == "connection"]
    assert option.secret is True


def test_the_generator_gets_the_group_with_its_help() -> None:
    ((group, options),) = meter_conf.list_opts()
    assert isinstance(group, cfg.OptGroup)
    assert group.name == "taskflow_meter"
    assert options is meter_conf.OPTS


# -- resolving the connection --------------------------------------------


def test_an_explicit_connection_wins(conf: cfg.ConfigOpts) -> None:
    conf.set_override(
        "connection", "sqlite:///explicit.db", group="taskflow_meter"
    )
    assert meter_conf.resolve_connection(conf) == "sqlite:///explicit.db"


def test_the_host_services_database_is_the_fallback() -> None:
    # The zero-extra-config case: the service's config already has it.
    conf = build_conf(host_database="mysql+pymysql://host/service")
    assert (
        meter_conf.resolve_connection(conf) == "mysql+pymysql://host/service"
    )


def test_an_explicit_connection_beats_the_host_service() -> None:
    # Taskflow logbooks do not have to live in the service's database.
    conf = build_conf(host_database="mysql+pymysql://host/service")
    conf.set_override(
        "connection", "mysql+pymysql://host/taskflow", group="taskflow_meter"
    )
    assert (
        meter_conf.resolve_connection(conf) == "mysql+pymysql://host/taskflow"
    )


def test_no_connection_anywhere_says_where_to_put_one(
    conf: cfg.ConfigOpts,
) -> None:
    with pytest.raises(cfg.RequiredOptError, match="connection"):
        meter_conf.resolve_connection(conf)


def test_a_blank_host_database_is_not_a_connection() -> None:
    conf = build_conf(host_database="")
    with pytest.raises(cfg.RequiredOptError):
        meter_conf.resolve_connection(conf)


# -- building the meter --------------------------------------------------


@pytest.fixture
def sqlite_conf(tmp_path: Any) -> cfg.ConfigOpts:
    conf = build_conf()
    conf.set_override(
        "connection", f"sqlite:///{tmp_path}/tf.db", group="taskflow_meter"
    )
    return conf


def test_a_meter_is_built_from_configuration_alone(
    sqlite_conf: cfg.ConfigOpts,
) -> None:
    meter = meter_conf.meter_from_config(sqlite_conf)
    assert meter.poller is not None
    assert isinstance(meter.store, MemoryDataSource)
    assert meter.supports_events is True


def test_the_retention_setting_reaches_the_store(
    sqlite_conf: cfg.ConfigOpts,
) -> None:
    sqlite_conf.set_override("max_events_per_run", 5, group="taskflow_meter")
    meter = meter_conf.meter_from_config(sqlite_conf)
    assert isinstance(meter.store, MemoryDataSource)
    assert meter.store.max_events_per_run == 5


def test_the_poll_interval_reaches_the_poller(
    sqlite_conf: cfg.ConfigOpts,
) -> None:
    sqlite_conf.set_override("poll_interval", 7.5, group="taskflow_meter")
    meter = meter_conf.meter_from_config(sqlite_conf)
    assert meter.poller is not None
    assert meter.poller.interval == 7.5


def test_turning_polling_off_gives_a_read_through_meter(
    sqlite_conf: cfg.ConfigOpts,
) -> None:
    # What an API worker runs when a collector process owns the polling.
    sqlite_conf.set_override("poll", False, group="taskflow_meter")
    meter = meter_conf.meter_from_config(sqlite_conf)
    assert meter.poller is None
    assert meter.store is None
    assert meter.supports_events is False


def test_a_wsgi_app_can_be_built_the_same_way(
    sqlite_conf: cfg.ConfigOpts,
) -> None:
    app = meter_conf.wsgi_app_from_config(sqlite_conf)
    assert app.meter.poller is not None

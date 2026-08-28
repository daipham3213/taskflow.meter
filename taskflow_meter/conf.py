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

"""Configuration, read from the host service's own oslo.config.

The point is that an operator adds the meter to their service's
pipeline and it works, without writing a second config file.  If
``[taskflow_meter] connection`` is unset we fall back to ``[database]
connection`` -- the option those services already set -- so the common
case needs no configuration at all.

That fallback is a convenience, not a guarantee: a deployment whose
taskflow logbooks live somewhere other than the service's main database
has to say so.  It is spelled out in the option help rather than left
for someone to discover from an empty flow list.
"""

from __future__ import annotations

from typing import Any

from oslo_config import cfg

from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.datasource.persistence import PersistenceDataSource
from taskflow_meter.meter import Meter
from taskflow_meter.poller import DEFAULT_INTERVAL

GROUP_NAME = "taskflow_meter"

OPT_GROUP = cfg.OptGroup(
    name=GROUP_NAME,
    title="TaskFlow monitoring",
    help="Read-only monitoring of taskflow flow execution progress.",
)

OPTS = [
    cfg.StrOpt(
        "connection",
        secret=True,
        help=(
            "Connection string for the taskflow persistence backend, in "
            "the form the flows themselves use. Defaults to [database] "
            "connection, which is right when taskflow shares this "
            "service's database and wrong when its logbooks live "
            "elsewhere -- set this explicitly if they do."
        ),
    ),
    cfg.BoolOpt(
        "poll",
        default=True,
        help=(
            "Poll the backend and keep an event history. Turn this off "
            "in API workers when a separate collector process keeps the "
            "store warm, so N workers do not become N pollers on the "
            "same database. With it off, the event and stream endpoints "
            "report 501."
        ),
    ),
    cfg.FloatOpt(
        "poll_interval",
        default=DEFAULT_INTERVAL,
        min=0.05,
        help=(
            "Seconds between polls. This is also the resolution of what "
            "can be observed: a state a flow entered and left within one "
            "interval was never visible, and no amount of polling "
            "afterwards recovers it."
        ),
    ),
    cfg.IntOpt(
        "max_events_per_run",
        default=1000,
        min=1,
        help=(
            "Events retained per flow before the oldest are dropped. A "
            "client that falls behind further than this is told its "
            "history has a hole rather than being handed a stream that "
            "silently skips events."
        ),
    ),
]


def register_opts(conf: cfg.ConfigOpts | None = None) -> cfg.ConfigOpts:
    """Register our options on ``conf``.  Safe to call repeatedly.

    Registered by group *name*, not by passing :data:`OPT_GROUP`.
    oslo.config keeps each option's state -- including overrides -- on
    the OptGroup object, so handing the same one to two ConfigOpts makes
    them share values.  Nothing notices while there is only the global
    CONF, and everything notices the moment there is a second one.
    """
    target = cfg.CONF if conf is None else conf
    target.register_opts(OPTS, group=GROUP_NAME)
    return target


def list_opts() -> list[tuple[cfg.OptGroup, list[cfg.Opt]]]:
    """Entry point for ``oslo-config-generator``.

    Returns the group object so the generated sample carries its title
    and help; registration deliberately does not.
    """
    return [(OPT_GROUP, OPTS)]


def resolve_connection(conf: cfg.ConfigOpts) -> str:
    """Find the taskflow connection, or explain what is missing."""
    configured = conf[GROUP_NAME].connection
    if configured:
        return str(configured)

    inherited = _host_database_connection(conf)
    if inherited:
        return inherited

    msg = (
        "no taskflow persistence connection configured: set "
        f"[{GROUP_NAME}] connection, or run inside a service that sets "
        "[database] connection"
    )
    raise cfg.RequiredOptError("connection", GROUP_NAME) from ValueError(msg)


def _host_database_connection(conf: cfg.ConfigOpts) -> str | None:
    """Read ``[database] connection`` if the host service has one.

    Read rather than imported: oslo.db is not our dependency, and the
    services that set this option have already registered it.
    """
    try:
        return str(conf.database.connection) or None
    except (cfg.NoSuchOptError, cfg.NoSuchGroupError, AttributeError):
        return None


def meter_from_config(conf: cfg.ConfigOpts | None = None) -> Meter:
    """Build a meter from configuration alone."""
    target = register_opts(conf)
    settings = target[GROUP_NAME]
    source = PersistenceDataSource(
        conf={"connection": resolve_connection(target)}
    )
    store = None
    if settings.poll:
        from taskflow_meter.datasource.memory import MemoryDataSource

        store = MemoryDataSource(
            max_events_per_run=settings.max_events_per_run
        )
    return Meter(
        source,
        store=store,
        poll=settings.poll,
        interval=settings.poll_interval,
    )


def wsgi_app_from_config(
    conf: cfg.ConfigOpts | None = None, **overrides: Any
) -> WSGIApp:
    """Build the WSGI callable a paste pipeline or Pecan tree can host."""
    return WSGIApp(meter_from_config(conf), **overrides)

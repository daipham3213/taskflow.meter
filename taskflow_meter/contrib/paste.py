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

"""Host the meter in a paste pipeline.

A service that composes its WSGI stack from ``api-paste.ini`` wants a
``paste.app_factory``.  Dispatch a prefix to the app and it is served
alongside the API it is monitoring, on the same port, behind the same
middleware::

    [composite:main]
    use = egg:Paste#urlmap
    /: your_api
    /taskflow-meter: taskflow_meter

    [app:taskflow_meter]
    paste.app_factory = taskflow_meter.contrib.paste:app_factory

Configuration comes from the ``[taskflow_meter]`` group in the
service's own oslo.config file, so there is no second config file to
deploy.  Anything in the paste stanza
overrides it, for the deployments that would rather keep it all in
``api-paste.ini``::

    [app:taskflow_meter]
    paste.app_factory = taskflow_meter.contrib.paste:app_factory
    connection = mysql+pymysql://user:pass@host/taskflow
    poll_interval = 5

urlmap gives the app a ``SCRIPT_NAME`` of the prefix it was mounted at,
which is exactly what the WSGI callable builds its links from, so the
mount point needs no configuring.
"""

from __future__ import annotations

from typing import Any

from oslo_config import cfg

from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.conf import GROUP_NAME
from taskflow_meter.conf import meter_from_config
from taskflow_meter.conf import register_opts

#: Settings a paste stanza may override, and how to read them.
_OVERRIDES: dict[str, Any] = {
    "connection": str,
    "poll": lambda value: str(value).lower() in {"1", "true", "yes", "on"},
    "poll_interval": float,
    "max_events_per_run": int,
}


def app_factory(
    global_config: dict[str, Any] | None = None,  # noqa: ARG001
    **local_conf: str,
) -> WSGIApp:
    """Build the WSGI app.  Signature fixed by paste.

    ``global_config`` is paste's ``[DEFAULT]`` section, which we do not
    read: the service's oslo.config is the source of truth, and the
    paste stanza is the override.
    """
    conf = register_opts()
    apply_overrides(conf, local_conf)
    return WSGIApp(meter_from_config(conf))


def apply_overrides(conf: cfg.ConfigOpts, local_conf: dict[str, str]) -> None:
    """Fold ``api-paste.ini`` values into the config.

    Paste hands everything over as a string, so each one is coerced by
    the type its option expects.  An unusable value is rejected here,
    naming the setting, rather than surfacing later as an odd failure
    from somewhere else.
    """
    for name, coerce in _OVERRIDES.items():
        if name not in local_conf:
            continue
        raw = local_conf[name]
        try:
            value = coerce(raw)
        except (TypeError, ValueError) as exc:
            msg = f"{name}={raw!r} in the paste stanza is not usable"
            raise ValueError(msg) from exc
        conf.set_override(name, value, group=GROUP_NAME)

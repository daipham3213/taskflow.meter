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

"""Build the same meter into every host framework we support.

Used by the conformance suite, which asserts they all answer
identically.  Each factory returns a callable the matching client can
drive, mounted at ``prefix``.
"""

from __future__ import annotations

import sys
import types
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from taskflow_meter.api.asgi import ASGIApp
from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.meter import Meter

#: Bumped per Django host so each prefix gets its own URL conf module.
_urlconf_counter = 0


def bare_asgi(meter: Meter, prefix: str) -> Any:
    """The raw ASGI callable; the prefix arrives as ``root_path``."""
    return ASGIApp(meter, stream_interval=0.01, heartbeat=0.05)


def bare_wsgi(meter: Meter, prefix: str) -> Any:
    """The raw WSGI callable; the prefix arrives as ``SCRIPT_NAME``."""
    return WSGIApp(meter, stream_interval=0.01, heartbeat=0.05)


def fastapi_host(meter: Meter, prefix: str) -> Any:
    """Routes registered in a FastAPI app's own router."""
    from fastapi import FastAPI

    from taskflow_meter.contrib.fastapi import meter_router

    app = FastAPI()
    app.include_router(
        meter_router(meter, stream_interval=0.01, heartbeat=0.05),
        prefix=prefix,
    )
    return app


def flask_host(meter: Meter, prefix: str) -> Any:
    """Routes registered in a Flask blueprint."""
    from flask import Flask

    from taskflow_meter.contrib.flask import meter_blueprint

    app = Flask(__name__)
    app.register_blueprint(
        meter_blueprint(meter, stream_interval=0.01, heartbeat=0.05),
        url_prefix=prefix or None,
    )
    return app.wsgi_app


def pecan_host(meter: Meter, prefix: str) -> Any:
    """A Pecan controller tree with the meter mounted inside it."""
    from pecan import make_app

    from taskflow_meter.contrib.pecan import MeterController

    controller = MeterController(
        WSGIApp(meter, stream_interval=0.01, heartbeat=0.05)
    )

    class RootController:
        # Populated below: what the tree looks like depends on where
        # the meter is mounted.
        _default: Any = None
        index: Any = None

    if prefix:
        # Pecan routes by attribute name, one segment at a time.
        segments = [part for part in prefix.split("/") if part]
        node: Any = controller
        for segment in reversed(segments[1:]):
            parent = type("Node", (), {})()
            setattr(parent, segment, node)
            node = parent
        setattr(RootController, segments[0], node)
    else:
        # Mounted at the root: the controller's own methods become the
        # root controller's.
        RootController._default = controller._default
        RootController.index = controller.index

    return make_app(RootController())


def django_host(meter: Meter, prefix: str) -> Any:
    """A Django project with the meter in its URL conf."""
    global _urlconf_counter

    configure_django()

    from django.core.handlers.wsgi import WSGIHandler
    from django.urls import include
    from django.urls import path

    from taskflow_meter.contrib.django import meter_urlpatterns

    _urlconf_counter += 1
    name = f"taskflow_meter_test_urlconf_{_urlconf_counter}"
    module = types.ModuleType(name)
    patterns = meter_urlpatterns(meter, stream_interval=0.01, heartbeat=0.05)
    route = f"{prefix.lstrip('/')}/" if prefix else ""
    urlpatterns = [path(route, include(patterns))]
    module.urlpatterns = urlpatterns  # type: ignore[attr-defined]
    sys.modules[name] = module

    from django.test import override_settings

    handler = WSGIHandler()

    def app(environ: dict[str, Any], start_response: Any) -> Any:
        with override_settings(ROOT_URLCONF=name):
            return handler(environ, start_response)

    return app


def configure_django() -> None:
    """Configure Django once per process, whoever asks first."""
    import django
    from django.conf import settings

    if settings.configured:
        return
    settings.configure(
        DEBUG=False,
        ALLOWED_HOSTS=["*"],
        SECRET_KEY="taskflow-meter-tests",
        DATABASES={},
        USE_TZ=True,
        ROOT_URLCONF=None,
        # Nothing here needs middleware, and the defaults would add
        # headers that make the responses harder to compare.
        MIDDLEWARE=[],
        LOGGING_CONFIG=None,
    )
    django.setup()


@dataclass(frozen=True)
class Host:
    """How to build one host, and how to talk to it.

    ``mount_in_path`` records the difference that matters: the bare
    callables are told their prefix out of band (``root_path`` or
    ``SCRIPT_NAME``), while a host framework routing our templates sees
    the prefix as part of the path.
    """

    name: str
    build: Callable[[Meter, str], Any]
    asgi: bool = False
    mount_in_path: bool = True


HOSTS: tuple[Host, ...] = (
    Host("bare-asgi", bare_asgi, asgi=True, mount_in_path=False),
    Host("bare-wsgi", bare_wsgi, mount_in_path=False),
    Host("fastapi", fastapi_host, asgi=True),
    Host("flask", flask_host),
    Host("pecan", pecan_host),
    Host("django", django_host),
)


def call(host: Host, app: Any, path: str, prefix: str, **kwargs: Any) -> Any:
    """Make one request to ``app``, however that host wants it."""
    from tests import asgi_client
    from tests import wsgi_client

    if host.asgi:
        target = f"{prefix}{path}" if prefix else path
        if not host.mount_in_path:
            kwargs["root_path"] = prefix
        return asgi_client.request(app, target, **kwargs)

    if host.mount_in_path:
        return wsgi_client.request(app, f"{prefix}{path}", **kwargs)
    return wsgi_client.request(app, path, script_name=prefix, **kwargs)

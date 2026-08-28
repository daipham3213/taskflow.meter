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

"""Host the meter inside a Pecan controller tree.

For services built on Pecan, when the meter should live in the
application's own routing rather than beside it in a paste pipeline::

    from taskflow_meter.contrib.pecan import MeterController


    class RootController:
        v1 = V1Controller()
        taskflow_meter = MeterController()

Everything under that path is handed to the WSGI callable.  Pecan is
built on WebOb, so the request already carries the environ the callable
wants; the only work is moving the part of the path Pecan has already
consumed out of ``PATH_INFO`` and into ``SCRIPT_NAME``, which is what
makes the generated links point back at the right place.

Mounting a sub-application means the host's own hooks and middleware do
not run for these requests.  For deployments where they must, wire the
route table into the host's router instead -- ``api.routes.build_routes``
is that table, and it is data.
"""

from __future__ import annotations

from typing import Any

import pecan
from webob import Request

from taskflow_meter.api.http import mount_prefix
from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.conf import wsgi_app_from_config


class MeterController:
    """A Pecan controller that delegates to the WSGI callable."""

    def __init__(self, app: WSGIApp | None = None) -> None:
        """Wrap ``app``, or build one from the service's config."""
        self.app = app if app is not None else wsgi_app_from_config()

    @pecan.expose()
    def index(self) -> Any:
        """The controller's own path, with nothing after it."""
        return self._delegate(())

    @pecan.expose()
    def _default(self, *remainder: str) -> Any:
        """Everything below it."""
        return self._delegate(remainder)

    def _delegate(self, remainder: tuple[str, ...]) -> Any:
        request = pecan.request
        environ = dict(request.environ)
        consumed, sub_path = _split(request.path_info, remainder)
        environ["SCRIPT_NAME"] = request.script_name + consumed
        environ["PATH_INFO"] = sub_path
        # Pecan uses a returned WebOb response as-is, so the callable's
        # status, headers and body reach the client untouched.
        return Request(environ).get_response(self.app)


def _split(path_info: str, remainder: tuple[str, ...]) -> tuple[str, str]:
    """Split the path into what Pecan consumed and what is left for us.

    Pecan does not rewrite ``PATH_INFO`` as it routes, so the mount
    point has to be recovered by taking the remainder off the end.
    """
    sub_path = "/" + "/".join(remainder) if remainder else "/"
    return mount_prefix(path_info, sub_path), sub_path

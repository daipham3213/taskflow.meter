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

"""A path-template router small enough to read in one sitting.

Exists so the route table is data rather than decorators: the same table
drives our own ASGI and WSGI callables and can be walked by an adapter
that registers the routes in a host framework's router instead.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from collections.abc import Iterable
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any

from taskflow_meter.api.http import MeterRequest

#: A handler takes the request and returns a response, or a stream.
Handler = Callable[[MeterRequest], Any]

_PARAM = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


@dataclass(frozen=True, slots=True)
class Route:
    """One method and path template, bound to a handler."""

    method: str
    template: str
    handler: Handler
    name: str

    def compile(self) -> re.Pattern[str]:
        """Build the matcher for this template.

        The literal spans are escaped and the parameters are not, which
        is why this walks the template instead of substituting into an
        already-escaped string -- ``re.escape`` escapes the braces too,
        leaving nothing for a substitution to find.
        """
        parts: list[str] = []
        cursor = 0
        for found in _PARAM.finditer(self.template):
            parts.append(re.escape(self.template[cursor : found.start()]))
            parts.append(f"(?P<{found.group(1)}>[^/]+)")
            cursor = found.end()
        parts.append(re.escape(self.template[cursor:]))
        return re.compile(f"^{''.join(parts)}$")


class Outcome(Enum):
    """Why a match did or did not happen."""

    MATCHED = "matched"
    NOT_FOUND = "not_found"
    METHOD_NOT_ALLOWED = "method_not_allowed"


@dataclass(frozen=True, slots=True)
class Match:
    outcome: Outcome
    route: Route | None = None
    params: dict[str, str] = field(default_factory=dict)
    allowed: tuple[str, ...] = ()


class Router:
    """Matches a method and path against a fixed table of routes."""

    def __init__(self, routes: Iterable[Route]) -> None:
        self.routes = tuple(routes)
        self._compiled = tuple(
            (route, route.compile()) for route in self.routes
        )
        self._by_name = {route.name: route for route in self.routes}

    def match(self, method: str, path: str) -> Match:
        allowed: list[str] = []
        for route, pattern in self._compiled:
            found = pattern.match(path)
            if found is None:
                continue
            if route.method == method:
                return Match(
                    Outcome.MATCHED, route=route, params=found.groupdict()
                )
            allowed.append(route.method)

        if allowed:
            # The path exists, the verb does not.  Saying so beats a 404
            # that sends a client hunting for a typo in the URL.
            return Match(
                Outcome.METHOD_NOT_ALLOWED,
                allowed=tuple(sorted(set(allowed))),
            )
        return Match(Outcome.NOT_FOUND)

    def template_for(self, name: str) -> str:
        return self._by_name[name].template

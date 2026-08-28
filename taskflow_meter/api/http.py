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

"""Requests and responses, with no web framework in sight.

Handlers take a :class:`MeterRequest` and return a :class:`MeterResponse`,
so the same handler serves an ASGI mount, a WSGI mount, or a host
framework's own router.  Neither adapter is the source of truth.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import urlencode

JSON_CONTENT_TYPE = "application/json"


class ApiError(Exception):
    """An error with an HTTP shape.

    Raised by handlers and rendered by the adapters.  Deliberately not a
    global exception handler: we catch our own and let anything else
    reach the host application, which knows what to do with it.
    """

    status = 500
    title = "Internal Server Error"

    def __init__(
        self,
        detail: str,
        *,
        headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.headers = headers


class BadRequestError(ApiError):
    status = 400
    title = "Bad Request"


class NotFoundError(ApiError):
    status = 404
    title = "Not Found"


class MethodNotAllowedError(ApiError):
    status = 405
    title = "Method Not Allowed"


class UnsupportedError(ApiError):
    """The datasource in use cannot answer this at all."""

    status = 501
    title = "Not Implemented"


def split_path(path: str, root_path: str) -> str:
    """Return the path to route on, given a mount prefix.

    Mirrors what Starlette's own ``get_route_path`` does, and for the
    same reason: since 0.33 ``Mount`` extends ``root_path`` and leaves
    ``scope["path"]`` whole, while older versions stripped ``path``
    instead, and a server given ``--root-path`` may report a prefix that
    never appears in ``path`` at all.  Stripping only when the prefix is
    genuinely there covers all three.
    """
    if not root_path or not path.startswith(root_path):
        return path
    return path[len(root_path) :] or "/"


def mount_prefix(full_path: str, sub_path: str) -> str:
    """Return the part of ``full_path`` that precedes ``sub_path``.

    How an adapter recovers where it was mounted when the host routed by
    consuming path segments rather than by rewriting the path: the
    difference between the two is the prefix.  Every generated link is
    built from it, so guessing would produce links to nowhere -- hence
    the empty string when the two do not line up.
    """
    if sub_path in ("", "/"):
        return full_path.rstrip("/")
    if not full_path.endswith(sub_path):
        return ""
    return full_path[: -len(sub_path)]


@dataclass(frozen=True, slots=True)
class MeterRequest:
    """One inbound request, reduced to what a handler needs."""

    method: str = "GET"
    path: str = "/"
    #: Where this app is mounted.  Every generated link starts here.
    prefix: str = ""
    query: dict[str, list[str]] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    path_params: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_query_string(
        cls, query_string: str, **kwargs: Any
    ) -> MeterRequest:
        return cls(
            query=parse_qs(query_string, keep_blank_values=True), **kwargs
        )

    def param(self, name: str) -> str:
        try:
            return self.path_params[name]
        except KeyError:  # pragma: no cover - a routing bug, not input
            msg = f"no such path parameter: {name}"
            raise LookupError(msg) from None

    def get(self, name: str, default: str | None = None) -> str | None:
        values = self.query.get(name)
        return values[0] if values else default

    def get_int(self, name: str, default: int) -> int:
        raw = self.get(name)
        if raw is None or raw == "":
            return default
        try:
            return int(raw)
        except ValueError:
            msg = f"{name} must be an integer, got {raw!r}"
            raise BadRequestError(msg) from None

    def url(self, path: str, **query: Any) -> str:
        """Build a link back into this app, under its mount prefix."""
        full = f"{self.prefix}{path}"
        pairs = [(k, v) for k, v in query.items() if v is not None]
        return f"{full}?{urlencode(pairs)}" if pairs else full


@dataclass(frozen=True, slots=True)
class MeterResponse:
    """A complete, already-rendered response."""

    status: int = 200
    body: bytes = b""
    headers: tuple[tuple[str, str], ...] = ()

    @classmethod
    def json(
        cls,
        payload: Any,
        *,
        status: int = 200,
        headers: tuple[tuple[str, str], ...] = (),
    ) -> MeterResponse:
        # Separators are pinned so two adapters serving the same handler
        # produce byte-identical output, which is what the conformance
        # suite compares.
        body = json.dumps(payload, separators=(",", ":")).encode()
        return cls(
            status=status,
            body=body,
            headers=(
                ("content-type", JSON_CONTENT_TYPE),
                ("content-length", str(len(body))),
                *headers,
            ),
        )

    @classmethod
    def from_error(cls, error: ApiError) -> MeterResponse:
        return cls.json(
            {
                "error": {
                    "status": error.status,
                    "title": error.title,
                    "detail": error.detail,
                }
            },
            status=error.status,
            headers=error.headers,
        )

    def header(self, name: str) -> str | None:
        wanted = name.lower()
        for key, value in self.headers:
            if key.lower() == wanted:
                return value
        return None


def normalise_headers(raw: Mapping[str, str]) -> dict[str, str]:
    """Lowercase header names, so lookups do not depend on the server."""
    return {key.lower(): value for key, value in raw.items()}

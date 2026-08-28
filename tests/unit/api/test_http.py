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

"""Requests, responses, and the mount-prefix rule."""

from __future__ import annotations

import json

import pytest

from taskflow_meter.api.http import ApiError
from taskflow_meter.api.http import BadRequestError
from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import MeterResponse
from taskflow_meter.api.http import NotFoundError
from taskflow_meter.api.http import normalise_headers
from taskflow_meter.api.http import split_path


@pytest.mark.parametrize(
    ("path", "root_path", "expected"),
    [
        # Not mounted at all.
        ("/api/v1/flows", "", "/api/v1/flows"),
        # Modern Starlette: root_path extended, path left whole.
        ("/meter/api/v1/flows", "/meter", "/api/v1/flows"),
        # Pre-0.33 Starlette: path already stripped by the Mount.
        ("/api/v1/flows", "/meter", "/api/v1/flows"),
        # A server given --root-path that never appears in the path.
        ("/api/v1/flows", "/behind-a-proxy", "/api/v1/flows"),
        # Mounted deeply.
        ("/a/b/c/healthz", "/a/b/c", "/healthz"),
        # The mount root itself must not become empty.
        ("/meter", "/meter", "/"),
        # A prefix that only looks like one.
        ("/metering/flows", "/meter", "ing/flows"),
    ],
)
def test_the_route_path_survives_every_mount_convention(
    path: str, root_path: str, expected: str
) -> None:
    assert split_path(path, root_path) == expected


def test_query_parsing() -> None:
    request = MeterRequest.from_query_string("limit=5&state=RUNNING&e=")
    assert request.get("limit") == "5"
    assert request.get("state") == "RUNNING"
    assert request.get("missing") is None
    assert request.get("missing", "fallback") == "fallback"
    assert request.get("e") == ""


def test_integer_parameters() -> None:
    request = MeterRequest.from_query_string("limit=5&blank=")
    assert request.get_int("limit", 1) == 5
    assert request.get_int("absent", 7) == 7
    # An empty value is an omitted value, not a zero.
    assert request.get_int("blank", 7) == 7


def test_a_non_numeric_integer_is_a_bad_request() -> None:
    request = MeterRequest.from_query_string("limit=lots")
    with pytest.raises(BadRequestError, match="must be an integer"):
        request.get_int("limit", 1)


def test_links_start_at_the_mount_prefix() -> None:
    request = MeterRequest(prefix="/meter")
    assert request.url("/api/v1/flows") == "/meter/api/v1/flows"
    assert (
        request.url("/api/v1/flows", marker="run-1")
        == "/meter/api/v1/flows?marker=run-1"
    )


def test_links_drop_empty_query_values() -> None:
    request = MeterRequest()
    assert request.url("/x", marker=None) == "/x"
    assert request.url("/x", since_seq=0) == "/x?since_seq=0"


def test_path_parameters() -> None:
    request = MeterRequest(path_params={"run_id": "abc"})
    assert request.param("run_id") == "abc"
    with pytest.raises(LookupError):
        request.param("nope")


def test_json_responses_carry_their_own_headers() -> None:
    response = MeterResponse.json({"a": 1})
    assert response.status == 200
    assert response.header("content-type") == "application/json"
    assert response.header("content-length") == str(len(response.body))
    assert json.loads(response.body) == {"a": 1}


def test_json_output_is_byte_stable() -> None:
    # Two adapters serving the same handler have to agree exactly, which
    # is what the conformance suite compares.
    first = MeterResponse.json({"b": 1, "a": [1, 2]}).body
    second = MeterResponse.json({"b": 1, "a": [1, 2]}).body
    assert first == second == b'{"b":1,"a":[1,2]}'


def test_errors_render_as_json_with_their_status() -> None:
    response = MeterResponse.from_error(NotFoundError("no such flow"))
    assert response.status == 404
    assert json.loads(response.body) == {
        "error": {
            "status": 404,
            "title": "Not Found",
            "detail": "no such flow",
        }
    }


def test_errors_can_carry_headers() -> None:
    error = ApiError("nope", headers=(("allow", "GET"),))
    assert MeterResponse.from_error(error).header("allow") == "GET"


def test_header_lookup_ignores_case() -> None:
    assert normalise_headers({"Content-Type": "x"}) == {"content-type": "x"}
    assert MeterResponse.json({}).header("CONTENT-TYPE") is not None
    assert MeterResponse.json({}).header("x-absent") is None

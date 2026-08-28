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

"""Route matching."""

from __future__ import annotations

import pytest

from taskflow_meter.api.http import MeterRequest
from taskflow_meter.api.http import MeterResponse
from taskflow_meter.api.router import Outcome
from taskflow_meter.api.router import Route
from taskflow_meter.api.router import Router


def handler(request: MeterRequest) -> MeterResponse:
    return MeterResponse.json({})


@pytest.fixture
def router() -> Router:
    return Router(
        [
            Route("GET", "/healthz", handler, name="health"),
            Route("GET", "/api/v1/flows", handler, name="flows"),
            Route("GET", "/api/v1/flows/{run_id}", handler, name="flow"),
            Route(
                "GET", "/api/v1/flows/{run_id}/atoms", handler, name="atoms"
            ),
            Route("POST", "/api/v1/flows/{run_id}", handler, name="post"),
        ]
    )


def test_a_static_route_matches(router: Router) -> None:
    match = router.match("GET", "/healthz")
    assert match.outcome is Outcome.MATCHED
    assert match.route is not None
    assert match.route.name == "health"
    assert match.params == {}


def test_a_parameter_is_captured(router: Router) -> None:
    match = router.match("GET", "/api/v1/flows/abc-123")
    assert match.params == {"run_id": "abc-123"}


def test_a_parameter_stops_at_the_next_slash(router: Router) -> None:
    # Otherwise /flows/{run_id} would swallow /flows/{run_id}/atoms.
    match = router.match("GET", "/api/v1/flows/abc/atoms")
    assert match.route is not None
    assert match.route.name == "atoms"


def test_a_parameter_does_not_match_across_segments(
    router: Router,
) -> None:
    assert router.match("GET", "/api/v1/flows/a/b/c").outcome is (
        Outcome.NOT_FOUND
    )


@pytest.mark.parametrize(
    "run_id", ["abc-123", "UUID-with-CAPS", "a.b~c", "%20encoded", "1"]
)
def test_parameters_accept_anything_but_a_slash(
    router: Router, run_id: str
) -> None:
    match = router.match("GET", f"/api/v1/flows/{run_id}")
    assert match.params == {"run_id": run_id}


def test_an_unknown_path_is_not_found(router: Router) -> None:
    assert router.match("GET", "/nope").outcome is Outcome.NOT_FOUND


def test_a_known_path_with_the_wrong_verb_says_so(router: Router) -> None:
    # A 404 here would send a client hunting for a typo in the URL.
    match = router.match("DELETE", "/api/v1/flows/abc")
    assert match.outcome is Outcome.METHOD_NOT_ALLOWED
    assert match.allowed == ("GET", "POST")


def test_the_allowed_verbs_are_deduplicated_and_sorted() -> None:
    router = Router(
        [
            Route("POST", "/x", handler, name="a"),
            Route("GET", "/x", handler, name="b"),
            Route("POST", "/x", handler, name="c"),
        ]
    )
    assert router.match("PUT", "/x").allowed == ("GET", "POST")


def test_a_template_with_regex_characters_is_matched_literally() -> None:
    # The literal spans are escaped; only the parameters are not.
    router = Router([Route("GET", "/a.b/{id}", handler, name="dotted")])
    assert router.match("GET", "/a.b/1").outcome is Outcome.MATCHED
    assert router.match("GET", "/axb/1").outcome is Outcome.NOT_FOUND


def test_templates_are_available_by_name(router: Router) -> None:
    assert router.template_for("flow") == "/api/v1/flows/{run_id}"

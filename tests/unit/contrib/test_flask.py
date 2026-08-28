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

"""Routes registered in a Flask application's own blueprint."""

from __future__ import annotations

from typing import Any

import pytest
from flask import Flask

from taskflow_meter import states
from taskflow_meter.contrib.flask import meter_blueprint
from taskflow_meter.contrib.flask import to_flask_rule
from taskflow_meter.meter import Meter
from tests.conftest import make_atom
from tests.conftest import make_flow
from tests.unit.test_poller import FakeSource

MOUNT = "/taskflow"


@pytest.fixture
def meter() -> Meter:
    source = FakeSource(
        make_flow(
            state=states.RUNNING,
            atoms=(make_atom("a", state=states.RUNNING, progress=0.5),),
        )
    )
    built = Meter(source)
    built.poll_once()
    return built


@pytest.fixture
def client(meter: Meter) -> Any:
    app = Flask(__name__)
    app.register_blueprint(meter_blueprint(meter), url_prefix=MOUNT)
    return app.test_client()


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        ("/healthz", "/healthz"),
        ("/api/v1/flows/{run_id}", "/api/v1/flows/<run_id>"),
        (
            "/api/v1/flows/{run_id}/atoms",
            "/api/v1/flows/<run_id>/atoms",
        ),
    ],
)
def test_our_templates_become_flask_rules(
    template: str, expected: str
) -> None:
    assert to_flask_rule(template) == expected


def test_the_endpoints_are_named_after_the_routes(meter: Meter) -> None:
    app = Flask(__name__)
    app.register_blueprint(
        meter_blueprint(meter, name="meter"), url_prefix=MOUNT
    )
    names = {rule.endpoint for rule in app.url_map.iter_rules()}
    assert {"meter.flows", "meter.flow", "meter.stream"} <= names


def test_a_flow_listing_is_served(client: Any) -> None:
    response = client.get(f"{MOUNT}/api/v1/flows")
    assert response.status_code == 200
    assert response.get_json()["flows"][0]["links"]["self"] == (
        f"{MOUNT}/api/v1/flows/run-1"
    )


def test_our_errors_are_still_ours(client: Any) -> None:
    response = client.get(f"{MOUNT}/api/v1/flows/nope")
    assert response.status_code == 404
    assert response.get_json()["error"]["title"] == "Not Found"


def test_the_hosts_hooks_apply(meter: Meter) -> None:
    """The reason to use a blueprint: the host's guards wrap our routes."""
    app = Flask(__name__)
    app.register_blueprint(meter_blueprint(meter), url_prefix=MOUNT)

    @app.before_request
    def forbid() -> tuple[str, int]:
        return "nope", 403

    assert app.test_client().get(f"{MOUNT}/healthz").status_code == 403


def test_the_meter_starts_on_the_first_request(meter: Meter) -> None:
    app = Flask(__name__)
    app.register_blueprint(meter_blueprint(meter), url_prefix=MOUNT)
    assert not meter.running
    try:
        assert app.test_client().get(f"{MOUNT}/healthz").status_code == 200
        assert meter.running
    finally:
        meter.stop()


def test_a_stream_is_served(meter: Meter, client: Any) -> None:
    source = meter.source
    assert isinstance(source, FakeSource)
    source.set(
        make_flow(
            state=states.SUCCESS,
            atoms=(make_atom("a", state=states.SUCCESS, progress=1.0),),
        )
    )
    meter.poll_once()

    response = client.get(f"{MOUNT}/api/v1/flows/run-1/stream")
    assert response.status_code == 200
    assert response.headers["Content-Type"].startswith("text/event-stream")
    assert b"event: end" in response.get_data()


def test_a_blueprint_at_the_root(meter: Meter) -> None:
    app = Flask(__name__)
    app.register_blueprint(meter_blueprint(meter))
    response = app.test_client().get("/api/v1/flows")
    assert response.get_json()["flows"][0]["links"]["self"] == (
        "/api/v1/flows/run-1"
    )


def test_a_script_name_is_included_in_links(meter: Meter) -> None:
    # A deployment behind a WSGI dispatcher has both a SCRIPT_NAME and
    # a blueprint prefix, and the links need both.
    app = Flask(__name__)
    app.register_blueprint(meter_blueprint(meter), url_prefix=MOUNT)
    response = app.test_client().get(
        f"{MOUNT}/api/v1/flows", environ_overrides={"SCRIPT_NAME": "/svc"}
    )
    assert response.get_json()["flows"][0]["links"]["self"] == (
        f"/svc{MOUNT}/api/v1/flows/run-1"
    )

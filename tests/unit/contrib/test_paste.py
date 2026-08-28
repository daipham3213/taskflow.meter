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

"""Hosting the meter in a paste pipeline."""

from __future__ import annotations

from typing import Any

import pytest
from oslo_config import cfg

from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.contrib import paste as contrib_paste
from tests import wsgi_client
from tests.unit.test_conf import build_conf


@pytest.fixture
def conf(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> cfg.ConfigOpts:
    """A global CONF the factory will find, as in a real service."""
    built = build_conf()
    built.set_override(
        "connection", f"sqlite:///{tmp_path}/tf.db", group="taskflow_meter"
    )
    monkeypatch.setattr(cfg, "CONF", built)
    monkeypatch.setattr(contrib_paste, "register_opts", lambda: built)
    return built


def test_the_factory_builds_an_app_from_the_services_config(
    conf: cfg.ConfigOpts,
) -> None:
    app = contrib_paste.app_factory({})
    assert isinstance(app, WSGIApp)
    assert app.meter.poller is not None


def test_the_factory_ignores_pastes_global_section(
    conf: cfg.ConfigOpts,
) -> None:
    # oslo.config is the source of truth; [DEFAULT] in api-paste.ini is
    # about the pipeline, not about us.
    app = contrib_paste.app_factory({"debug": "True"})
    assert isinstance(app, WSGIApp)


def test_the_paste_stanza_can_override_the_config(
    conf: cfg.ConfigOpts, tmp_path: Any
) -> None:
    contrib_paste.app_factory(
        {},
        connection=f"sqlite:///{tmp_path}/other.db",
        poll_interval="5.5",
        max_events_per_run="7",
    )
    settings = conf.taskflow_meter
    assert settings.connection.endswith("other.db")
    assert settings.poll_interval == 5.5
    assert settings.max_events_per_run == 7


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("false", False),
        ("0", False),
        ("no", False),
    ],
)
def test_booleans_survive_the_trip_through_ini(
    conf: cfg.ConfigOpts, value: str, expected: bool
) -> None:
    # Paste hands everything over as a string, so "false" would
    # otherwise be a true value.
    contrib_paste.app_factory({}, poll=value)
    assert conf.taskflow_meter.poll is expected


@pytest.mark.parametrize(
    ("setting", "value"),
    [("poll_interval", "soon"), ("max_events_per_run", "lots")],
)
def test_an_unusable_value_names_the_setting(
    conf: cfg.ConfigOpts, setting: str, value: str
) -> None:
    with pytest.raises(ValueError, match=setting):
        contrib_paste.app_factory({}, **{setting: value})


def test_settings_the_stanza_omits_are_left_alone(
    conf: cfg.ConfigOpts,
) -> None:
    contrib_paste.app_factory({}, poll_interval="3")
    assert conf.taskflow_meter.max_events_per_run == 1000


def test_urlmap_gives_the_app_its_mount_point(
    conf: cfg.ConfigOpts,
) -> None:
    """The links have to point back through the composite's prefix.

    urlmap sets SCRIPT_NAME to the prefix it dispatched on, which is
    exactly what the callable builds links from -- so mounting needs no
    configuration of its own.
    """
    app = contrib_paste.app_factory({})
    response = wsgi_client.request(
        app, "/api/v1/flows", script_name="/taskflow-meter"
    )
    assert response.status == 200
    assert response.json()["links"]["self"] == ("/taskflow-meter/api/v1/flows")

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

"""Command line entry point."""

from __future__ import annotations

import subprocess
import sys
from types import TracebackType
from typing import Any

import pytest

from taskflow_meter import cli


def test_version_exits_zero() -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--version"])
    assert excinfo.value.code == 0


def test_no_arguments_prints_help_and_succeeds(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.main([]) == 0
    assert "taskflow-meter" in capsys.readouterr().out


def test_unknown_argument_is_rejected() -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--nope"])
    assert excinfo.value.code != 0


def test_runs_as_a_module() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "taskflow_meter.cli", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "taskflow-meter" in result.stdout


class FakeServer:
    """Stands in for a real one, without ever binding a socket."""

    def __init__(self, *, interrupt: bool = False) -> None:
        self.server_address = ("127.0.0.1", 8080)
        self.served = 0
        self.interrupt = interrupt

    def __enter__(self) -> FakeServer:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def serve_forever(self) -> None:
        self.served += 1
        if self.interrupt:
            raise KeyboardInterrupt


@pytest.fixture
def sqlite_conf(tmp_path: object) -> str:
    return f"sqlite:///{tmp_path}/cli.db"


def test_serve_runs_the_server_and_stops_the_meter(
    monkeypatch: pytest.MonkeyPatch, sqlite_conf: str
) -> None:
    server = FakeServer()
    captured: dict[str, Any] = {}

    def fake_make_server(
        host: str, port: int, app: Any, **kwargs: Any
    ) -> FakeServer:
        captured.update(host=host, port=port, app=app, **kwargs)
        return server

    monkeypatch.setattr(cli, "make_server", fake_make_server)
    assert cli.main(["serve", "--connection", sqlite_conf]) == 0

    assert server.served == 1
    assert captured["host"] == cli.DEFAULT_HOST
    assert captured["port"] == cli.DEFAULT_PORT
    # One thread per connection: the single-threaded default would let
    # one open stream block every other request.
    assert captured["server_class"] is cli.ThreadingWSGIServer
    # The meter is stopped on the way out, not left polling.
    assert not captured["app"].meter.running


def test_serve_binds_localhost_by_default() -> None:
    # A monitoring API exposes flow names, atom names and failure
    # detail, none of which belongs on 0.0.0.0 by accident.
    assert cli.DEFAULT_HOST == "127.0.0.1"


def test_ctrl_c_is_a_clean_shutdown(
    monkeypatch: pytest.MonkeyPatch, sqlite_conf: str
) -> None:
    monkeypatch.setattr(
        cli, "make_server", lambda *a, **kw: FakeServer(interrupt=True)
    )
    assert cli.main(["serve", "--connection", sqlite_conf]) == 0


def test_serve_needs_somewhere_to_read_from(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # A usage error, not a traceback: there are two valid sources and
    # the message has to name both.
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["serve"])
    assert excinfo.value.code == 2
    assert "--connection or --store-url" in capsys.readouterr().err


def test_building_an_app_with_neither_source_is_rejected() -> None:
    with pytest.raises(ValueError, match="--connection or --store-url"):
        cli.build_app()


def test_no_poll_reads_the_backend_directly(sqlite_conf: str) -> None:
    app = cli.build_app(sqlite_conf, poll=False, interval=1.0)
    assert app.meter.poller is None
    assert app.meter.supports_events is False


def test_polling_gives_the_meter_an_event_history(
    sqlite_conf: str,
) -> None:
    app = cli.build_app(sqlite_conf, poll=True, interval=1.0)
    assert app.meter.poller is not None
    assert app.meter.supports_events is True


def test_collect_defaults_to_amqp() -> None:
    args = cli.build_parser().parse_args(
        ["collect", "--url", "amqp://host//", "--store-url", "sqlite://"]
    )
    assert args.transport == "amqp"
    assert args.amqp_url == "amqp://host//"


def test_collect_can_read_the_notification_bus() -> None:
    args = cli.build_parser().parse_args(
        [
            "collect",
            "--transport",
            "oslo-messaging",
            "--url",
            "rabbit://host//",
            "--store-url",
            "sqlite://",
        ]
    )
    assert args.transport == "oslo-messaging"


def test_the_1_0_spelling_of_the_url_still_works() -> None:
    """--amqp-url shipped in 1.0, so collectors already use it."""
    args = cli.build_parser().parse_args(
        ["collect", "--amqp-url", "amqp://host//", "--store-url", "sqlite://"]
    )
    assert args.amqp_url == "amqp://host//"


def test_the_two_spellings_of_the_url_are_exclusive(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            [
                "collect",
                "--url",
                "a",
                "--amqp-url",
                "b",
                "--store-url",
                "sqlite://",
            ]
        )
    assert "not allowed with" in capsys.readouterr().err


def test_collect_needs_a_url() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["collect", "--store-url", "sqlite://"])


@pytest.mark.parametrize("transport", sorted(cli.SUBSCRIBERS))
def test_every_offered_transport_can_be_built(transport: str) -> None:
    """A choice the parser offers but cannot construct is a trap."""
    url = {"amqp": "memory://", "oslo-messaging": "fake://"}[transport]
    subscriber = cli.build_subscriber(transport, url)
    assert subscriber.name


def test_a_transport_without_its_extra_says_which_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--transport offers the choice whether or not the extra is there."""

    import importlib

    def no_module(name: str) -> Any:
        raise ImportError(name)

    monkeypatch.setattr(importlib, "import_module", no_module)
    with pytest.raises(SystemExit, match=r"taskflow-meter\[oslo-messaging\]"):
        cli.build_subscriber("oslo-messaging", "rabbit://host//")


def test_the_store_without_its_extra_says_which_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins

    real_import = builtins.__import__

    def no_sqlalchemy(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("taskflow_meter.datasource.sqlalchemy"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_sqlalchemy)
    with pytest.raises(SystemExit, match=r"taskflow-meter\[sqlalchemy\]"):
        cli.build_store("sqlite://")

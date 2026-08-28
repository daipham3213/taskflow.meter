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

"""Command line entry point for ``taskflow-meter``.

``serve`` runs the WSGI callable on :mod:`wsgiref`, which is in the
standard library -- so a monitoring server costs no dependency at all.
It is a development server: fine for a laptop, a container sidecar, or
a look at a staging logbook, but a real deployment should put the same
callable behind gunicorn or mount it in a service it already runs.
"""

from __future__ import annotations

import argparse
import logging
import socketserver
from collections.abc import Sequence
from wsgiref.simple_server import WSGIRequestHandler
from wsgiref.simple_server import WSGIServer
from wsgiref.simple_server import make_server

from taskflow_meter import __version__
from taskflow_meter.api.wsgi import WSGIApp
from taskflow_meter.datasource.persistence import PersistenceDataSource
from taskflow_meter.meter import Meter
from taskflow_meter.poller import DEFAULT_INTERVAL

LOG = logging.getLogger(__name__)

#: Localhost by default: a monitoring API exposes flow names, atom
#: names and failure detail, none of which belongs on 0.0.0.0 by
#: accident.
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080


class ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    """One thread per connection.

    The single-threaded default would let one open SSE stream block
    every other request for as long as it stayed connected.
    """

    daemon_threads = True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taskflow-meter",
        description="Monitor OpenStack TaskFlow flow execution progress.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subcommands = parser.add_subparsers(dest="command")

    serve = subcommands.add_parser(
        "serve",
        help="serve the monitoring API over HTTP",
        description=(
            "Read a taskflow persistence backend and serve the "
            "monitoring API.  A development server; put the WSGI "
            "callable behind a real one for anything else."
        ),
    )
    serve.add_argument(
        "--connection",
        required=True,
        metavar="URL",
        help=(
            "taskflow persistence connection, as the flows themselves "
            "use it (for example sqlite:///taskflow.db)"
        ),
    )
    serve.add_argument("--host", default=DEFAULT_HOST)
    serve.add_argument("--port", type=int, default=DEFAULT_PORT)
    serve.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="seconds between polls of the backend",
    )
    serve.add_argument(
        "--no-poll",
        action="store_true",
        help=(
            "read the backend directly instead of polling it; no event "
            "history, and the stream endpoints report 501"
        ),
    )
    serve.set_defaults(handler=serve_command)
    return parser


def build_app(connection: str, *, poll: bool, interval: float) -> WSGIApp:
    source = PersistenceDataSource(conf={"connection": connection})
    meter = Meter(source, poll=poll, interval=interval)
    return WSGIApp(meter)


def serve_command(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    app = build_app(
        args.connection, poll=not args.no_poll, interval=args.interval
    )
    with (
        app.meter,
        make_server(
            args.host,
            args.port,
            app,
            server_class=ThreadingWSGIServer,
            handler_class=WSGIRequestHandler,
        ) as server,
    ):
        host, port = server.server_address[:2]
        LOG.info("serving on http://%s:%s", host, port)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            LOG.info("shutting down")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0
    result: int = handler(args)
    return result


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

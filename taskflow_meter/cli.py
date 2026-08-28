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

Three commands, which between them cover both deployments:

``serve``
    The API.  Runs on :mod:`wsgiref`, which is in the standard library,
    so a monitoring server costs no dependency at all.  It is a
    development server: fine for a laptop, a container sidecar or a
    look at a staging logbook, but a real deployment should put the
    same callable behind gunicorn or mount it in a service it runs.

``collect``
    The other half of a multi-process deployment: consume events from a
    broker and write them to a shared store, so the flows publish once
    and any number of API workers read without polling anything.

``upgrade``
    Bring that store's schema up to date.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import socketserver
from collections.abc import Sequence
from typing import Any
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
    serve.add_argument(
        "--store-url",
        metavar="URL",
        help=(
            "read the meter's own database instead, as filled by a "
            "collect process; implies no polling"
        ),
    )
    serve.set_defaults(handler=serve_command)

    collect = subcommands.add_parser(
        "collect",
        help="consume events from a broker into the meter's database",
        description=(
            "The collector half of a multi-process deployment. Flows "
            "publish events to a broker; this writes them to a store "
            "the API workers read."
        ),
    )
    collect.add_argument(
        "--transport",
        choices=sorted(SUBSCRIBERS),
        default="amqp",
        help="how to receive events (default: amqp)",
    )
    source = collect.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--url",
        dest="amqp_url",
        metavar="URL",
        help="broker to consume from, for example amqp://host//",
    )
    source.add_argument(
        # The 1.0 spelling.  It only ever meant a broker URL, which is
        # what --url means, so it stays as an alias rather than
        # breaking every collector already deployed.
        "--amqp-url",
        dest="amqp_url",
        metavar="URL",
        help=argparse.SUPPRESS,
    )
    collect.add_argument(
        "--store-url",
        required=True,
        metavar="URL",
        help="the meter's own database to write to",
    )
    collect.add_argument(
        "--create-schema",
        action="store_true",
        help="create the tables if missing, instead of migrating",
    )
    collect.add_argument(
        "--once",
        action="store_true",
        help="drain whatever is queued and exit, rather than looping",
    )
    collect.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        help="seconds to wait for a batch before looking again",
    )
    collect.set_defaults(handler=collect_command)

    upgrade = subcommands.add_parser(
        "upgrade",
        help="bring the meter's own database up to date",
    )
    upgrade.add_argument("--store-url", required=True, metavar="URL")
    upgrade.set_defaults(handler=upgrade_command)
    return parser


def build_app(
    connection: str | None = None,
    *,
    poll: bool = True,
    interval: float = DEFAULT_INTERVAL,
    store_url: str | None = None,
) -> WSGIApp:
    """Build the callable for whichever deployment was asked for.

    With ``store_url`` the meter reads a store a collector fills, and
    polls nothing.  Otherwise it watches a taskflow backend directly.
    """
    if store_url is not None:
        return WSGIApp(Meter(build_store(store_url), poll=False))
    if connection is None:
        msg = "pass --connection or --store-url"
        raise ValueError(msg)
    source = PersistenceDataSource(conf={"connection": connection})
    return WSGIApp(Meter(source, poll=poll, interval=interval))


#: The transports `collect` can read from, by their plugin name.  Kept
#: as names rather than classes so importing the CLI does not import
#: kombu and oslo.messaging, both of which are extras.
SUBSCRIBERS = {
    "amqp": ("taskflow_meter.transports.amqp", "AMQPSubscriber"),
    "oslo-messaging": (
        "taskflow_meter.transports.oslo_messaging",
        "OsloMessagingSubscriber",
    ),
}


#: Which extra installs each transport, for the error message below.
TRANSPORT_EXTRAS = {"amqp": "amqp", "oslo-messaging": "oslo-messaging"}


def build_subscriber(transport: str, url: str) -> Any:
    """Open the receiving end of whichever transport was asked for."""
    module_name, class_name = SUBSCRIBERS[transport]
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        # --transport offers the choice whether or not its extra is
        # installed, so say which one rather than surfacing the
        # traceback for a library the user never named.
        extra = TRANSPORT_EXTRAS[transport]
        msg = (
            f"the {transport} transport needs its extra: "
            f"pip install 'taskflow-meter[{extra}]'"
        )
        raise SystemExit(msg) from exc
    return getattr(module, class_name)(url)


def build_store(url: str, *, create_schema: bool = False) -> Any:
    """Open the meter's own database.

    Imported here rather than at module scope: SQLAlchemy is an extra,
    and `taskflow-meter serve` against a taskflow backend must not
    require it.
    """
    try:
        from taskflow_meter.datasource.sqlalchemy import SQLADataSource
    except ImportError as exc:
        msg = (
            "the meter's own database needs its extra: "
            "pip install 'taskflow-meter[sqlalchemy]'"
        )
        raise SystemExit(msg) from exc

    return SQLADataSource(url, create_schema=create_schema)


def serve_command(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    app = build_app(
        args.connection,
        poll=not args.no_poll,
        interval=args.interval,
        store_url=args.store_url,
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


def collect_command(args: argparse.Namespace) -> int:
    """Consume from the broker until interrupted."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    store = build_store(args.store_url, create_schema=args.create_schema)
    subscriber = build_subscriber(args.transport, args.amqp_url)
    total = 0

    with store, subscriber:
        LOG.info("collecting from %s over %s", args.amqp_url, args.transport)
        try:
            while True:
                received = subscriber.consume(
                    store.apply_many, timeout=args.timeout
                )
                total += received
                if received:
                    LOG.info("stored %d events (%d total)", received, total)
                if args.once:
                    break
        except KeyboardInterrupt:
            LOG.info("shutting down after %d events", total)
    return 0


def upgrade_command(args: argparse.Namespace) -> int:
    """Run the migrations against the meter's own database."""
    logging.basicConfig(level=logging.INFO)
    from taskflow_meter.datasource.sqlalchemy import upgrade

    upgrade(args.store_url)
    LOG.info("schema is up to date")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0
    if args.command == "serve" and not (args.connection or args.store_url):
        # Neither source given: a usage error, not a traceback.
        parser.error("serve needs --connection or --store-url")
    result: int = handler(args)
    return result


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

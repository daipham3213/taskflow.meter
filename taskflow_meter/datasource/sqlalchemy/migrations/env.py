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

"""Alembic environment for the meter's own schema.

Driven programmatically by :func:`taskflow_meter.datasource.sqlalchemy
.upgrade`, so the connection arrives through the config rather than from
an ``alembic.ini`` a deployment would have to carry.

Everything variable is read from ``config.attributes``, never from a
main option: alembic passes main options through configparser, which
treats ``%`` as an interpolation and rejects a percent-encoded password.
See :func:`~taskflow_meter.datasource.sqlalchemy.source.alembic_config`.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from alembic import context
from sqlalchemy import pool

from taskflow_meter.datasource.sqlalchemy.models import metadata

config = context.config
target_metadata = metadata

#: Default name of alembic's own bookkeeping table.  Overridable,
#: because this schema may share a database with a host service that has
#: an alembic tree of its own -- and two trees writing one
#: ``alembic_version`` row each mistake the other's revision for a
#: missing one.
DEFAULT_VERSION_TABLE = "alembic_version"


def _version_table() -> str:
    return config.attributes.get("version_table") or DEFAULT_VERSION_TABLE


def _url() -> str | None:
    """Where to connect, when no live connection was handed over.

    Falls back to the main option so that running these migrations from
    a hand-written ``alembic.ini`` still works.  Escaping ``%`` is the
    ini author's problem there, and ``%%`` is the documented convention
    for it.
    """
    url = config.attributes.get("url")
    if url is not None:
        return str(url)
    return config.get_main_option("sqlalchemy.url", None)


def run_migrations_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table=_version_table(),
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:
        # The usual path: somebody else owns the connection and the
        # transaction around it.
        _run(connection)
        return

    url = _url()
    if url is None:
        # Reached only by a config built by hand that set neither.
        # Saying so beats whatever SQLAlchemy makes of ``None``.
        msg = (
            "no connection and no url to migrate: build the config with "
            "taskflow_meter.datasource.sqlalchemy.alembic_config()"
        )
        raise RuntimeError(msg)

    engine = sa.create_engine(url, poolclass=pool.NullPool)
    try:
        with engine.connect() as owned:
            _run(owned)
    finally:
        engine.dispose()


def _run(connection: Any) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        version_table=_version_table(),
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

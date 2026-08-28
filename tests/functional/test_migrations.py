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

"""The migrations, exercised the way an operator would run them.

Outside the mirrored unit tree because the subject is alembic driving a
database, not one module.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from taskflow_meter.datasource.sqlalchemy.source import MIGRATIONS
from taskflow_meter.datasource.sqlalchemy.source import SQLADataSource
from taskflow_meter.datasource.sqlalchemy.source import upgrade
from taskflow_meter.events import Event
from taskflow_meter.events import EventKind


def config_for(url: str) -> Config:
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS))
    config.set_main_option("sqlalchemy.url", url)
    return config


def tables_of(url: str) -> set[str]:
    inspector = sa.inspect(sa.create_engine(url))
    return {
        name
        for name in inspector.get_table_names()
        if name.startswith("taskflow_meter_")
    }


@pytest.fixture
def url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'meter.db'}"


def test_upgrading_creates_the_tables(url: str) -> None:
    upgrade(url)
    assert tables_of(url) == {
        "taskflow_meter_flows",
        "taskflow_meter_events",
    }


def test_downgrading_removes_them_again(url: str) -> None:
    # A migration whose downgrade is wrong is worse than one with none:
    # it fails halfway through somebody's rollback.
    upgrade(url)
    command.downgrade(config_for(url), "base")
    assert tables_of(url) == set()


def test_the_cycle_can_be_repeated(url: str) -> None:
    upgrade(url)
    command.downgrade(config_for(url), "base")
    upgrade(url)

    store = SQLADataSource(url)
    store.apply(Event(run_id="run-1", seq=1, ts=1.0, kind=EventKind.HEARTBEAT))
    assert store.get_flow("run-1") is not None


def test_the_sql_can_be_generated_without_a_database(
    url: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--sql`` mode, for deployments that review DDL before running it."""
    command.upgrade(config_for(url), "head", sql=True)

    generated = capsys.readouterr().out
    assert "CREATE TABLE taskflow_meter_flows" in generated
    assert "CREATE TABLE taskflow_meter_events" in generated
    # Nothing was actually created.
    assert tables_of(url) == set()


def test_the_revision_is_recorded(url: str) -> None:
    upgrade(url)
    with sa.create_engine(url).connect() as conn:
        version = conn.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar()
    assert version == "0001"

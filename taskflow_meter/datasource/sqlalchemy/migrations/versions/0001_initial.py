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

"""The initial schema.

Revision ID: 0001
Revises:
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "taskflow_meter_flows",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("book_id", sa.String(64), index=True),
        sa.Column("book_name", sa.String(255)),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("state", sa.String(32), index=True),
        sa.Column("observed_at", sa.Float, nullable=False),
        sa.Column("meta", sa.JSON, nullable=False),
        sa.Column("atoms", sa.JSON, nullable=False),
    )
    op.create_index(
        "ix_taskflow_meter_flows_listing",
        "taskflow_meter_flows",
        ["observed_at", "run_id"],
    )
    op.create_table(
        "taskflow_meter_events",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("seq", sa.Integer, primary_key=True),
        sa.Column("ts", sa.Float, nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("book_id", sa.String(64)),
        sa.Column("atom_name", sa.String(255)),
        sa.Column("atom_uuid", sa.String(64)),
        sa.Column("atom_type", sa.String(16)),
        sa.Column("state", sa.String(32)),
        sa.Column("old_state", sa.String(32)),
        sa.Column("intention", sa.String(16)),
        sa.Column("progress", sa.Float),
        sa.Column("details", sa.JSON, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("taskflow_meter_events")
    op.drop_index(
        "ix_taskflow_meter_flows_listing",
        table_name="taskflow_meter_flows",
    )
    op.drop_table("taskflow_meter_flows")

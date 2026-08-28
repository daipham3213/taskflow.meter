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

"""The meter's own schema.

Two tables.  A flow's atoms live as JSON inside its row rather than in
a table of their own: every read wants the whole snapshot, and nothing
in the API filters or joins on an individual atom.  The cost of that
choice is that a query like "which flows have a failed atom" would need
a schema change -- worth knowing before someone needs one.
"""

from __future__ import annotations

import sqlalchemy as sa

metadata = sa.MetaData()

#: The current state of every run the collector has seen.
flows = sa.Table(
    "taskflow_meter_flows",
    metadata,
    sa.Column("run_id", sa.String(64), primary_key=True),
    sa.Column("book_id", sa.String(64), index=True),
    sa.Column("book_name", sa.String(255)),
    sa.Column("name", sa.String(255), nullable=False, default=""),
    sa.Column("state", sa.String(32), index=True),
    sa.Column("observed_at", sa.Float, nullable=False),
    sa.Column("meta", sa.JSON, nullable=False),
    sa.Column("atoms", sa.JSON, nullable=False),
    # Listing is always newest first, and paging breaks ties on run_id.
    sa.Index("ix_taskflow_meter_flows_listing", "observed_at", "run_id"),
)

#: Every event, keyed so that re-applying one is a no-op rather than a
#: duplicate -- a collector that reconnects and replays must not
#: double-count.
events = sa.Table(
    "taskflow_meter_events",
    metadata,
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

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

"""A datasource with a schema of its own."""

from __future__ import annotations

from taskflow_meter.datasource.sqlalchemy.models import metadata
from taskflow_meter.datasource.sqlalchemy.source import SQLADataSource
from taskflow_meter.datasource.sqlalchemy.source import alembic_config
from taskflow_meter.datasource.sqlalchemy.source import upgrade

__all__ = ["SQLADataSource", "alembic_config", "metadata", "upgrade"]

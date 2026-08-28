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

"""Datasources: where flow state is read from, and optionally written to."""

from __future__ import annotations

from taskflow_meter.datasource.base import DataSource
from taskflow_meter.datasource.base import EventPage
from taskflow_meter.datasource.base import FlowPage
from taskflow_meter.datasource.base import UnknownMarkerError
from taskflow_meter.datasource.base import WritableDataSource
from taskflow_meter.datasource.memory import MemoryDataSource

__all__ = [
    "DataSource",
    "EventPage",
    "FlowPage",
    "MemoryDataSource",
    "UnknownMarkerError",
    "WritableDataSource",
]

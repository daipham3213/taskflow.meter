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

"""The emit side: watch an engine as it runs."""

from __future__ import annotations

from taskflow_meter.collect.attachment import Attachment
from taskflow_meter.collect.attachment import attach
from taskflow_meter.collect.attachment import describe_graph
from taskflow_meter.collect.listener import MeterListener
from taskflow_meter.collect.pipeline import EventPipeline
from taskflow_meter.collect.progress import ProgressTap

__all__ = [
    "Attachment",
    "EventPipeline",
    "MeterListener",
    "ProgressTap",
    "attach",
    "describe_graph",
]

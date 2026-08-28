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

"""Where events go once they leave the process that produced them.

A publisher is handed batches, never single events: the pipeline drains
its queue in one go, so a transport that can amortise a round trip gets
the chance to.
"""

from __future__ import annotations

import abc
from collections.abc import Sequence
from types import TracebackType
from typing import Self

from taskflow_meter.events import Event


class Publisher(abc.ABC):
    """Sends events somewhere.

    Implementations may raise: the pipeline that drives them counts and
    logs failures rather than letting one reach the flow being watched.
    """

    #: Stevedore plugin name, set by subclasses.
    name: str = ""

    def start(self) -> None:  # noqa: B027 - optional hook, not abstract
        """Acquire whatever the transport needs.  Idempotent."""

    def stop(self) -> None:  # noqa: B027 - optional hook, not abstract
        """Release it again.  Idempotent, and safe to call unstarted."""

    @abc.abstractmethod
    def publish(self, events: Sequence[Event]) -> None:
        """Send a batch.  Never called with an empty one."""

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

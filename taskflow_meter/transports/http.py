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

"""Post batches of events to a webhook.

Uses :mod:`urllib.request` rather than a HTTP client library: this is
one POST of a JSON array, and a monitoring sidecar is a poor reason to
put another dependency into somebody's service.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Sequence

from taskflow_meter.events import Event
from taskflow_meter.transports.base import Publisher

LOG = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5.0
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF = 0.2

#: Statuses worth trying again.  A 4xx means the request was wrong and
#: will be just as wrong the second time.
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class HTTPTransport(Publisher):
    """POSTs ``{"events": [...]}`` to a URL."""

    name = "http"

    def __init__(
        self,
        url: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        backoff: float = DEFAULT_BACKOFF,
        headers: dict[str, str] | None = None,
    ) -> None:
        if retries < 0:
            msg = "retries cannot be negative"
            raise ValueError(msg)
        self.url = url
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.headers = {"content-type": "application/json", **(headers or {})}

    def publish(self, events: Sequence[Event]) -> None:
        body = json.dumps(
            {"events": [event.to_dict() for event in events]},
            separators=(",", ":"),
        ).encode()

        last: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                self._post(body)
            except urllib.error.HTTPError as exc:
                last = exc
                if exc.code not in RETRYABLE_STATUSES:
                    # Retrying a rejected request just rejects it again.
                    raise
            except OSError as exc:
                last = exc
            else:
                return

            if attempt < self.retries:
                time.sleep(self.backoff * (2**attempt))

        assert last is not None
        raise last

    def _post(self, body: bytes) -> None:
        request = urllib.request.Request(
            self.url, data=body, headers=self.headers, method="POST"
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            LOG.debug("published to %s: %s", self.url, response.status)

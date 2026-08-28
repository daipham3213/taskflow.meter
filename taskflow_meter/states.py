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

"""State vocabulary, derived from taskflow rather than duplicated.

Importing the values from :mod:`taskflow.states` keeps us from drifting if
upstream ever renames one, and keeps the groupings in a single place.
"""

from __future__ import annotations

from taskflow import states as _tf

#: Flow states after which no further atom activity is expected.
FLOW_FINISH_STATES: frozenset[str] = frozenset(
    {_tf.SUCCESS, _tf.FAILURE, _tf.REVERTED, _tf.SUSPENDED}
)

#: Atom states in which a result (or failure) is available.  Mirrors
#: ``taskflow.listeners.base.FINISH_STATES``.
ATOM_FINISH_STATES: frozenset[str] = frozenset(
    {_tf.SUCCESS, _tf.FAILURE, _tf.REVERTED, _tf.REVERT_FAILURE}
)

#: Atom states during which reported progress is meaningful.
ATOM_RUNNING_STATES: frozenset[str] = frozenset(
    {_tf.RUNNING, _tf.REVERTING, _tf.RETRYING}
)

#: Atom states that contribute a completed unit of forward work.
#:
#: ``IGNORE`` counts because a decider has ruled the atom out: it will never
#: run, so treating it as outstanding would leave the flow permanently short
#: of 100%.  ``REVERTED`` deliberately does *not* count -- taskflow sets an
#: atom's progress back to 1.0 when its revert finishes, which says the
#: revert completed, not that the work did.
ATOM_COMPLETE_STATES: frozenset[str] = frozenset({_tf.SUCCESS, _tf.IGNORE})

SUCCESS: str = _tf.SUCCESS
FAILURE: str = _tf.FAILURE
PENDING: str = _tf.PENDING
RUNNING: str = _tf.RUNNING
REVERTING: str = _tf.REVERTING
REVERTED: str = _tf.REVERTED
REVERT_FAILURE: str = _tf.REVERT_FAILURE
RETRYING: str = _tf.RETRYING
IGNORE: str = _tf.IGNORE
SUSPENDED: str = _tf.SUSPENDED

EXECUTE: str = _tf.EXECUTE
REVERT: str = _tf.REVERT

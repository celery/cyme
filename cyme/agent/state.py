"""cyme.agent.state

- Global agent state.

- Used to keep track lost connections and so on, which is used by the
  supervisor to know if a node is actually down, or if it is just the
  connection being shaky.

"""

from __future__ import absolute_import

from time import time

from ..utils import cached_property, find_symbol


class State(object):
    broker_last_revived = None

    #: set to true if the process is an cyme-agent
    is_agent = False

    def on_broker_revive(self, *args, **kwargs):
        self.broker_last_revived = time()
        self.supervisor.resume()

    @property
    def time_since_broker_revived(self):
        return time() - self.broker_last_revived

    @cached_property
    def supervisor(self):
        return find_symbol(self, ".supervisor.supervisor")

state = State()

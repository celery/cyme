from time import time

from kombu.utils import cached_property


class State(object):
    broker_last_revived = None

    def on_broker_revive(self, *args, **kwargs):
        self.broker_last_revived = time()
        self.supervisor.resume()

    @property
    def time_since_broker_revived(self):
        return time() - self.broker_last_revived

    @cached_property
    def supervisor(self):
        from scs.supervisor import supervisor
        return supervisor

state = State()

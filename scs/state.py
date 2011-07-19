from time import time


class State(object):
    broker_last_revived = None

    def on_broker_revive(self, *args, **kwargs):
        self.broker_last_revived = time()

    @property
    def time_since_broker_revived(self):
        return time() - self.broker_last_revived

state = State()

"""scs.thread"""

from __future__ import absolute_import

from cl.g import spawn, timer
from cl.log import LogMixin


class gThread(LogMixin):
    name = None

    def __init__(self):
        self.name = self.name or self.__class__.__name__

    def run(self):
        raise NotImplementedError("gThreads must implement 'run'")

    def before(self):
        pass

    def after(self):
        pass

    def start(self):
        self.before()
        try:
            g = spawn(self.run)
            self.debug("%s spawned" % (self.name, ))
            return g
        finally:
            self.after()

    def start_periodic_timer(self, interval, fun, *args, **kwargs):
        return timer(interval, fun, *args, **kwargs)

    @property
    def logger_name(self):
        return self.name

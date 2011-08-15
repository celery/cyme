"""scs.thread"""

from __future__ import absolute_import

import sys

from eventlet import greenthread

from cl.log import LogMixin


class gThread(LogMixin):
    name = None

    def __init__(self):
        if self.name is None:
            self.name = self.__class__.__name__

    def run(self):
        raise NotImplementedError("gThreads must implement 'run'")

    def before(self):
        pass

    def after(self):
        pass

    def start(self):
        self.before()
        try:
            return greenthread.spawn(self.run)
        finally:
            self.after()

    def start_periodic_timer(self, interval, fun, *args, **kwargs):

        def _re(interval):
            try:
                greenthread.spawn(fun, *args, **kwargs).wait()
            except Exception, exc:
                self.error("Periodic timer %r raised: %r" % (fun, exc),
                           exc_info=sys.exc_info())
            finally:
                greenthread.spawn_after(interval, _re, interval=interval)
        return greenthread.spawn_after(interval, _re, interval=interval)

    @property
    def logger_name(self):
        return self.name

import logging
import sys

from celery.utils.encoding import safe_str
from eventlet import greenthread


class gThread(object):
    name = None

    def __init__(self):
        if self.name is None:
            self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

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

    def run(self):
        raise NotImplementedError("gThreads must implement 'run'")

    def debug(self, *args, **kwargs):
        return self._log(logging.DEBUG, *args, **kwargs)

    def info(self, *args, **kwargs):
        return self._log(logging.INFO, *args, **kwargs)

    def warn(self, *args, **kwargs):
        return self._log(logging.WARN, *args, **kwargs)

    def error(self, *args, **kwargs):
        return self._log(logging.ERROR, *args, **kwargs)

    def _log(self, severity, *args, **kwargs):
        body = "{%s} %s" % (self.name, " ".join(map(safe_str, args)))
        return self.logger.log(severity, body, **kwargs)

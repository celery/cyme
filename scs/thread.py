import logging

from eventlet import greenthread


class gThread(object):
    name = None

    def __init__(self):
        if self.name is None:
            self.name = self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    def start(self):
        return greenthread.spawn(self.run)

    def run(self):
        raise NotImplementedError("gThreads must implement 'run'")

    def debug(self, t):
        return self._log(logging.DEBUG, t)

    def info(self, t):
        return self._log(logging.INFO, t)

    def warn(self, t):
        return self._log(logging.WARN, t)

    def error(self, t):
        return self._log(logging.ERROR, t)

    def _log(self, severity, t):
        return self.logger.log(severity, "%s: %s" % (self.name, t, ))


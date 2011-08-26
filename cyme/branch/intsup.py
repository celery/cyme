"""cyme.branch.intsup

- Internal supervisor used to ensure our threads are still running.

"""

from __future__ import absolute_import, with_statement

from os import _exit
from time import sleep

from cl.g import Event

from .thread import gThread

SUP_ERROR_NOT_STARTED = """\
found thread not able to start?\
"""

SUP_ERROR_PING_TIMEOUT = """\
suspected thread crash or blocking: %r\
"""


class gSup(gThread):

    def __init__(self, thread, signal, interval=5, timeout=600):
        self.thread = thread
        self.interval = interval
        self.timeout = timeout
        self.signal = signal
        super(gSup, self).__init__()

    def start_wait_child(self):
        self._ready_event = Event()
        self.signal.connect(self._on_thread_ready, sender=self.thread)
        self.thread.start()
        self._ready_event.wait()
        assert self._ready_event.ready()
        return self.thread

    def _on_thread_ready(self, **kwargs):
        self._ready_event.send(1)
        self.signal.disconnect(self._on_thread_ready)

    def run(self):
        self.debug("starting")
        interval = self.interval
        thread = self.start_wait_child()
        self.info("started")
        timeout = self.timeout
        critical = self.critical

        while not self.should_stop:
            try:
                pong = thread.ping(timeout)
            except (self.Timeout, Exception), exc:
                critical(SUP_ERROR_PING_TIMEOUT % (exc, ))
                _exit(0)
            if not pong:
                critical(SUP_ERROR_NOT_STARTED)
                _exit(0)
            sleep(interval)

    def stop(self):
        super(gSup, self).stop()
        self.thread.stop()

    @property
    def logger_name(self):
        return "%s<%s>" % (self.name, self.thread.name)

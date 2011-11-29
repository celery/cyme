"""cyme.branch.thread

- Utilities for working with greenlets.

"""

from __future__ import absolute_import
from __future__ import with_statement

import os

from Queue import Empty

from cl.g import Event, spawn, timer, Queue
from kombu.log import LogMixin
from eventlet import Timeout

from . import signals


class AlreadyStartedError(Exception):
    """Raised if trying to start a thread instance that is already started."""


class gThread(LogMixin):
    AlreadyStarted = AlreadyStartedError
    Timeout = Timeout

    #: Name of the thread, used in logs and such.
    name = None

    #: Greenlet instance of the thread, set when the thread is started.
    g = None

    #: Set when the thread is requested to stop.
    should_stop = False

    #: Set this to False if it is not possible to join the thread.
    joinable = True

    _exit_event = None
    _ping_queue = None
    _timers = None
    extra_startup_steps = 0
    extra_shutdown_steps = 0

    def __init__(self):
        self.name = self.name or self.__class__.__name__
        # the exit event is sent when the thread exits,
        # and used by `join` to detect this.
        self._exit_event = Event()

        # we maintain a list of timers started by the thread,
        # so these can be cancelled at shutdown.
        self._timers = []

    def before(self):
        """Called at the beginning of :meth:`start`."""
        pass

    def run(self):
        raise NotImplementedError("gThreads must implement 'run'")

    def after(self):
        """Call after the thread has shut down."""
        pass

    def start_periodic_timer(self, interval, fun, *args, **kwargs):
        """Apply function every ``interval`` seconds.

        :param interval: Interval in seconds (int/float).
        :param fun: The function to apply.
        :param \*args: Additional arguments to pass.
        :keyword \*\*kwargs: Additional keyword arguments to pass.

        :returns: entry object, with ``cancel`` and ``kill`` methods.

        """
        entry = timer(interval, fun, *args, **kwargs)
        self._timers.append(entry)
        return entry

    def start(self):
        """Spawn green thread, and returns
        :class:`~eventlet.greenthread.GreenThread` instance."""
        if self.g is None:
            self.before()
            signals.thread_pre_start.send(sender=self)
            self._ping_queue = Queue()
            g = self.g = self.spawn(self._crashsafe, self.run)
            g.link(self._on_exit)
            self.debug("%s spawned", self.name)
            signals.thread_post_start.send(sender=self)
            return g
        raise self.AlreadyStarted("can't start thread twice")

    def stop(self, join=True, timeout=1e100):
        """Shutdown the thread.

        This will also cancel+kill any periodic timers registered
        by the thread.

        :keyword join: Given that the thread is :attr:`joinable`, if
            true will also wait until the thread exits
            (by calling :meth:`join`).
        :keyword timeout: Timeout for join (default is 1e+100).

        """
        self.debug("shutdown initiated")
        signals.thread_pre_shutdown.send(sender=self)
        self.should_stop = True
        for entry in self._timers:
            self.debug("killing timer %r" % (entry, ))
            entry.cancel()
            entry.kill()
        if join and self.joinable:
            try:
                self.join(timeout)
            except self.Timeout:
                self.error(
                    "exceeded exit timeout (%s), will try to kill", timeout)
                self.kill()
        self.after()
        signals.thread_post_shutdown.send(sender=self)

    def join(self, timeout=None):
        """Wait until the thread exits.

        :keyword timeout: Timeout in seconds (int/float).

        :raises eventlet.Timeout: if the thread can't be joined
            before the provided timeout.


        """
        with self.Timeout(timeout):
            signals.thread_pre_join.send(sender=self, timeout=timeout)
            self.debug("joining (%s)", timeout)
            self._exit_event.wait()
            signals.thread_post_join.send(sender=self)

    def respond_to_ping(self):
        try:
            self._ping_queue.get_nowait().send()
        except Empty:
            pass

    def _do_ping(self, timeout=None):
        with self.Timeout(timeout):
            event = Event()
            self._ping_queue.put_nowait(event)
            event.wait()
            return event.ready()

    def ping(self, timeout=None):
        if self.g and self._ping_queue is not None:
            return self._do_ping(timeout)
        return False

    def _on_exit(self, g):
        # called when the thread exits to unblock `join`.
        self._exit_event.send()

    def kill(self):
        """Kill the green thread."""
        if self.g is not None:
            self.g.kill()

    def _crashsafe(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
            signals.thread_exit.send(sender=self)
            self.debug("exiting")
        except Exception, exc:
            self.error("Thread crash detected: %r", exc)
            os._exit(0)
        except self.Timeout, exc:
            self.error("Thread raised timeout: %r", exc)
            os._exit(0)

    def spawn(self, fun, *args, **kwargs):
        return spawn(fun, *args, **kwargs)

    @property
    def logger_name(self):
        return self.name

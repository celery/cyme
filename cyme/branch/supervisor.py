"""cyme.branch.supervisor"""

from __future__ import absolute_import
from __future__ import with_statement

from threading import Lock
from Queue import Empty

from celery.local import Proxy
from eventlet.queue import LightQueue
from eventlet.event import Event

from .signals import supervisor_ready
from .thread import gThread

from ..status import Status

__current = None


class Supervisor(gThread, Status):
    """The supervisor wakes up at intervals to monitor changes in the model.
    It can also be requested to perform specific operations, and these
    operations can be either async or sync.

    :keyword interval:  This is the interval (in seconds as an int/float),
       between verifying all the registered instances.
    :keyword queue: Custom :class:`~Queue.Queue` instance used to send
        and receive commands.

    It is responsible for:

        * Stopping removed instances.
        * Starting new instances.
        * Restarting unresponsive/killed instances.
        * Making sure the instances consumes from the queues
          specified in the model, sending ``add_consumer``/-
          ``cancel_consumer`` broadcast commands to the instances as it
          finds inconsistencies.
        * Making sure the max/min concurrency setting is as specified in the
          model,  sending ``autoscale`` broadcast commands to the noes
          as it finds inconsistencies.

    The supervisor is resilient to intermittent connection failures,
    and will auto-retry any operation that is dependent on a broker.

    Since workers cannot respond to broadcast commands while the
    broker is off-line, the supervisor will not restart affected
    instances until the instance has had a chance to reconnect (decided
    by the :attr:`wait_after_broker_revived` attribute).

    """
    #: Limit instance restarts to 1/m, so out of control
    #: instances will be disabled
    restart_max_rate = "1/m"

    #: Default interval_max for ensure_connection is 30 secs.
    wait_after_broker_revived = 35.0

    #: Connection errors pauses the supervisor, so events does not accumulate.
    paused = False

    #: Default interval (time in seconds as a float to reschedule).
    interval = 60.0

    def __init__(self, interval=None, queue=None, set_as_current=True):
        self.set_as_current = set_as_current
        if self.set_as_current:
            set_current(self)
        self._orig_queue_arg = queue
        self.interval = interval or self.interval
        self.queue = LightQueue() if queue is None else queue
        self._pause_mutex = Lock()
        self._last_update = None
        gThread.__init__(self)
        Status.__init__(self)

    def __copy__(self):
        return self.__class__(self.interval, self._orig_queue_arg)

    def pause(self):
        """Pause all timers."""
        self.respond_to_ping()
        with self._pause_mutex:
            if not self.paused:
                self.debug("pausing")
                self.paused = True

    def resume(self):
        """Resume all timers."""
        with self._pause_mutex:
            if self.paused:
                self.debug("resuming")
                self.paused = False

    def verify(self, instances, ratelimit=False):
        """Verify the consistency of one or more instances.

        :param instances: List of instances to verify.

        This operation is asynchronous, and returns a :class:`Greenlet`
        instance that can be used to wait for the operation to complete.

        """
        return self._request(instances, self._do_verify_instance,
                            {"ratelimit": ratelimit})

    def restart(self, instances):
        """Restart one or more instances.

        :param instances: List of instances to restart.

        This operation is asynchronous, and returns a :class:`Greenlet`
        instance that can be used to wait for the operation to complete.

        """
        return self._request(instances, self._do_restart_instance)

    def shutdown(self, instances):
        """Shutdown one or more instances.

        :param instances: List of instances to stop.

        This operation is asynchronous, and returns a :class:`Greenlet`
        instance that can be used to wait for the operation to complete.

        .. warning::

            Note that the supervisor will automatically restart
            any stopped instances unless the corresponding :class:`Instance`
            model has been marked as disabled.

        """
        return self._request(instances, self._do_stop_instance)

    def _request(self, instances, action, kwargs={}):
        event = Event()
        self.queue.put_nowait((instances, event, action, kwargs))
        return event

    def before(self):
        self.start_periodic_timer(self.interval, self._verify_all)

    def run(self):
        queue = self.queue
        self.info("started")
        supervisor_ready.send(sender=self)
        while not self.should_stop:
            try:
                instances, event, action, kwargs = queue.get(timeout=1)
            except Empty:
                self.respond_to_ping()
                continue
            self.respond_to_ping()
            self.debug("wake-up")
            try:
                for instance in instances:
                    try:
                        action(instance, **kwargs)
                    except Exception, exc:
                        self.error("Event caused exception: %r", exc)
            finally:
                event.send(True)

    def _verify_all(self, force=False):
        if self._last_update and self._last_update.ready():
            try:
                self._last_update.wait()  # collect result
            except self.GreenletExit:
                pass
            force = True
        if not self._last_update or force:
            self._last_update = self.verify(self.all_instances(),
                                            ratelimit=True)


class _OfflineSupervisor(object):

    def wait(self):
        pass

    def _noop(self, *args, **kwargs):
        return self
    pause = resume = verify = shutdown = restart = _noop


def set_current(sup):
    global __current
    __current = sup
    return __current


def get_current():
    if __current is None:
        return _OfflineSupervisor()
    return __current

supervisor = Proxy(get_current)

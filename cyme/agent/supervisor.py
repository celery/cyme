"""cyme.agent.supervisor"""

from __future__ import absolute_import, with_statement

from collections import defaultdict
from threading import Lock
from Queue import Empty

from celery.datastructures import TokenBucket
from celery.local import LocalProxy
from celery.log import SilenceRepeated
from celery.utils.timeutils import rate
from cl.common import insured as _insured
from eventlet.queue import LightQueue
from eventlet.event import Event
from kombu.syn import blocking
from kombu.utils import fxrangemax

from .signals import supervisor_ready
from .state import state
from .thread import gThread

from ..models import Instance

__current = None


def insured(instance, fun, *args, **kwargs):
    """Ensures any function performing a broadcast command completes
    despite intermittent connection failures."""

    def errback(exc, interval):
        supervisor.error(
            "Error while trying to broadcast %r: %r\n" % (fun, exc))
        supervisor.pause()

    return _insured(instance.broker.pool, fun, args, kwargs,
                    on_revive=state.on_broker_revive,
                    errback=errback)


def ib(fun, *args, **kwargs):
    """Shortcut to ``blocking(insured(fun.im_self, fun(*args, **kwargs)))``"""
    return blocking(insured, fun.im_self, fun, *args, **kwargs)


class Supervisor(gThread):
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
    and will autoretry any operation that is dependent on a broker.

    Since workers cannot respond to broadcast commands while the
    broker is offline, the supervisor will not restart affected
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
        self._buckets = defaultdict(lambda: TokenBucket(
                                        rate(self.restart_max_rate)))
        self._pause_mutex = Lock()
        self._last_update = None
        super(Supervisor, self).__init__()
        self._rinfo = SilenceRepeated(self.info, max_iterations=30)

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
            self._rinfo("wake-up")
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
            self._last_update = self.verify(Instance.objects.all(),
                                            ratelimit=True)

    def _request(self, instances, action, kwargs={}):
        event = Event()
        self.queue.put_nowait((instances, event, action, kwargs))
        return event

    def _verify_restart_instance(self, instance):
        """Restarts the instance, and verifies that the instance is
        actually able to start."""
        self.warn("%s instance.restart" % (instance, ))
        blocking(instance.restart)
        is_alive = False
        for i in fxrangemax(0.1, 1, 0.4, 30):
            self.info("%s pingWithTimeout: %s", instance, i)
            self.respond_to_ping()
            if insured(instance, instance.responds_to_ping, timeout=i):
                is_alive = True
                break
        if is_alive:
            self.warn("%s successfully restarted" % (instance, ))
        else:
            self.warn("%s instance doesn't respond after restart" % (
                    instance, ))

    def _can_restart(self):
        """Returns true if the supervisor is allowed to restart
        an instance at this point."""
        if state.broker_last_revived is None:
            return True
        return state.time_since_broker_revived \
                > self.wait_after_broker_revived

    def _do_restart_instance(self, instance, ratelimit=False):
        bucket = self._buckets[instance.restart]
        if ratelimit:
            if self._can_restart():
                if bucket.can_consume(1):
                    self._verify_restart_instance(instance)
                else:
                    self.error(
                        "%s instance.disabled: Restarted too often", instance)
                    instance.disable()
                    self._buckets.pop(instance.restart)
        else:
            self._buckets.pop(instance.restart, None)
            self._verify_restart_instance(instance)

    def _do_stop_instance(self, instance):
        self.warn("%s instance.shutdown" % (instance, ))
        blocking(instance.stop)

    def _do_verify_instance(self, instance, ratelimit=False):
        if not self.paused:
            if instance.is_enabled and instance.pk:
                if not ib(instance.alive):
                    self._do_restart_instance(instance, ratelimit=ratelimit)
                self._verify_instance_processes(instance)
                self._verify_instance_queues(instance)
            else:
                if ib(instance.alive):
                    self._do_stop_instance(instance)

    def _verify_instance_queues(self, instance):
        """Verify that the queues the instance is consuming from matches
        the queues listed in the model."""
        queues = set(instance.queues)
        reply = ib(instance.consuming_from)
        if reply is None:
            return
        consuming_from = set(reply.keys())

        for queue in consuming_from ^ queues:
            if queue in queues:
                self.warn("%s: instance.consume_from: %s" % (instance, queue))
                ib(instance.add_queue, queue)
            elif queue == instance.direct_queue:
                pass
            else:
                self.warn(
                    "%s: instance.cancel_consume: %s" % (instance, queue))
                ib(instance.cancel_queue, queue)

    def _verify_instance_processes(self, instance):
        """Verify that the max/min concurrency settings of the
        instance matches that which is specified in the model."""
        max, min = instance.max_concurrency, instance.min_concurrency
        try:
            current = insured(instance, instance.stats)["autoscaler"]
        except (TypeError, KeyError):
            return
        if max != current["max"] or min != current["min"]:
            self.warn("%s: instance.set_autoscale max=%r min=%r" % (
                instance, max, min))
            ib(instance.autoscale, max, min)


class _MockSupervisor(object):

    def wait(self):
        pass

    def _noop(self, *args, **kwargs):
        return self
    verify = shutdown = restart = _noop


def set_current(sup):
    global __current
    __current = sup
    return __current


def get_current():
    if __current is None:
        return _MockSupervisor()
    return __current

supervisor = LocalProxy(get_current)

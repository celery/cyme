"""cyme.status"""

from __future__ import absolute_import, with_statement

from collections import defaultdict

from celery.datastructures import TokenBucket
from celery.utils.timeutils import rate
from kombu.common import insured as _insured
from kombu.log import LogMixin
from kombu.utils import fxrangemax

from .models import Instance
from .branch.state import state


class Status(LogMixin):
    paused = False
    restart_max_rate = "100/s"

    def __init__(self):
        self._buckets = defaultdict(lambda: TokenBucket(
                                        rate(self.restart_max_rate)))

    def start_all(self):
        for instance in self.all_instances():
            self._do_verify_instance(instance, ratelimit=False)

    def restart_all(self):
        for instance in self.all_instances():
            self._do_restart_instance(instance, ratelimit=False)

    def shutdown_all(self):
        for instance in self.all_instances():
            try:
                self._do_stop_verify_instance(instance)
            except Exception, exc:
                self.error("Couldn't stop instance %s: %r" % (instance.name,
                                                              exc))

    def all_instances(self):
        return Instance.objects.all()

    def insured(self, instance, fun, *args, **kwargs):
        """Ensures any function performing a broadcast command completes
        despite intermittent connection failures."""

        def errback(self, exc, interval):
            self.error(
                "Error while trying to broadcast %r: %r\n" % (fun, exc))
            self.pause()

        return _insured(instance.broker.pool, fun, args, kwargs,
                        on_revive=state.on_broker_revive,
                        errback=errback)

    def ib(self, fun, *args, **kwargs):
        """Shortcut to ``self.insured(
            fun.im_self, fun(*args, **kwargs))``"""
        return self.insured(fun.im_self, fun, *args, **kwargs)

    def pause(self):
        self.paused = True  # only used in supervisor.

    def resume(self):
        self.paused = False

    def respond_to_ping(self):
        pass

    def _verify_restart_instance(self, instance):
        """Restarts the instance, and verifies that the instance is
        actually able to start."""
        self.info("%s instance.restart" % (instance, ))
        instance.restart()
        is_alive = False
        for i in fxrangemax(0.1, 1, 0.4, 30):
            self.info("%s pingWithTimeout: %s", instance, i)
            self.respond_to_ping()
            if self.insured(instance, instance.responds_to_ping, timeout=i):
                is_alive = True
                break
        if is_alive:
            self.info("%s successfully restarted" % (instance, ))
        else:
            self.info("%s instance doesn't respond after restart" % (
                    instance, ))

    def _can_restart(self):
        """Returns true if the supervisor is allowed to restart
        an instance at this point."""
        return True

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
        self.info("%s instance.shutdown" % (instance, ))
        instance.stop()

    def _do_stop_verify_instance(self, instance):
        self.info("%s instance.shutdown" % (instance, ))
        instance.stop_verify()

    def _do_verify_instance(self, instance, ratelimit=False):
        if not self.paused:
            if instance.is_enabled and instance.pk:
                if not self.ib(instance.alive):
                    self._do_restart_instance(instance, ratelimit=ratelimit)
                self._verify_instance_processes(instance)
                self._verify_instance_queues(instance)
            else:
                if self.ib(instance.alive):
                    self._do_stop_instance(instance)

    def _verify_instance_queues(self, instance):
        """Verify that the queues the instance is consuming from matches
        the queues listed in the model."""
        queues = set(instance.queues)
        reply = self.ib(instance.consuming_from)
        if reply is None:
            return
        consuming_from = set(reply.keys())

        for queue in consuming_from ^ queues:
            if queue in queues:
                self.info("%s: instance.consume_from: %s" % (instance, queue))
                self.ib(instance.add_queue, queue)
            elif queue == instance.direct_queue:
                pass
            else:
                self.info(
                    "%s: instance.cancel_consume: %s" % (instance, queue))
                self.ib(instance.cancel_queue, queue)

    def _verify_instance_processes(self, instance):
        """Verify that the max/min concurrency settings of the
        instance matches that which is specified in the model."""
        max, min = instance.max_concurrency, instance.min_concurrency
        try:
            current = self.insured(instance, instance.stats)["autoscaler"]
        except (TypeError, KeyError):
            return
        if max != current["max"] or min != current["min"]:
            self.info("%s: instance.set_autoscale max=%r min=%r" % (
                instance, max, min))
            self.ib(instance.autoscale, max, min)

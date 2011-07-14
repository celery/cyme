import eventlet
import logging

from collections import defaultdict

from celery.datastructures import TokenBucket
from celery.utils.timeutils import rate
from eventlet.queue import LightQueue
from eventlet.event import Event
from kombu.syn import blocking

from scs.models import Node
from scs.thread import gThread


logger = logging.getLogger("Supervisor")


class Supervisor(gThread):
    restart_max_rate = "1/m"

    def __init__(self, queue=None):
        if queue is None:
            queue = LightQueue()
        self.queue = queue
        self._buckets = defaultdict(lambda: TokenBucket(
                                    rate(self.restart_max_rate)))
        super(Supervisor, self).__init__()

    def start(self):
        self.start_periodic_update()
        return super(Supervisor, self).start()

    def verify(self, nodes):
        return self._request(nodes, self._do_verify_node)

    def restart(self, nodes):
        return self._request(nodes, self._do_restart_node)

    def stop(self, nodes):
        return self._request(nodes, self._do_stop_node)

    def start_periodic_update(self, interval=5.0):
        self.verify(Node.objects.all())
        eventlet.spawn_after(interval, self.start_periodic_update,
                             interval=interval)

    def run(self):
        queue = self.queue
        debug = self.debug
        self.info("started...")
        while 1:
            nodes, event, action = queue.get()
            debug("wake-up")
            for node in nodes:
                action(node)
            event.send(True)

    def _request(self, nodes, action):
        event = Event()
        self.queue.put_nowait((nodes, event, action))
        return event

    def _verify_restart_node(self, node):
        blocking(node.restart)
        is_alive = False
        for i in (0.1, 0.5, 1, 1, 1, 1):
            self.info("%s pingWithTimeout: %s" % (node, i))
            if node.responds_to_ping(timeout=i):
                is_alive = True
                break
        if is_alive:
            self.warn("%s successfully restarted" % (node, ))
        else:
            self.warn("%s node doesn't respond after restart" % (
                    node, ))

    def _do_restart_node(self, node, ratelimit=False):
        self.warn("%s node.restart" % (node, ))
        bucket = self._buckets[node.restart]
        if ratelimit:
            if bucket.can_consume(1):
                self._verify_restart_node(node)
            else:
                self.error(
                    "%s node.disabled: Restarted too many times" % (node, ))
                node.disable()
                self._buckets.pop(node.restart)
        else:
            self._buckets.pop(node.restart, None)
            self._verify_restart_node(node)

    def _do_stop_node(self, node):
        self.warn("%s node.shutdown" % (node, ))
        blocking(node.stop)

    def _do_verify_node(self, node):
        if node.is_enabled and node.pk:
            if not node.alive():
                self._do_restart_node(node, ratelimit=True)
            self.verify_node_processes(node)
            self.verify_node_queues(node)
        else:
            if node.alive():
                self._do_stop_node(node)

    def verify_node_queues(self, node):
        queues = set(queue.name for queue in node.queues.enabled())
        consuming_from = set(node.consuming_from().keys())

        for queue in consuming_from ^ queues:
            if queue in queues:
                self.warn("%s: node.consume_from: %s" % (node, queue))
                blocking(node.add_queue, queue)
            elif queue == node.direct_queue:
                pass
            else:
                self.warn("%s: node.cancel_consume: %s" % (node, queue))
                blocking(node.cancel_queue, queue)

    def verify_node_processes(self, node):
        max, min = node.max_concurrency, node.min_concurrency
        try:
            current = node.stats()["autoscaler"]
        except (TypeError, KeyError):
            return
        if max != current["max"] or min != current["min"]:
            self.warn("%s: node.set_autoscale max=%r min=%r" % (
                node, max, min))
            blocking(node.autoscale, max, min)

supervisor = Supervisor()

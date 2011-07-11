import eventlet
import logging

from eventlet.queue import LightQueue
from eventlet.event import Event
from time import sleep

from django.db.models import signals

from scs.models import Node


logger = logging.getLogger("Supervisor")


class Supervisor(object):

    def __init__(self, queue=None):
        if queue is None:
            queue = LightQueue()
        self.queue = queue

    def start(self):
        self.start_periodic_update()
        return eventlet.spawn(self.run)

    def request_update(self, nodes):
        event = Event()
        self.queue.put_nowait((nodes, event))
        return event

    def start_periodic_update(self, interval=5.0):
        self.request_update(Node.objects.all())
        eventlet.spawn_after(interval, self.start_periodic_update,
                             interval=interval)

    def run(self):
        w = lambda t: logger.warning("Supervisor: %s" % (t, ))
        queue = self.queue
        logger.info("Supervisor started...")
        while 1:
            nodes, event = self.queue.get()
            logger.info("Supervisor wake-up")
            for node in nodes:
                if node.is_active:
                    if not node.alive():
                        w("Restarting offline node: %r" % ( node, ))
                        eventlet.spawn(node.restart).wait()
                else:
                    if node.alive():
                        w("Shutting down disabled node: %r" % (node, ))
                        eventlet.spawn(node.stop).wait()
            event.send(True)

supervisor = Supervisor()

from kombu import Exchange, Queue

from scs.consumer import gConsumer
from scs.models import Node, Queue as Q


class Controller(gConsumer):
    node_control_exchange = Exchange("scs.nodes")
    node_control_consumer = None
    node_control = set()
    q_control_exchange = Exchange("scs.queues")
    q_control_consumer = None
    q_control = set()

    def __init__(self, id):
        self.id = id
        super(Controller, self).__init__()

    def when_node_enabled(self, node):
        self.node_control.add(
            self.node_control_consumer.add_queue(
                self.create_node_queue(node.name)))

    def when_node_disabled(self, node):
        queue = self.find_queue_by_rkey(node.name, self.node_control)
        if queue:
            self.node_control_consumer.cancel_by_queue(queue.name)

    def when_queue_added(self, queue):
        self.q_control.add(
                self.q_control_consumer.add_queue(
                    self.create_q_queue(queue.name)))

    def when_queue_removed(self, q):
        queue = self.find_queue_by_rkey(q.name, self.q_control)
        if queue:
            self.q_control_consumer.cancel_by_queue(queue.name)

    def create_node_control_consumer(self, Consumer):
        self.sync_model_queues([node.name for node in Node.objects.enabled()],
                               self.node_control, self.create_node_queue)
        return Consumer(self.node_control, callbacks=[self.on_node_control])

    def create_q_control_consumer(self, Consumer):
        self.sync_model_queues([q.name for q in Q.objects.enabled()],
                               self.q_control, self.create_q_queue)
        return Consumer(self.q_control, callbacks=[self.on_q_control])

    def create_node_queue(self, nodename):
        return Queue(self.uuid(), self.node_control_exchange, nodename,
                     auto_delete=True)

    def create_q_queue(self, qname):
        return Queue(self.uuid(), self.q_control_exchange, qname,
                     auto_delete=True)

    def get_consumers(self, Consumer):
        return [self.create_node_control_consumer(Consumer),
                self.create_q_control_consumer(Consumer)]

    def on_node_control(self, body, message):
        message.ack()

    def on_q_control(self, body, message):
        message.ack()

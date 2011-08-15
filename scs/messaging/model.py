"""scs.messaging.model"""

from __future__ import absolute_import

from kombu import Consumer, Queue
from kombu.utils import gen_unique_id


class ModelConsumer(Consumer):
    model = None
    field = "name"
    auto_delete = True

    def __init__(self, channel, exchange, *args, **kwargs):
        model = kwargs.pop("model", None)
        self.model = model if model is not None else self.model
        self.exchange = exchange
        self.prepare_signals(kwargs.pop("sigmap", None))
        queues = self.sync_queues(kwargs.pop("queues", []))
        super(ModelConsumer, self).__init__(channel, queues, *args, **kwargs)

    def prepare_signals(self, sigmap=None):
        for callback, connect in (sigmap or {}).iteritems():
            if isinstance(callback, basestring):
                callback = getattr(self, callback)
            connect(callback)

    def create_queue(self, field_value):
        return Queue(gen_unique_id(), self.exchange, field_value,
                     auto_delete=self.auto_delete)

    def sync_queues(self, keep_queues=[]):
        expected = [getattr(obj, self.field)
                        for obj in self.model._default_manager.enabled()]
        queues = set()
        create = self.create_queue

        for v in expected:
            queues.add(create(v))
        for queue in queues:
            if queue.routing_key not in expected:
                queues.discard(v)
        return list(keep_queues) + list(queues)

    def on_create(self, instance=None, **kwargs):
        fv = getattr(instance, self.field)
        if not self.find_queue_by_rkey(fv):
            self.add_queue(self.create_queue(fv))
            self.consume()

    def on_delete(self, instance=None, **kwargs):
        fv = getattr(instance, self.field)
        queue = self.find_queue_by_rkey(fv)
        if queue:
            self.cancel_by_queue(queue.name)

    def find_queue_by_rkey(self, rkey):
        for queue in self.queues:
            if queue.routing_key == rkey:
                return queue

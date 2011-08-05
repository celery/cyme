from kombu import Consumer
from kombu.utils import gen_unique_id


class ModelConsumer(Consumer):
    model = None
    field = "name"
    auto_delete = True

    def __init__(self, channel, exchange, *args, **kwargs):
        model = kwargs.pop("model", None)
        if model is not None:
            self.model = model
        self.exchange = exchange
        self.prepare_signals(kwargs.pop("sigmap", None))
        super(ModelConsumer, self).__init__(channel, self.sync_queues(),
                                            *args, **kwargs)

    def prepare_signals(self, sigmap=None):
        for callback, signal in (sigmap or {}).iteritems():
            if isinstance(callback, basestring):
                callback = getattr(self, callback)
            signal.connect(callback)

    def create_queue(self, field_value):
        return self.model(gen_unique_id(), self.exchange, field_value,
                     auto_delete=self.auto_delete)

    def sync_queues(self):
        expected = [getattr(obj, self.field)
                        for obj in self.model._default_manager.enabled()]
        queues = []
        create = self.create_queue

        for v in expected:
            queues.add(create(v))
        for queue in queues:
            if queue.routing_key not in expected:
                queues.discard(v)
        return queues

    def on_create(self, instance=None, **kwargs):
        fv = getattr(instance, self.field)
        if not self.find_queue_by_rkey(fv):
            self.add(self.create_queue(fv))
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

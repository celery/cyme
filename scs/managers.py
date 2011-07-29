from anyjson import serialize
from celery import current_app as celery
from celery.utils import gen_unique_id

from djcelery.managers import ExtendedManager

from scs.utils import maybe_list


class BrokerManager(ExtendedManager):

    def get_default(self):
        conf = celery.conf
        broker, _ = self.get_or_create(
                        hostname=conf.BROKER_HOST or "127.0.0.1",
                        userid=conf.BROKER_USER or "guest",
                        password=conf.BROKER_PASSWORD or "guest",
                        port=conf.BROKER_PORT or 5672,
                        virtual_host=conf.BROKER_VHOST or "/")
        return broker


class NodeManager(ExtendedManager):

    def enabled(self):
        return self.filter(is_enabled=True)

    def disabled(self):
        return self.filter(is_enabled=False)

    def _maybe_queues(self, queues):
        acc = []
        Queue = self.Queue
        if isinstance(queues, basestring):
            queues = queues.split(",")
        for queue in queues:
            if not isinstance(queue, Queue):
                queue, _ = Queue._default_manager.get_or_create(name=queue)
            acc.append(queue)
        return acc

    def add(self, nodename=None, queues=None, max_concurrency=1,
            min_concurrency=1, broker=None):
        nodename = nodename or gen_unique_id()

        node = self.create(name=nodename or gen_unique_id(),
                           max_concurrency=max_concurrency,
                           min_concurrency=min_concurrency)
        needs_save = False
        if queues:
            node.queues = self._maybe_queues(queues)
            needs_save = True
        if broker:
            node._broker = broker
            needs_save = True
        if needs_save:
            node.save()
        return node

    def modify(self, nodename, queues, max_concurrency=None,
            min_concurrency=None):
        node = self.get(name=nodename)
        node.queues = self._maybe_queues(queues)
        node.max_concurrency = max_concurrency
        node.min_concurrency = min_concurrency
        node.save()
        return node

    def remove(self, nodename):
        node = self.get(name=nodename)
        node.delete()
        return node

    def enable(self, nodename):
        node = self.get(name=nodename)
        node.enable()
        return node

    def disable(self, nodename):
        node = self.get(name=nodename)
        node.disable()
        return node

    def add_queue(self, name, nodenames=None, exchange=None,
            exchange_type=None, routing_key=None, **options):
        options = serialize(options) if options else None
        nodenames = maybe_list(nodenames)
        queue = self.queues.update_or_create(name=name,
                    defaults={"exchange": exchange,
                              "exchange_type": exchange_type,
                              "routing_key": routing_key,
                              "options": options})
        if nodenames:
            self._add_queue_to_nodes(queue, name__in=nodenames)
        else:
            self._remove_queue_from_nodes(queue)
            self._add_queue_to_nodes(queue)

    def remove_queue(self, name, nodenames=None):
        nodenames = maybe_list(nodenames)
        queue = self.queues.get(name=name)
        if nodenames:
            self._remove_queue_from_nodes(queue, name__in=nodenames)
        else:
            self._remove_queue_from_nodes(queue)

    def _remove_queue_from_nodes(self, queue, **query):
        for node in self.filter(**query).iterator():
            node.queues.remove(queue)
            node.save()

    def _add_queue_to_nodes(self, queue, **query):
        for node in self.filter(**query).iterator():
            node.queues.add(queue)
            node.save()

    @property
    def Queue(self):
        return self.model.Queue

    @property
    def queues(self):
        return self.Queue._default_manager


class QueueManager(ExtendedManager):

    def enabled(self):
        return self.filter(is_enabled=True)

    def add(self, name, exchange=None, exchange_type=None,
            routing_key=None, **options):
        options = serialize(options) if options else None
        return self.create(name=name, exchange=exchange,
                           exchange_type=exchange_type,
                           routing_key=routing_key,
                           options=options)

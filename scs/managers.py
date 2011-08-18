"""scs.managers"""

from __future__ import absolute_import

from anyjson import serialize
from celery import current_app as celery
from celery.utils import gen_unique_id
from djcelery.managers import ExtendedManager
from kombu.utils import cached_property


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


class AppManager(ExtendedManager):

    def from_json(self, name=None, broker=None):
        return {"name": name, "broker": self.get_broker(**broker)}

    def recreate(self, name=None, broker=None):
        d = self.from_json(name, broker)
        return self.get_or_create(name=d["name"],
                                  defaults={"broker": d["broker"]})[0]

    def instance(self, name=None, broker=None):
        return self.model(**self.from_json(name, broker))

    def get_broker(self, **kwargs):
        if kwargs["hostname"]:
            return self.Brokers.get_or_create(**kwargs)[0]

    def add(self, name=None, hostname=None, port=None, userid=None,
            password=None, virtual_host=None):
        broker = None
        if hostname:
            broker = self.get_broker(hostname=hostname, port=port,
                                     userid=userid, password=password,
                                     virtual_host=virtual_host)
        return self.get_or_create(name=name, defaults={"broker": broker})[0]

    def get_default(self):
        return self.get_or_create(name="scs")[0]

    @cached_property
    def Brokers(self):
        return self.model.Broker._default_manager


class NodeManager(ExtendedManager):

    def enabled(self):
        return self.filter(is_enabled=True)

    def _maybe_queues(self, queues):
        if isinstance(queues, basestring):
            queues = queues.split(",")
        return [(queue.name if isinstance(queue, Queue) else queue)
                    for queue in queues]

    def add(self, nodename=None, queues=None, max_concurrency=1,
            min_concurrency=1, broker=None, app=None):
        node = self.create(name=nodename or gen_unique_id(),
                           max_concurrency=max_concurrency,
                           min_concurrency=min_concurrency,
                           app=app)
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

    def _action(self, nodename, action, *args, **kwargs):
        node = self.get(name=nodename)
        getattr(node, action)(*args, **kwargs)
        return node

    def remove(self, nodename):
        return self._action(nodename, "delete")

    def enable(self, nodename):
        return self._action(nodename, "enable")

    def disable(self, nodename):
        return self._action(nodename, "disable")

    def remove_queue_from_nodes(self, queue, **query):
        nodes = []
        for node in self.filter(**query).iterator():
            if queue in node.queues:
                node.queues.remove(queue)
                node.save()
                nodes.append(node)
        return nodes

    def add_queue_to_nodes(self, queue, **query):
        nodes = []
        for node in self.filter(**query).iterator():
            node.queues.add(queue)
            node.save()
            nodes.append(node)
        return nodes


class QueueManager(ExtendedManager):

    def enabled(self):
        return self.filter(is_enabled=True)

    def _add(self, name, **declaration):
        return self.get_or_create(name=name, defaults=declaration)[0]

    def add(self, name, exchange=None, exchange_type=None,
            routing_key=None, **options):
        options = serialize(options) if options else None
        return self._add(name, exchange=exchange, exchange_type=exchange_type,
                               routing_key=routing_key, options=options)

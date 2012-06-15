"""cyme.managers

- These are the managers for our models in :mod:`cyme.models`.

- They are not to be used directly, but accessed through
  the ``objects`` attribute of a Model.


"""

from __future__ import absolute_import

from anyjson import serialize
from celery import current_app as celery
from djcelery.managers import ExtendedManager

from cyme.utils import cached_property, uuid


class BrokerManager(ExtendedManager):

    def get_default(self):
        return self.get_or_create(url=self.default_url)[0]

    @property
    def default_url(self):
        return celery.broker_connection().as_uri()


class AppManager(ExtendedManager):

    def from_json(self, name=None, broker=None):
        return {'name': name, 'broker': self.get_broker(broker)}

    def recreate(self, name=None, broker=None, arguments=None,
            extra_config=None):
        d = self.from_json(name, broker)
        return self.get_or_create(name=d['name'],
                                  defaults={'broker': d['broker'],
                                            'arguments': arguments,
                                            'extra_config': extra_config})[0]

    def instance(self, name=None, broker=None):
        return self.model(**self.from_json(name, broker))

    def get_broker(self, url):
        return self.Brokers.get_or_create(url=url)[0]

    def add(self, name=None, broker=None, arguments=None, extra_config=None):
        broker = self.get_broker(broker) if broker else None
        return self.get_or_create(name=name, defaults={
                'broker': broker,
                'arguments': arguments,
                'extra_config': extra_config})[0]

    def get_default(self):
        return self.get_or_create(name='cyme')[0]

    @cached_property
    def Brokers(self):
        return self.model.Broker._default_manager


class InstanceManager(ExtendedManager):

    def enabled(self):
        return self.filter(is_enabled=True)

    def _maybe_queues(self, queues):
        if isinstance(queues, basestring):
            queues = queues.split(',')
        return [(queue.name if isinstance(queue, self.model.Queue) else queue)
                    for queue in queues]

    def add(self, name=None, queues=None, max_concurrency=1,
            min_concurrency=1, broker=None, pool=None, app=None,
            arguments=None, extra_config=None):
        instance = self.create(name=name or uuid(),
                               max_concurrency=max_concurrency,
                               min_concurrency=min_concurrency,
                               pool=pool,
                               app=app,
                               arguments=arguments,
                               extra_config=extra_config)
        needs_save = False
        if queues:
            instance.queues = self._maybe_queues(queues)
            needs_save = True
        if broker:
            instance._broker = broker
            needs_save = True
        if needs_save:
            instance.save()
        return instance

    def _action(self, name, action, *args, **kwargs):
        instance = self.get(name=name)
        getattr(instance, action)(*args, **kwargs)
        return instance

    def remove(self, name):
        return self._action(name, 'delete')

    def enable(self, name):
        return self._action(name, 'enable')

    def disable(self, name):
        return self._action(name, 'disable')

    def remove_queue_from_instances(self, queue, **query):
        instances = []
        for instance in self.filter(**query).iterator():
            if queue in instance.queues:
                instance.queues.remove(queue)
                instance.save()
                instances.append(instance)
        return instances

    def add_queue_to_instances(self, queue, **query):
        instances = []
        for instance in self.filter(**query).iterator():
            instance.queues.add(queue)
            instance.save()
            instances.append(instance)
        return instances


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

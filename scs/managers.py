from celery.utils import gen_unique_id

from django.db import models

from djcelery.managers import ExtendedManager

from scs.utils import maybe_list


class NodeManager(ExtendedManager):

    def active(self):
        return self.filter(is_active=True)

    def _maybe_queues(self, queues):
        acc = []
        Queue = self.Queue
        for queue in queues:
            if not isinstance(queue, Queue):
                queue, _ = Queue._default_manager.get_or_create(name=queue)
            acc.append(queue)
        return acc

    def add(self, nodename=None, queues=None, concurrency=None):
        nodename = nodename or gen_unique_id()

        node = self.create(name=nodename or gen_unique_id(),
                           concurrency=concurrency)
        if queues:
            node.queues = self._maybe_queues(queues)
            node.save()
        return node

    def remove(self, nodename):
        self.get(name=nodename).delete()
        # ... stop node

    def add_queue(self, name, nodenames=None):
        nodenames = maybe_list(nodenames)
        queue, _ = self.queues.update_or_create(name=name,
                        defaults={"is_default": nodenames is None})
        if nodenames:
            self._add_queue_to_nodes(queue, name__in=nodenames)
        else:
            self._remove_queue_from_nodes(queue)
            queue.is_default = True
            queue.save()

    def remove_queue(self, name, nodenames=None):
        nodenames = maybe_list(nodenames)
        queue = self.queues.get(name=name)
        if nodenames:
            self._remove_queue_from_nodes(queue, name__in=nodenames)
        else:
            queue.is_default = False
            queue.save()
        # ... broadcast cancel queue event

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

    def active(self):
        return self.filter(is_active=True)

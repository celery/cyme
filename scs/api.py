from piston.handler import BaseHandler

from kombu.utils import cached_property

from scs.agent import Cluster
from scs.models import Node, Queue


class QueueHandler(BaseHandler):
    allowed_methods = ("GET", "POST", "PUT", "DELETE")
    fields = ("name", "exchange", "exchange_type",
              "routing_key", "options")
    exclude = ("id", )
    model = Queue

    def read(self, request, name=None):
        if name:
            return self.model._default_manager.get(name=name)
        return self.model._default_manager.all()

    def create(self, request, name):
        return self.model._default_manager.create(name=name,
                exchange=request.POST.get("exchange"),
                exchange_type=request.POST.get("exchange_type"),
                routing_key=request.POST.get("routing_key"),
                options=request.POST.get("options"))

    def delete(self, request, name):
        q = self.model._default_manager.get(name=name)
        q.delete()
        return q


class NodeHandler(BaseHandler):
    allowed_methods = ("GET", "POST", "PUT", "DELETE")
    fields = ("name", "max_concurrency", "min_concurrency", "is_enabled",
              ("queues", QueueHandler.fields))
    exclude = ("id", )
    model = Node
    cluster = Cluster

    def read(self, request, nodename=None):
        if nodename:
            return self.model._default_manager.get(name=nodename)
        return self.model._default_manager.all()

    def create(self, request, nodename=None):
        max_concurrency = request.POST.get("max_concurrency")
        min_concurrency = request.POST.get("min_concurrency")
        if max_concurrency:
            max_concurrency = int(max_concurrency)
        if min_concurrency:
            min_concurrency = int(min_concurrency)
        return self.cluster.add(nodename,
                                queues=request.POST.get("queues"),
                                max_concurrency=max_concurrency,
                                min_concurrency=min_concurrency)

    def update(self, request, nodename):
        return self.model._default_manager.modify(nodename,
                        queues=request.POST.get("queues"),
                        concurrency=request.POST.get("concurrency"))

    def delete(self, request, nodename):
        return self.cluster.remove(nodename)

    @cached_property
    def cluster(self):
        return self.Cluster()

from piston.handler import BaseHandler

from scs.models import Node


class NodeHandler(BaseHandler):
    allowed_methods = ("GET", "POST", "PUT", "DELETE")
    fields = ("name", "concurrency", "is_active",
              ("queues", ("name", "is_default")))
    exclude = ("id", )
    model = Node

    def read(self, request, nodename=None):
        if nodename:
            return self.model._default_manager.get(name=nodename)
        return self.model._default_manager.all()

    def create(self, request, nodename=None):
        return self.model._default_manager.add(nodename,
                        queues=request.POST.get("queues"),
                        concurrency=request.POST.get("concurrency"))

    def update(self, request, nodename):
        return self.model._default_manager.modify(nodename,
                        queues=request.POST.get("queues"),
                        concurrency=request.POST.get("concurrency"))

    def delete(self, request, nodename):
        return self.model._default_manager.get(name=nodename).delete()

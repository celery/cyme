from scs.models import Node
from scs.supervisor import supervisor


class Cluster(object):
    Nodes = Node._default_manager
    supervisor = supervisor

    def get(self, nodename):
        return self.Nodes.get(name=nodename)

    def add(self, nodename=None, queues=None, concurrency=None):
        node = self.Nodes.add(nodename, queues, concurrency)
        self.supervisor.request_update(node)
        return node

    def modify(self, nodename, queues=None, concurrency=None):
        node = self.Nodes.modify(nodename, queues, concurrency)
        self.supervisor.request_update(node)
        return node

    def remove(self, nodename):
        node = self.Nodes.remove(nodename)
        self.supervisor.request_update(node)
        return node

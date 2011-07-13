from scs.models import Node
from scs.supervisor import supervisor


class Cluster(object):
    Nodes = Node._default_manager
    supervisor = supervisor

    def get(self, nodename):
        return self.Nodes.get(name=nodename)

    def add(self, nodename=None, queues=None,
            max_concurrency=1, min_concurrency=1):
        node = self.Nodes.add(nodename, queues, max_concurrency,
                                                min_concurrency)
        self.supervisor.verify([node]).wait()
        return node

    def modify(self, nodename, queues=None, max_concurrency=None,
                                            min_concurrency=None):
        node = self.Nodes.modify(nodename, queues, max_concurrency,
                                                   min_concurrency)
        self.supervisor.verify([node]).wait()
        return node

    def remove(self, nodename):
        node = self.Nodes.remove(nodename)
        self.supervisor.stop([node]).wait()
        return node

    def restart(self, nodename):
        self.supervisor.restart([self.get(nodename)]).wait()

    def enable(self, nodename):
        self.supervisor.verify([self.Nodes.enable(nodename)]).wait()

    def disable(self, nodename):
        self.supervisor.verify([self.Nodes.disable(nodename)]).wait()


cluster = Cluster()

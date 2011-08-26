"""cyme.agent.managers

- Contains the :class:`LocalNodeManager` instance,
  which is the preferred API used to control and manage worker nodes handled
  by this agent.  I.e. it can be used to do synchronous actions that
  don't return until the supervisor has performed them.

"""

from __future__ import absolute_import

from .supervisor import supervisor as sup

from ..models import Broker, Node
from ..utils import force_list


class LocalNodeManager(object):
    Nodes = Node._default_manager
    Brokers = Broker._default_manager

    def get(self, nodename):
        return self.Nodes.get(name=nodename)

    def add(self, nodename=None, queues=None,
            max_concurrency=1, min_concurrency=1, broker=None,
            pool=None, app=None, nowait=False, **kwargs):
        broker = self.Brokers.get_or_create(url=broker)[0] if broker else None
        node = self.Nodes.add(nodename, queues, max_concurrency,
                              min_concurrency, broker, pool, app)
        return self.maybe_wait(sup.verify, node, nowait)

    def remove(self, nodename, nowait=False):
        return self.maybe_wait(sup.shutdown,
                               self.Nodes.remove(nodename), nowait)

    def restart(self, nodename, nowait=False):
        return self.maybe_wait(sup.restart, self.get(nodename), nowait)

    def enable(self, nodename, nowait=False):
        return self.maybe_wait(sup.verify,
                               self.Nodes.enable(nodename), nowait)

    def disable(self, nodename, nowait=False):
        return self.maybe_wait(sup.verify,
                               self.Nodes.disable(nodename), nowait)

    def add_consumer(self, name, queue, nowait=False):
        return self.maybe_wait(sup.verify,
                self.get(name).add_queue_eventually(queue), nowait)

    def cancel_consumer(self, name, queue, nowait=False):
        return self.maybe_wait(sup.verify,
                self.Nodes.remove_queue_from_nodes(queue, name=name), nowait)

    def remove_queue(self, queue, nowait=False):
        return self.maybe_wait(sup.verify,
                self.Nodes.remove_queue_from_nodes(queue), nowait)

    def maybe_wait(self, fun, nodes, nowait):
        if nodes:
            g = fun(force_list(nodes))
            nowait and g.wait()
        return nodes
local_nodes = LocalNodeManager()

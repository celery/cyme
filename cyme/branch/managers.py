"""cyme.branch.managers

- Contains the :class:`LocalInstanceManager` instance,
  which is the preferred API used to control and manage worker
  instances handled by this branch.  I.e. it can be used to do synchronous
  actions that don't return until the supervisor has performed them.

"""

from __future__ import absolute_import

from .supervisor import supervisor as sup

from ..models import Broker, Instance
from ..utils import force_list


class LocalInstanceManager(object):
    Instances = Instance._default_manager
    Brokers = Broker._default_manager

    def get(self, name):
        return self.Instances.get(name=name)

    def add(self, name=None, queues=None,
            max_concurrency=1, min_concurrency=1, broker=None,
            pool=None, app=None, arguments=None, extra_config=None,
            nowait=False, **kwargs):
        broker = self.Brokers.get_or_create(url=broker)[0] if broker else None
        instance = self.Instances.add(name, queues, max_concurrency,
                                  min_concurrency, broker, pool, app,
                                  arguments, extra_config)
        return self.maybe_wait(sup.verify, instance, nowait)

    def remove(self, name, nowait=False):
        return self.maybe_wait(sup.shutdown,
                               self.Instances.remove(name), nowait)

    def restart(self, name, nowait=False):
        return self.maybe_wait(sup.restart, self.get(name), nowait)

    def enable(self, name, nowait=False):
        return self.maybe_wait(sup.verify,
                               self.Instances.enable(name), nowait)

    def disable(self, name, nowait=False):
        return self.maybe_wait(sup.verify,
                               self.Instances.disable(name), nowait)

    def add_consumer(self, name, queue, nowait=False):
        return self.maybe_wait(sup.verify,
                self.get(name).add_queue_eventually(queue), nowait)

    def cancel_consumer(self, name, queue, nowait=False):
        return self.maybe_wait(sup.verify,
                self.Instances.remove_queue_from_instances(queue, name=name),
                nowait)

    def remove_queue(self, queue, nowait=False):
        return self.maybe_wait(sup.verify,
                self.Instances.remove_queue_from_instances(queue), nowait)

    def maybe_wait(self, fun, instances, nowait):
        if instances:
            g = fun(force_list(instances))
            nowait and g.wait()
        return instances
local_instances = LocalInstanceManager()

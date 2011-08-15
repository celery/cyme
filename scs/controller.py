"""scs.controller"""

from __future__ import absolute_import

from cl import Agent
from cl.models import ModelActor
from cl.utils import flatten
from celery import current_app as celery
from kombu import Exchange
from kombu.utils import cached_property

from django.db.models.signals import post_delete, post_save

from . import signals
from .models import Node, Queue
from .state import state
from .thread import gThread


class SCSModelActor(ModelActor):

    def __init__(self, connection=None, *args, **kwargs):
        if not connection:
            connection = celery.broker_connection()
        super(SCSModelActor, self).__init__(connection, *args, **kwargs)

        # retry publishing messages by default if running as scs-agent.
        self.retry = state.is_agent
        self.default_fields = {"agent_id": self.id}


class NodeActor(SCSModelActor):
    model = Node
    exchange = Exchange("scs.nodes")
    sigmap = {"on_create": signals.node_started.connect,
              "on_delete": signals.node_stopped.connect}

    class state:

        def all(self):
            return [node.name for node in Node.objects.all()]

        def get(self, name):
            return self.agent.get(name).as_dict()

        def add(self, name=None, **kwargs):
            return self.agent.add(name, **kwargs).as_dict()

        def remove(self, name):
            self.agent.remove(name)
            return True

        def restart(self, name):
            self.agent.restart(name)
            return True

        def enable(self, name):
            self.agent.enable(name)
            return True

        def disable(self, name):
            self.agent.disable(name)
            return "ok"

        def add_consumer(self, name, queue):
            self.agent.add_consumer(name, queue)
            return "ok"

        def cancel_consumer(self, name, queue):
            self.agent.cancel_consumer(name, queue)
            return "ok"

        def remove_queue_from_all(self, queue):
            return [node.name for node in
                        Node.objects.remove_queue_from_nodes(queue)]

        def autoscale(self, name, max=None, min=None):
            node = Node.objects.get(name=name)
            node.autoscale(max=max, min=min)
            return [node.max_concurrency, node.min_concurrency]

        def consuming_from(self, name):
            return self.agent.get(name).consuming_from()

        def stats(self, name):
            return self.agent.get(name).stats()

        @cached_property
        def agent(self):
            from .agent import cluster
            return cluster

    def get(self, name, **kw):
        return self.send("get", {"name": name}, to=name, **kw)

    def all(self):
        return flatten(self.scatter("all"))

    def add(self, name=None, nowait=False, **kwargs):
        return self.throw("add", dict({"name": name}, **kwargs),
                          nowait=nowait)

    def remove(self, name, **kw):
        return self.send("remove", {"name": name}, to=name, **kw)

    def restart(self, name, **kw):
        return self.send("restart", {"name": name}, to=name, **kw)

    def enable(self, name, **kw):
        return self.send("enable", args={"name": name}, to=name, **kw)

    def disable(self, name, **kw):
        return self.send("disable", args={"name": name}, to=name, **kw)

    def add_consumer(self, name, queue, **kw):
        return self.send("add_consumer",
                         {"name": name, "queue": queue}, to=name, **kw)

    def cancel_consumer(self, name, queue, **kw):
        return self.send("cancel_consumer",
                         {"name": name, "queue": queue}, to=name, **kw)

    def remove_queue_from_all(self, queue, **kw):
        return flatten(self.scatter("remove_queue_from_all",
                                    {"queue": queue}, **kw) or [])

    def autoscale(self, name, max=None, min=None, **kw):
        return self.send("autoscale",
                         {"name": name, "min": min, "max": max},
                         to=name, **kw)

    def consuming_from(self, name, **kw):
        return self.send("consuming_from", {"name": name}, to=name, **kw)

    def stats(self, name, **kw):
        return self.send("stats", {"name": name}, to=name, **kw)
nodes = NodeActor()


class QueueActor(SCSModelActor):
    model = Queue
    exchange = Exchange("scs.queues")
    sigmap = {"on_create": lambda f: post_save.connect(f, sender=Queue),
              "on_delete": lambda f: post_delete.connect(f, sender=Queue)}

    class state:

        def all(self):
            return [q.name for q in Queue.objects.all()]

        def get(self, name):
            try:
                return Queue.objects.get(name=name).as_dict()
            except Queue.DoesNotExist:
                raise KeyError(name)

        def add(self, name, **declaration):
            return Queue.objects._add(name, **declaration).as_dict()

        def delete(self, name):
            Queue.objects.filter(name=name).delete()
            return "ok"

    def all(self):
        return flatten(self.scatter("all"))

    def get(self, name):
        try:
            # see if we have the queue locally.
            return self.state.get(name)
        except KeyError:
            # if not, ask the agents.
            return self.send("get", {"name": name}, to=name)

    def add(self, name, **decl):
        return self.throw("add", dict({"name": name}, **decl))

    def delete(self, name, **kw):
        nodes.remove_queue_from_all(name, nowait=True)
        return self.send("delete", {"name": name}, to=name, **kw)
queues = QueueActor()


class Controller(Agent, gThread):
    actors = [NodeActor(), QueueActor()]
    connect_max_retries = celery.conf.BROKER_CONNECTION_MAX_RETRIES

    def __init__(self, *args, **kwargs):
        Agent.__init__(self, *args, **kwargs)
        gThread.__init__(self)

    def on_connection_revived(self):
        state.on_broker_revive()

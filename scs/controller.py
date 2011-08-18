"""scs.controller"""

from __future__ import absolute_import

from cl import Actor
from cl.models import ModelActor
from cl.presence import AwareAgent
from cl.utils import flatten, first_or_raise
from celery import current_app as celery
from kombu import Exchange
from kombu.utils import cached_property

from django.db.models.signals import post_delete, post_save

from . import conf
from . import signals
from . import metrics
from .models import App, Node, Queue
from .state import state
from .thread import gThread


def _init_actor(actor, base, connection=None, *args, **kwargs):
    if not connection:
        connection = celery.broker_connection()
    super(base, actor).__init__(connection, *args, **kwargs)

    # retry publishing messages by default if running as scs-agent.
    actor.retry = state.is_agent
    actor.default_fields = {"agent_id": actor.id}


class SCSActor(Actor):

    def __init__(self, *args, **kwargs):
        _init_actor(self, SCSActor, *args, **kwargs)


class SCSModelActor(ModelActor):

    def __init__(self, *args, **kwargs):
        _init_actor(self, SCSModelActor, *args, **kwargs)


class AppActor(SCSActor):
    name = "App"
    types = ("scatter", )
    _cache = {}

    class state:

        def all(self):
            return [app.name for app in App.objects.all()]

        def add(self, name, **broker):
            return App.objects.add(name, **broker).as_dict()

        def get(self, name):
            try:
                return App.objects.get(name=name).as_dict()
            except App.DoesNotExist:
                raise self.Next()

        def metrics(self):
            return {"load_average": metrics.load_average(),
                    "disk_use": metrics.df(conf.SCS_INSTANCE_DIR).capacity}

    def all(self):
        return flatten(self.scatter("all"))

    def add(self, name, **broker):
        self.scatter("add", dict({"name": name}, **broker), nowait=True)
        return self.state.add(name, **broker)

    def metrics(self, name=None):
        return list(self.scatter("metrics"))

    def get(self, name=None):
        if not name:
            return App.objects.get_default()
        if name not in self._cache:
            app = self._get(name)
            if state.is_agent:
                # copy app to local
                self._cache[name] = App.objects.recreate(**app)
            else:
                self._cache[name] = App.objects.instance(**app)
        return self._cache[name]

    def _get(self, name):
        try:
            return self.state.get(name)
        except self.Next:
            return first_or_raise(self.scatter("get", {"name": name}))
apps = AppActor()


class NodeActor(SCSModelActor):
    model = Node
    exchange = Exchange("scs.Node")
    sigmap = {"on_create": signals.node_started.connect,
              "on_delete": signals.node_stopped.connect}
    default_timeout = 10

    class state:

        def all(self, app=None):
            return [node.name for node in
                        Node.objects.filter(app=apps.get(app))]

        def get(self, name, app=None):
            print("+int GET %r" % (name, ))
            x = Node.objects.get(name=name)
            print("-int GET")
            return x.as_dict()

        def add(self, name=None, app=None, **kwargs):
            return self.agent.add(name, app=apps.get(app), **kwargs).as_dict()

        def remove(self, name, app=None):
            return self.agent.remove(name) and "ok"

        def restart(self, name, app=None):
            return self.agent.restart(name) and "ok"

        def enable(self, name, app=None):
            return self.agent.enable(name) and "ok"

        def disable(self, name, app=None):
            return self.agent.disable(name) and "ok"

        def add_consumer(self, name, queue):
            return self.agent.add_consumer(name, queue) and "ok"

        def cancel_consumer(self, name, queue):
            return self.agent.cancel_consumer(name, queue) and "ok"

        def remove_queue_from_all(self, queue):
            return [node.name for node in
                        Node.objects.remove_queue_from_nodes(queue)]

        def autoscale(self, name, max=None, min=None):
            node = self.agent.get(name=name)
            node.autoscale(max=max, min=min)
            return {"max": node.max_concurrency, "min": node.min_concurrency}

        def consuming_from(self, name):
            return self.agent.get(name).consuming_from()

        def stats(self, name):
            return self.agent.get(name).stats()

        @cached_property
        def agent(self):
            from .agent import cluster
            return cluster

    def get(self, name, app=None, **kw):
        print("+ext GET %r" % (name, ))
        x = self.send("get", {"name": name, "app": app}, to=name, **kw)
        print("-exc GET")
        return x

    def all(self, app=None):
        return flatten(self.scatter("all", {"app": app}))

    def add(self, name=None, app=None, nowait=False, **kwargs):
        return self.throw("add", dict({"name": name, "app": app}, **kwargs),
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
                         {"name": name, "min": min, "max": max}, to=name, **kw)

    def consuming_from(self, name, **kw):
        return self.send("consuming_from", {"name": name}, to=name, **kw)

    def stats(self, name, **kw):
        return self.send("stats", {"name": name}, to=name, **kw)
nodes = NodeActor()


class QueueActor(SCSModelActor):
    model = Queue
    exchange = Exchange("scs.Queue")
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
        #try:
        #    # see if we have the queue locally.
        #    return self.state.get(name)
        #except KeyError:
        #    # if not, ask the agents.
        return self.send("get", {"name": name}, to=name)

    def add(self, name, **decl):
        return self.throw("add", dict({"name": name}, **decl))

    def delete(self, name, **kw):
        nodes.remove_queue_from_all(name, nowait=True)
        return self.send("delete", {"name": name}, to=name, **kw)
queues = QueueActor()

ControllerBase = AwareAgent

class Controller(ControllerBase, gThread):
    actors = [AppActor(), NodeActor(), QueueActor()]
    connect_max_retries = celery.conf.BROKER_CONNECTION_MAX_RETRIES

    def __init__(self, *args, **kwargs):
        ControllerBase.__init__(self, *args, **kwargs)
        gThread.__init__(self)

    def on_awake(self):
        # bind global actors to this agent,
        # so precense can be used.
        for actor in (apps, nodes, queues):
            actor.agent = self

    def on_connection_revived(self):
        state.on_broker_revive()

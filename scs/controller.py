"""scs.controller"""

from __future__ import absolute_import

from functools import partial

from cl import Actor
from cl.presence import AwareAgent, AwareActorMixin, announce_after
from cl.utils import flatten, first_or_raise, shortuuid
from celery import current_app as celery
from kombu import Exchange

from . import conf
from . import metrics
from .models import App, Node, Queue
from .state import state
from .thread import gThread
from .utils import cached_property

ControllerBase = AwareAgent


class SCSActor(Actor, AwareActorMixin):
    meta_lookup_section = None

    def __init__(self, connection=None, *args, **kwargs):
        if not connection:
            connection = celery.broker_connection()
        Actor.__init__(self, connection, *args, **kwargs)

        # retry publishing messages by default if running as scs-agent.
        self.retry = state.is_agent
        self.default_fields = {"actor_id": self.id}


class AppActor(SCSActor):
    name = "App"
    types = ("scatter", )
    exchange = Exchange("xscs.App")
    _cache = {}

    class state:

        def all(self):
            return [app.name for app in App.objects.all()]

        def add(self, name, **broker):
            return App.objects.add(name, **broker).as_dict()

        def delete(self, name):
            return App.objects.filter(name=name).delete() and "ok"

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

    def delete(self, name, **kw):
        self._cache.pop(name, None)
        return list(self.scatter("delete", dict({"name": name}, **kw)))

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


class NodeActor(SCSActor):
    name = "Node"
    exchange = Exchange("xscs.Node")
    default_timeout = 60
    types = ("direct", "scatter", "round-robin")
    meta_lookup_section = "nodes"

    class state:

        def all(self, app=None):
            fun = Node.objects.all
            if app:
                fun = partial(Node.objects.filter, app=apps.get(app))
            return [node.name for node in fun()]

        def get(self, name, app=None):
            try:
                x = Node.objects.get(name=name)
            except Node.DoesNotExist:
                raise self.Next()
            return x.as_dict()

        @announce_after
        def add(self, name=None, app=None, **kwargs):
            return self.scs.add(name, app=apps.get(app), **kwargs).as_dict()

        @announce_after
        def remove(self, name, app=None):
            return self.scs.remove(name) and "ok"

        def restart(self, name, app=None):
            return self.scs.restart(name) and "ok"

        def enable(self, name, app=None):
            return self.scs.enable(name) and "ok"

        def disable(self, name, app=None):
            return self.scs.disable(name) and "ok"

        def add_consumer(self, name, queue):
            return self.scs.add_consumer(name, queue) and "ok"

        def cancel_consumer(self, name, queue):
            return self.scs.cancel_consumer(name, queue) and "ok"

        def remove_queue_from_all(self, queue):
            return [node.name for node in
                        Node.objects.remove_queue_from_nodes(queue)]

        def autoscale(self, name, max=None, min=None):
            node = self.scs.get(name)
            node.autoscale(max=max, min=min)
            return {"max": node.max_concurrency, "min": node.min_concurrency}

        def consuming_from(self, name):
            return self.scs.get(name).consuming_from()

        def stats(self, name):
            return self.scs.get(name).stats()

        @cached_property
        def scs(self):
            from .agent import cluster
            return cluster

    def get(self, name, app=None, **kw):
        return self.send_to_able("get",
                                 {"name": name, "app": app}, to=name, **kw)

    def all(self, app=None):
        return flatten(self.scatter("all", {"app": app}))

    def add(self, name=None, app=None, nowait=False, **kwargs):
        return self.throw("add", dict({"name": name, "app": app}, **kwargs),
                          nowait=nowait)

    def remove(self, name, **kw):
        return self.send_to_able("remove", {"name": name}, to=name, **kw)

    def restart(self, name, **kw):
        return self.send_to_able("restart", {"name": name}, to=name, **kw)

    def enable(self, name, **kw):
        return self.send_to_able("enable", args={"name": name}, to=name, **kw)

    def disable(self, name, **kw):
        return self.send_to_able("disable", args={"name": name}, to=name, **kw)

    def add_consumer(self, name, queue, **kw):
        return self.send_to_able("add_consumer",
                                 {"name": name, "queue": queue}, to=name, **kw)

    def cancel_consumer(self, name, queue, **kw):
        return self.send_to_able("cancel_consumer",
                                 {"name": name, "queue": queue}, to=name, **kw)

    def remove_queue_from_all(self, queue, **kw):
        return flatten(self.scatter("remove_queue_from_all",
                                    {"queue": queue}, **kw) or [])

    def autoscale(self, name, max=None, min=None, **kw):
        return self.send_to_able("autoscale",
                         {"name": name, "min": min, "max": max}, to=name, **kw)

    def consuming_from(self, name, **kw):
        return self.send_to_able("consuming_from",
                                 {"name": name}, to=name, **kw)

    def stats(self, name, **kw):
        return self.send_to_able("stats", {"name": name}, to=name, **kw)

    @property
    def meta(self):
        return {"nodes": self.state.all()}
nodes = NodeActor()


class QueueActor(SCSActor):
    name = "Queue"
    exchange = Exchange("xscs.Queue")
    types = ("direct", "scatter", "round-robin")
    default_timeout = 2
    meta_lookup_section = "queues"

    class state:

        def all(self):
            return [q.name for q in Queue.objects.all()]

        def get(self, name):
            try:
                return Queue.objects.get(name=name).as_dict()
            except Queue.DoesNotExist:
                raise KeyError(name)

        @announce_after
        def add(self, name, **declaration):
            return Queue.objects._add(name, **declaration).as_dict()

        @announce_after
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
            return self.send_to_able("get", {"name": name}, to=name)

    def add(self, name, nowait=False, **decl):
        return self.throw("add", dict({"name": name}, **decl), nowait=nowait)

    def delete(self, name, **kw):
        nodes.remove_queue_from_all(name, nowait=True)
        return self.send_to_able("delete", {"name": name}, to=name, **kw)

    @property
    def meta(self):
        return {"queues": self.state.all()}
queues = QueueActor()


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

    @property
    def logger_name(self):
        return '#'.join([self.__class__.__name__, shortuuid(self.id)])

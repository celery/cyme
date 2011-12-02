"""cyme.branch.controller

- Actors used to manage entities across all branches.

"""

from __future__ import absolute_import

from functools import partial

from cl.presence import AwareActorMixin, announce_after
from cl.utils import flatten, first_or_raise, shortuuid
from celery import current_app as celery
from kombu import Exchange
from kombu.common import uuid

from . import metrics
from . import signals
from .state import state
from .thread import gThread

from .. import conf
from .. import models
from ..utils import cached_property, find_symbol, promise
from ..utils.actors import Actor, AwareAgent


class CymeActor(Actor, AwareActorMixin):
    _announced = set()  # note: global

    def setup(self):
        # retry publishing messages by default if running as cyme-branch.
        self.retry = state.is_branch
        self.default_fields = {"actor_id": self.id}


class ModelActor(CymeActor):
    model = None

    def on_agent_ready(self):
        if self.name not in self._announced:
            self.log.info("%s: %s", self.name_plural,
                    promise(lambda: ", ".join(self.state.all())))
            self._announced.add(self.name)

    def contribute_to_state(self, state):
        state.model = self.model
        state.objects = self.model._default_manager
        return Actor.contribute_to_state(self, state)

    @cached_property
    def name(self):
        return unicode(self.model._meta.verbose_name.capitalize())

    @cached_property
    def name_plural(self):
        return unicode(self.model._meta.verbose_name_plural).capitalize()


class Branch(CymeActor):
    exchange = Exchange("cyme.Branch")
    default_timeout = 60
    types = ("direct", "scatter", "round-robin")
    meta_lookup_section = "this"

    def on_agent_ready(self):
        if self.name not in self._announced:
            self.log.info("Actor ready", self.name)
        self._announced.add(self.name)

    class state:

        def id(self):
            return self.agent.branch.id

        def url(self):
            return self.agent.branch.httpd.thread.url

        def about(self):
            return self.agent.branch.about()

        def shutdown(self, id):
            if id in [self.id(), "*"]:
                assert state.is_branch
                self.log.warn("Shutdown requested from remote.")
                raise SystemExit()
            raise self.Next()

    def all(self, **kw):
        return flatten(self.scatter("id", **kw))

    def get(self, id, **kw):
        return self.send_to_able("about", to=id, **kw)

    def url(self, id=None, **kw):
        if id:
            return self.send_to_able("url", to=id, **kw)
        return flatten(self.scatter("url", **kw))

    def shutdown(self, id):
        return self.send_to_able("shutdown", {"id": id}, to=id, nowait=True)

    def shutdown_all(self):
        return self.scatter("shutdown", {"id": "*"}, nowait=True)

    @property
    def meta(self):
        return {"this": [self.state.id()]}
branches = Branch()


class App(ModelActor):
    """Actor for managing the app model."""
    model = models.App
    types = ("scatter", )
    exchange = Exchange("cyme.App")
    _cache = {}

    class state:

        def all(self):
            return [app.name for app in self.objects.all()]

        def add(self, name, broker=None, arguments=None, extra_config=None):
            return self.objects.add(name, broker=broker,
                                          arguments=arguments,
                                          extra_config=extra_config).as_dict()

        def delete(self, name):
            return self.objects.filter(name=name).delete() and "ok"

        def get(self, name):
            try:
                return self.objects.get(name=name).as_dict()
            except self.model.DoesNotExist:
                raise self.Next()

        def metrics(self):
            instance_dir = str(conf.CYME_INSTANCE_DIR)
            return {"load_average": metrics.load_average(),
                    "disk_use": metrics.df(instance_dir).capacity}

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
        objects = self.state.objects
        if not name:
            return objects.get_default()
        if name not in self._cache:
            app = self._get(name)
            if not app:
                raise KeyError(name)
            self._cache[name] = objects.recreate(**app)

        return self._cache[name]

    def _get(self, name):
        try:
            return self.state.get(name)
        except self.Next:
            return first_or_raise(self.scatter("get", {"name": name},
                                               propagate=False),
                                  self.NoRouteError(name))
apps = App()


class Instance(ModelActor):
    """Actor for managing the Instance model."""
    model = models.Instance
    exchange = Exchange("cyme.Instance")
    default_timeout = 60
    types = ("direct", "scatter", "round-robin")
    meta_lookup_section = "instances"

    class state:

        def all(self, app=None):
            fun = self.objects.all
            if app:
                fun = partial(self.objects.filter, app=apps.get(app))
            return [instance.name for instance in fun()]

        def get(self, name, app=None):
            try:
                x = self.objects.get(name=name)
            except self.model.DoesNotExist:
                raise self.Next()
            return x.as_dict()

        @announce_after
        def add(self, name=None, app=None, **kwargs):
            return self.local.add(name, app=apps.get(app), **kwargs).as_dict()

        @announce_after
        def remove(self, name, app=None):
            return self.local.remove(name) and "ok"

        def restart(self, name, app=None):
            return self.local.restart(name) and "ok"

        def enable(self, name, app=None):
            return self.local.enable(name) and "ok"

        def disable(self, name, app=None):
            return self.local.disable(name) and "ok"

        def add_consumer(self, name, queue):
            return self.local.add_consumer(name, queue) and "ok"

        def cancel_consumer(self, name, queue):
            return self.local.cancel_consumer(name, queue) and "ok"

        def remove_queue_from_all(self, queue):
            return [instance.name for instance in
                        self.objects.remove_queue_from_instances(queue)]

        def autoscale(self, name, max=None, min=None):
            instance = self.local.get(name)
            instance.autoscale(max=max, min=min)
            return {"max": instance.max_concurrency,
                    "min": instance.min_concurrency}

        def consuming_from(self, name):
            return self.local.get(name).consuming_from()

        def stats(self, name):
            return self.local.get(name).stats()

        @cached_property
        def local(self):
            return find_symbol(self, ".managers.local_instances")

    def get(self, name, app=None, **kw):
        return self.send_to_able("get",
                                 {"name": name, "app": app}, to=name, **kw)

    def all(self, app=None):
        return flatten(self.scatter("all", {"app": app}))

    def add(self, name=None, app=None, nowait=False, **kwargs):
        if nowait:
            name = name if name else uuid()
        ret = self.throw("add", dict({"name": name, "app": app}, **kwargs),
                         nowait=nowait)
        if nowait:
            return {"name": name}
        return ret

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
        return {"instances": self.state.all()}
instances = Instance()


class Queue(ModelActor):
    """Actor for managing the Queue model."""
    model = models.Queue
    exchange = Exchange("cyme.Queue")
    types = ("direct", "scatter", "round-robin")
    default_timeout = 2
    meta_lookup_section = "queues"

    class state:

        def all(self):
            return [q.name for q in self.objects.all()]

        def get(self, name):
            try:
                return self.objects.get(name=name).as_dict()
            except self.model.DoesNotExist:
                raise KeyError(name)

        @announce_after
        def add(self, name, **declaration):
            return self.objects._add(name, **declaration).as_dict()

        @announce_after
        def delete(self, name):
            self.objects.filter(name=name).delete()
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
        instances.remove_queue_from_all(name, nowait=True)
        return self.send_to_able("delete", {"name": name}, to=name, **kw)

    @property
    def meta(self):
        return {"queues": self.state.all()}
queues = Queue()


class Controller(AwareAgent, gThread):
    actors = [Branch(), App(), Instance(), Queue()]
    connect_max_retries = celery.conf.BROKER_CONNECTION_MAX_RETRIES
    extra_shutdown_steps = 2
    _ready_sent = False
    _presence_ready_sent = False

    def __init__(self, *args, **kwargs):
        self.branch = kwargs.pop("branch", None)
        AwareAgent.__init__(self, *args, **kwargs)
        gThread.__init__(self)

    def on_awake(self):
        # bind global actors to this agent,
        # so presence can be used.
        for actor in (branches, apps, instances, queues):
            actor.agent = self

    def on_connection_revived(self):
        state.on_broker_revive()

    def on_consume_ready(self, *args, **kwargs):
        if not self._ready_sent:
            signals.controller_ready.send(sender=self)
            self._ready_sent = True
        super(Controller, self).on_consume_ready()

    def on_iteration(self):
        self.respond_to_ping()

    def on_connection_error(self, exc, interval):
        self.respond_to_ping()
        super(Controller, self).on_connection_error(exc, interval)

    def on_presence_ready(self):
        if not self._presence_ready_sent:
            signals.presence_ready.send(sender=self.presence)
            self._presence_ready_sent = True

    def stop(self):
        self.should_stop = True
        if hasattr(self, "presence") and self.presence.g:
            self.debug("waiting for presence to exit")
            signals.thread_shutdown_step.send(sender=self)
            self.presence.g.wait()
            signals.thread_shutdown_step.send(sender=self)
        super(Controller, self).stop()

    @property
    def logger_name(self):
        return '#'.join([self.__class__.__name__, self._shortid()])

    def _shortid(self):
        if '.' in self.id:
            return shortuuid(self.id) + ".." + self.id[-2:]
        return shortuuid(self.id)

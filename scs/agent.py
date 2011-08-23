"""scs.agent"""

from __future__ import absolute_import

import logging

from celery import current_app as celery
from celery.utils import instantiate
from cl.g import Event
from kombu.utils import gen_unique_id

from . import signals
from . import supervisor
from .models import Broker, Node
from .intsup import gSup
from .state import state
from .thread import gThread
from .utils import setup_logging


class Agent(gThread):
    controller_cls = "scs.controller.Controller"
    httpd_cls = "scs.httpd.HttpServer"
    supervisor_cls = "scs.supervisor.Supervisor"
    httpd = None
    controller = None

    _components_ready = {}
    _ready = False

    def __init__(self, addrport="", id=None, loglevel=logging.INFO,
            logfile=None, without_httpd=False, numc=2, sup_interval=None,
            ready_event=None, **kwargs):
        self.id = id or gen_unique_id()
        if isinstance(addrport, basestring):
            addr, _, port = addrport.partition(":")
            addrport = (addr, int(port) if port else 8000)
        self.addrport = addrport
        self.connection = celery.broker_connection()
        self.without_httpd = without_httpd
        self.logfile = logfile
        self.loglevel = loglevel
        self.numc = numc
        self.ready_event = ready_event
        self.exit_request = Event()
        if not self.without_httpd:
            self.httpd = gSup(instantiate(self.httpd_cls, addrport),
                              signals.httpd_ready)
        self.supervisor = gSup(instantiate(self.supervisor_cls, sup_interval),
                               signals.supervisor_ready)
        self.controllers = [gSup(instantiate(self.controller_cls,
                                   id="%s.%s" % (self.id, i),
                                   connection=self.connection),
                                 signals.controller_ready)
                                for i in xrange(1, numc + 1)]
        c = [self.supervisor] + self.controllers + [self.httpd]
        c = self.components = list(filter(None, c))
        self._components_ready = dict(zip([z.thread for z in c],
                                          [False] * len(c)))
        super(Agent, self).__init__()

    def on_controller_ready(self, sender=None, **kwargs):
        self._component_ready(sender)

    def _component_ready(self, sender=None, **kwargs):
        if not self._ready:
            self._components_ready[sender] = True
            if all(self._components_ready.values()):
                if self.ready_event:
                    self.ready_event.send()
                    self.ready_event = None
                signals.agent_ready.send(sender=self)
                self._ready = True

    def on_ready(self, **kwargs):
        pass

    def prepare_signals(self):
        signals.controller_ready.connect(self._component_ready)
        signals.httpd_ready.connect(self._component_ready)
        signals.supervisor_ready.connect(self._component_ready)
        signals.agent_ready.connect(self.on_ready)

    def run(self):
        state.is_agent = True
        self.prepare_signals()
        setup_logging(self.loglevel, self.logfile)
        self.info("Starting with id %r", self.id)
        [g.start() for g in self.components]
        self.exit_request.wait()

    def stop(self):
        self.exit_request.send(1)
        super(Agent, self).stop()

    def after(self):
        for component in reversed(self.components):
            if self._components_ready[component.thread]:
                try:
                    component.stop()
                except KeyboardInterrupt:
                    pass
                except BaseException, exc:
                    component.error("Error in shutdown: %r", exc)


def maybe_wait(g, nowait):
    not nowait and g.wait()


class Cluster(object):
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
        maybe_wait(self.sup.verify([node]), nowait)
        return node

    def remove(self, nodename, nowait=False):
        node = self.Nodes.remove(nodename)
        maybe_wait(self.sup.shutdown([node]), nowait)
        return node

    def restart(self, nodename, nowait=False):
        node = self.get(nodename)
        maybe_wait(self.sup.restart([node]), nowait)
        return node

    def enable(self, nodename, nowait=False):
        node = self.Nodes.enable(nodename)
        maybe_wait(self.sup.verify([node]), nowait)
        return node

    def disable(self, nodename, nowait=False):
        node = self.Nodes.disable(nodename)
        maybe_wait(self.sup.verify([node]), nowait)
        return node

    def add_consumer(self, name, queue, nowait=False):
        node = self.get(name)
        node.queues.add(queue)
        node.save()
        maybe_wait(self.sup.verify([node]), nowait)
        return node

    def cancel_consumer(self, name, queue, nowait=False):
        nodes = self.Nodes.remove_queue_from_nodes(queue, name=name)
        if nodes:
            maybe_wait(self.sup.verify(nodes), nowait)
        return nodes

    def remove_queue(self, queue, nowait=False):
        nodes = self.Nodes.remove_queue_from_nodes(queue)
        maybe_wait(self.sup.verify(nodes), nowait)
        return nodes

    @property
    def sup(self):
        return supervisor.supervisor
cluster = Cluster()

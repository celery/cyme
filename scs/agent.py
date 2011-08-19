"""scs.agent"""

from __future__ import absolute_import

import logging

from celery import current_app as celery
from celery.utils import instantiate
from kombu.utils import gen_unique_id

from . import signals
from . import supervisor as _sup
from .models import Broker, Node
from .state import state
from .thread import gThread


class Agent(gThread):
    controller_cls = "scs.controller.Controller"
    httpd_cls = "scs.httpd.HttpServer"
    supervisor_cls = "scs.supervisor.Supervisor"
    httpd = None
    controller = None

    _components_ready = {"httpd": False,
                         "controllers": False,
                         "supervisor": False}
    _controllers_ready = 0
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
        if not self.without_httpd:
            self.httpd = instantiate(self.httpd_cls, addrport)
        else:
            self._components_ready["httpd"] = True
        supervisor = _sup.set_current(instantiate(
                            self.supervisor_cls, sup_interval))
        self.controllers = [instantiate(self.controller_cls,
                                   id="%s.%s" % (self.id, i),
                                   connection=self.connection)
                                for i in xrange(1, numc + 1)]
        components = [self.httpd, supervisor] + self.controllers
        self.components = list(filter(None, components))
        super(Agent, self).__init__()

    def on_controller_ready(self, **kwargs):
        self._controllers_ready += 1
        if self._controllers_ready >= self.numc:
            signals.all_controllers_ready.send(sender=self,
                                               controllers=self.controllers)
            signals.controller_ready.disconnect(self.on_controller_ready)

    def _component_ready(self, component):
        if not self._ready:
            self._components_ready[component] = True
            if all(self._components_ready.values()):
                if self.ready_event:
                    self.ready_event.send()
                    self.ready_event = None
                signals.agent_ready.send(sender=self)
                self._ready = True

    def on_all_controllers_ready(self, **kwargs):
        self._component_ready("controllers")
        signals.all_controllers_ready.disconnect(self.on_all_controllers_ready)

    def on_httpd_ready(self, **kwargs):
        self._component_ready("httpd")
        signals.httpd_ready.disconnect(self.on_httpd_ready)

    def on_supervisor_ready(self, **kwargs):
        self._component_ready("supervisor")
        signals.supervisor_ready.disconnect(self.on_supervisor_ready)

    def on_ready(self, **kwargs):
        self.info("all systems go")
        signals.agent_ready.disconnect(self.on_ready)

    def prepare_signals(self):
        signals.controller_ready.connect(self.on_controller_ready)
        signals.httpd_ready.connect(self.on_httpd_ready)
        signals.supervisor_ready.connect(self.on_supervisor_ready)
        signals.all_controllers_ready.connect(self.on_all_controllers_ready)
        signals.agent_ready.connect(self.on_ready)

    def run(self):
        state.is_agent = True
        self.prepare_signals()
        celery.log.setup_logging_subsystem(self.loglevel, self.logfile)
        self.info("Starting with id %r" % (self.id, ))
        [g.start() for g in self.components][-1].wait()


def maybe_wait(g, nowait):
    not nowait and g.wait()


class Cluster(object):
    Nodes = Node._default_manager
    Brokers = Broker._default_manager

    def get(self, nodename):
        return self.Nodes.get(name=nodename)

    def add(self, nodename=None, queues=None,
            max_concurrency=1, min_concurrency=1, hostname=None,
            port=None, userid=None, password=None, virtual_host=None,
            app=None, nowait=False, **kwargs):
        broker = None
        if hostname:
            broker = self.Brokers.get_or_create(
                            hostname=hostname, port=port,
                            userid=userid, password=password,
                            virtual_host=virtual_host)
        node = self.Nodes.add(nodename, queues, max_concurrency,
                              min_concurrency, broker, app)
        maybe_wait(self.sup.verify([node]), nowait)
        return node

    def remove(self, nodename, nowait=False):
        node = self.Nodes.remove(nodename)
        maybe_wait(self.sup.stop([node]), nowait)
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
        return _sup.supervisor
cluster = Cluster()

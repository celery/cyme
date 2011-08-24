"""scs.agent

- This is the Agent thread started by the :program:`scs-agent` program.

  It starts the HTTP server, the Supervisor, and one or more controllers.

- It also contains the :class:`LocalNodeManager` instance,
  which is the preferred API used to control and manage worker nodes handled
  by this agent.  I.e. it can be used to do synchronous actions that
  don't return until the supervisor has performed them.

"""

from __future__ import absolute_import

import logging

from celery import current_app as celery
from celery.utils.term import colored
from cl.g import Event
from kombu.utils import gen_unique_id

from . import signals
from . import supervisor
from .intsup import gSup
from .state import state
from .thread import gThread

from ..models import Broker, Node
from ..utils import instantiate, setup_logging, LazyProgressBar


class Agent(gThread):
    controller_cls = ".controller.Controller"
    httpd_cls = ".httpd.HttpServer"
    supervisor_cls = ".supervisor.Supervisor"
    httpd = None
    controller = None

    _components_ready = {}
    _components_shutdown = {}
    _presence_ready = 0
    _ready = False

    def __init__(self, addrport="", id=None, loglevel=logging.INFO,
            logfile=None, without_httpd=False, numc=2, sup_interval=None,
            ready_event=None, colored=None, **kwargs):
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
        self.colored = colored or colored(enabled=False)
        if not self.without_httpd:
            self.httpd = gSup(instantiate(self, self.httpd_cls, addrport),
                              signals.httpd_ready)
        self.supervisor = gSup(instantiate(self, self.supervisor_cls,
                                sup_interval), signals.supervisor_ready)
        self.controllers = [gSup(instantiate(self, self.controller_cls,
                                   id="%s.%s" % (self.id, i),
                                   connection=self.connection),
                                 signals.controller_ready)
                                for i in xrange(1, numc + 1)]
        c = [self.supervisor] + self.controllers + [self.httpd]
        c = self.components = list(filter(None, c))
        self._components_ready = dict(zip([z.thread for z in c],
                                          [False] * len(c)))
        for controller in self.controllers:
            if hasattr(controller.thread, "presence"):
                self._components_ready[controller.thread.presence] = False
        self._components_shutdown = dict(self._components_ready)
        super(Agent, self).__init__()

    def _component_ready(self, sender=None, **kwargs):
        if not self._ready:
            self._components_ready[sender] = True
            if all(self._components_ready.values()):
                if self._startup_pbar:
                    self._startup_pbar.finish()
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
        signals.presence_ready.connect(self._component_ready)
        signals.agent_ready.connect(self.on_ready)

    def run(self):
        state.is_agent = True
        self.setup_startup_progress()
        self.setup_shutdown_progress()
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

    def _component_shutdown(self, sender, **kwargs):
        self._components_shutdown[sender] = True
        if all(self._components_shutdown.values()):
            if self._shutdown_pbar:
                self._shutdown_pbar.finish()

    def setup_shutdown_progress(self):
        if self.is_enabled_for("DEBUG"):
            return
        sigs = (signals.thread_pre_shutdown,
                signals.thread_pre_join,
                signals.thread_post_join,
                signals.thread_post_shutdown)
        estimate = (len(sigs) * ((len(self.components) + 1) * 2)
                    + sum(c.thread.extra_shutdown_steps
                            for c in self.components))
        text = self.colored.blue("Shutdown...")
        p = self._shutdown_pbar = LazyProgressBar(estimate, text.embed())
        [sig.connect(p.step) for sig in sigs]
        signals.thread_post_shutdown.connect(self._component_shutdown)

    def setup_startup_progress(self):
        if self.is_enabled_for("INFO"):
            return
        tsigs = (signals.thread_pre_start,
                 signals.thread_post_start)
        osigs = (signals.httpd_ready,
                 signals.supervisor_ready,
                 signals.controller_ready,
                 signals.agent_ready)

        estimate = (len(tsigs) + ((len(self.components) + 10) * 2)
                     + len(osigs))
        text = self.colored.blue("Startup...")
        p = self._startup_pbar = LazyProgressBar(estimate, text.embed())
        [sig.connect(p.step) for sig in tsigs + osigs]



def maybe_wait(g, nowait):
    not nowait and g.wait()


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
local_nodes = LocalNodeManager()

"""cyme.branch

- This is the Branch thread started by the :program:`cyme-branch` program.

  It starts the HTTP server, the Supervisor, and one or more controllers.

"""

from __future__ import absolute_import

import logging

from celery import current_app as celery
from celery.utils import LOG_LEVELS, term
from cl.g import Event
from kombu.log import LogMixin
from kombu.utils import gen_unique_id

from . import signals
from .state import state
from .thread import gThread

from ..utils import find_symbol, instantiate


class MockSup(LogMixin):

    def __init__(self, thread, *args):
        self.thread = thread

    def start(self):
        self.thread.start()

    def stop(self):
        return self.thread.stop()


class Branch(gThread):
    controller_cls = ".controller.Controller"
    httpd_cls = ".httpd.HttpServer"
    supervisor_cls = ".supervisor.Supervisor"
    intsup_cls = ".intsup.gSup"

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
        self.colored = colored or term.colored(enabled=False)
        self.httpd = None
        gSup = find_symbol(self, self.intsup_cls)
        if not self.without_httpd:
            self.httpd = MockSup(instantiate(self, self.httpd_cls, addrport),
                              signals.httpd_ready)
        self.supervisor = gSup(instantiate(self, self.supervisor_cls,
                                sup_interval), signals.supervisor_ready)
        self.controllers = [gSup(instantiate(self, self.controller_cls,
                                   id="%s.%s" % (self.id, i),
                                   connection=self.connection,
                                   branch=self),
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
        super(Branch, self).__init__()

    def _component_ready(self, sender=None, **kwargs):
        if not self._ready:
            self._components_ready[sender] = True
            if all(self._components_ready.values()):
                signals.branch_ready.send(sender=self)
                if self.ready_event:
                    self.ready_event.send()
                    self.ready_event = None
                self._ready = True

    def on_ready(self, **kwargs):
        pass

    def prepare_signals(self):
        signals.controller_ready.connect(self._component_ready)
        signals.httpd_ready.connect(self._component_ready)
        signals.supervisor_ready.connect(self._component_ready)
        signals.presence_ready.connect(self._component_ready)
        signals.branch_ready.connect(self.on_ready)
        signals.thread_post_shutdown.connect(self._component_shutdown)

    def run(self):
        state.is_branch = True
        signals.branch_startup_request.send(sender=self)
        self.prepare_signals()
        self.info("Starting with id %r", self.id)
        [g.start() for g in self.components]
        self.exit_request.wait()

    def stop(self):
        self.exit_request.send(1)
        super(Branch, self).stop()

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
            signals.branch_shutdown_complete.send(sender=self)

    def about(self):
        url = port = None
        if self.httpd:
            url, port = self.httpd.thread.url, self.httpd.thread.port
        port = self.httpd.thread.port if self.httpd else None
        return {"id": self.id,
                "loglevel": LOG_LEVELS[self.loglevel],
                "numc": self.numc,
                "sup_interval": self.supervisor.interval,
                "logfile": self.logfile,
                "port": port,
                "url": url}

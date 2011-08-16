"""scs.agent"""

from __future__ import absolute_import

import logging

from celery import current_app as celery
from celery.utils import instantiate
from kombu.utils import gen_unique_id

from .models import Broker, Node
from .supervisor import supervisor
from .state import state
from .thread import gThread


class Agent(gThread):
    controller_cls = "scs.controller.Controller"
    httpd_cls = "scs.httpd.HttpServer"
    srs_cls = "scs.srs.SRSAgent"

    httpd = None
    srs = None
    controller = None

    def __init__(self, addrport="", id=None, loglevel=logging.INFO,
            logfile=None, without_httpd=False, without_srs=False,
            **kwargs):
        self.id = id or gen_unique_id()
        if isinstance(addrport, basestring):
            addr, _, port = addrport.partition(":")
            port = int(port) if port else 8000
            addrport = (addr, port)
        self.addrport = addrport
        self.connection = celery.broker_connection()
        self.without_httpd = without_httpd
        self.without_srs = without_srs
        self.logfile = logfile
        self.loglevel = loglevel
        if not self.without_httpd:
            self.httpd = instantiate(self.httpd_cls, addrport)
        if not self.without_srs:
            self.srs = instantiate(self.srs_cls,
                                   id=self.id, connection=self.connection)
        self.controller = instantiate(self.controller_cls,
                                      id=self.id, connection=self.connection)

        components = [self.httpd, self.srs, supervisor, self.controller]
        self.components = list(filter(None, components))
        super(Agent, self).__init__()

    def run(self):
        state.is_agent = True
        celery.log.setup_logging_subsystem(loglevel=self.loglevel,
                                           logfile=self.logfile)
        self.info("Starting with id %r" % (self.id, ))
        threads = []
        for component in self.components:
            threads.append(component.start())
            self.debug("Started %s thread" % (
                component.__class__.__name__, ))
        threads[-1].wait()


class Cluster(object):
    Nodes = Node._default_manager
    Brokers = Broker._default_manager
    supervisor = supervisor

    def _maybe_wait(self, g, nowait):
        if not nowait:
            return g.wait()

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
        self._maybe_wait(self.supervisor.verify([node]), nowait)
        return node

    def modify(self, nodename, queues=None, max_concurrency=None,
            min_concurrency=None, nowait=False):
        node = self.Nodes.modify(nodename, queues, max_concurrency,
                                                   min_concurrency)
        self._maybe_wait(self.supervisor.verify([node]), nowait)
        return node

    def remove(self, nodename, nowait=False):
        node = self.Nodes.remove(nodename)
        self._maybe_wait(self.supervisor.stop([node]), nowait)
        return node

    def restart(self, nodename, nowait=False):
        node = self.get(nodename)
        self._maybe_wait(self.supervisor.restart([node]), nowait)
        return node

    def enable(self, nodename, nowait=False):
        node = self.Nodes.enable(nodename)
        self._maybe_wait(self.supervisor.verify([node]), nowait)
        return node

    def disable(self, nodename, nowait=False):
        node = self.Nodes.disable(nodename)
        self._maybe_wait(self.supervisor.verify([node]), nowait)
        return node

    def add_consumer(self, name, queue, nowait=False):
        print("+GET NODE")
        node = self.get(name)
        print("-GET NODE")
        print("+ADD QUEUE")
        node.queues.add(queue)
        node.save()
        print("-ADD QUEUE")
        print("+WAIT FOR SUPERVISOR")
        self._maybe_wait(self.supervisor.verify([node]), nowait)
        print("-WAIT FOR SUPERVISOR")
        return node

    def cancel_consumer(self, name, queue, nowait=False):
        nodes = self.Nodes.remove_queue_from_nodes(queue, name=name)
        if nodes:
            self._maybe_wait(self.supervisor.verify(nodes), nowait)
        return nodes

    def remove_queue(self, queue, nowait=False):
        nodes = self.Nodes.remove_queue_from_nodes(queue)
        self._maybe_wait(self.supervisor.verify(nodes), nowait)
cluster = Cluster()

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
    httpd = None
    controller = None

    def __init__(self, addrport="", id=None, loglevel=logging.INFO,
            logfile=None, without_httpd=False, numc=10, **kwargs):
        self.id = id or gen_unique_id()
        if isinstance(addrport, basestring):
            addr, _, port = addrport.partition(":")
            addrport = (addr, int(port) if port else 8000)
        self.addrport = addrport
        self.connection = celery.broker_connection()
        self.without_httpd = without_httpd
        self.logfile = logfile
        self.loglevel = loglevel
        if not self.without_httpd:
            self.httpd = instantiate(self.httpd_cls, addrport)
        controllers = [instantiate(self.controller_cls,
                                   id="%s.%s" % (self.id, i),
                                   connection=self.connection)
                            for i in xrange(numc)]
        #self.controller = instantiate(self.controller_cls,
        #                              id=self.id, connection=self.connection)

        components = [self.httpd, supervisor] + controllers
        self.components = list(filter(None, components))
        super(Agent, self).__init__()

    def run(self):
        state.is_agent = True
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
        maybe_wait(supervisor.verify([node]), nowait)
        return node

    def remove(self, nodename, nowait=False):
        node = self.Nodes.remove(nodename)
        maybe_wait(supervisor.stop([node]), nowait)
        return node

    def restart(self, nodename, nowait=False):
        node = self.get(nodename)
        maybe_wait(supervisor.restart([node]), nowait)
        return node

    def enable(self, nodename, nowait=False):
        node = self.Nodes.enable(nodename)
        maybe_wait(supervisor.verify([node]), nowait)
        return node

    def disable(self, nodename, nowait=False):
        node = self.Nodes.disable(nodename)
        maybe_wait(supervisor.verify([node]), nowait)
        return node

    def add_consumer(self, name, queue, nowait=False):
        node = self.get(name)
        node.queues.add(queue)
        node.save()
        maybe_wait(supervisor.verify([node]), nowait)
        return node

    def cancel_consumer(self, name, queue, nowait=False):
        nodes = self.Nodes.remove_queue_from_nodes(queue, name=name)
        if nodes:
            maybe_wait(supervisor.verify(nodes), nowait)
        return nodes

    def remove_queue(self, queue, nowait=False):
        nodes = self.Nodes.remove_queue_from_nodes(queue)
        maybe_wait(supervisor.verify(nodes), nowait)
        return nodes
cluster = Cluster()

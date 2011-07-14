import logging

from celery import current_app as celery
from kombu.utils import gen_unique_id

from scs.amqp import AMQAgent
from scs.httpd import HttpServer
from scs.models import Node
from scs.supervisor import supervisor
from scs.thread import gThread


class Agent(gThread):

    def __init__(self, addrport="", loglevel=logging.INFO, logfile=None,
            without_httpd=False, **kwargs):
        self.id = gen_unique_id()
        self.addrport = addrport
        self.without_httpd = without_httpd
        self.logfile = logfile
        self.loglevel = loglevel
        self.threads = []
        self.httpd = HttpServer(addrport) if not self.without_httpd else None
        self.amq_agent = AMQAgent(self.id)

        components = [self.httpd, supervisor, self.amq_agent]
        self.components = list(filter(None, components))
        super(Agent, self).__init__()

    def run(self):
        celery.log.setup_logging_subsystem(loglevel=self.loglevel,
                                           logfile=self.logfile)
        threads = []
        for component in self.components:
            threads.append(component.start())
        threads[-1].wait()


class Cluster(object):
    Nodes = Node._default_manager
    supervisor = supervisor

    def get(self, nodename):
        return self.Nodes.get(name=nodename)

    def add(self, nodename=None, queues=None,
            max_concurrency=1, min_concurrency=1):
        node = self.Nodes.add(nodename, queues, max_concurrency,
                                                min_concurrency)
        self.supervisor.verify([node]).wait()
        return node

    def modify(self, nodename, queues=None, max_concurrency=None,
                                            min_concurrency=None):
        node = self.Nodes.modify(nodename, queues, max_concurrency,
                                                   min_concurrency)
        self.supervisor.verify([node]).wait()
        return node

    def remove(self, nodename):
        node = self.Nodes.remove(nodename)
        self.supervisor.stop([node]).wait()
        return node

    def restart(self, nodename):
        self.supervisor.restart([self.get(nodename)]).wait()

    def enable(self, nodename):
        self.supervisor.verify([self.Nodes.enable(nodename)]).wait()

    def disable(self, nodename):
        self.supervisor.verify([self.Nodes.disable(nodename)]).wait()


cluster = Cluster()

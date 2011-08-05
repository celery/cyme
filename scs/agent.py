import logging

from celery import current_app as celery
from celery.utils import instantiate
from kombu.utils import gen_unique_id

from scs.models import Broker, Node
from scs.supervisor import supervisor
from scs.thread import gThread


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
            if port:
                port = int(port)
            addrport = (addr, port)
        self.addrport = addrport
        self.without_httpd = without_httpd
        self.without_srs = without_srs
        self.logfile = logfile
        self.loglevel = loglevel
        if not self.without_httpd:
            self.httpd = instantiate(self.httpd_cls, addrport)
        if not self.without_srs:
            self.srs = instantiate(self.srs_cls, self.id)
        self.controller = instantiate(self.controller_cls, self.id)

        components = [self.httpd, supervisor, self.srs, self.controller]
        self.components = list(filter(None, components))
        super(Agent, self).__init__()

    def run(self):
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

    def get(self, nodename):
        return self.Nodes.get(name=nodename)

    def add(self, nodename=None, queues=None,
            max_concurrency=1, min_concurrency=1, hostname=None,
            port=None, userid=None, password=None, virtual_host=None,
            **kwargs):
        broker = None
        if hostname:
            broker = self.Brokers.get_or_create(
                            hostname=hostname, port=port,
                            userid=userid, password=password,
                            virtual_host=virtual_host)
        node = self.Nodes.add(nodename, queues, max_concurrency,
                              min_concurrency, broker)
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
        node = self.get(nodename)
        self.supervisor.restart([node]).wait()
        return node

    def enable(self, nodename):
        node = self.Nodes.enable(nodename)
        self.supervisor.verify([node]).wait()
        return node

    def disable(self, nodename):
        node = self.Nodes.disable(nodename)
        self.supervisor.verify([node]).wait()
        return node

cluster = Cluster()

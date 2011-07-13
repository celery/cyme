from __future__ import absolute_import

import eventlet
import logging
import os
import sys
import threading

from optparse import make_option as Option

from celery import current_app as celery
from celery.utils import LOG_LEVELS
from djcelery.management.base import CeleryCommand
from eventlet import wsgi

from django.core.handlers import wsgi as djwsgi
from django.core.servers.basehttp import AdminMediaHandler

from scs.supervisor import supervisor
from scs.thread import gThread


class SCSMediaHandler(AdminMediaHandler):

    def get_base_dir(self):
        import scs
        return os.path.join(scs.__path__[0], "media")

    def get_base_url(self):
        return "/static/"


class HttpServer(gThread):

    def __init__(self, addrport=None):
        self.addrport = addrport
        super(HttpServer, self).__init__()

    def run(self):
        addr = self.addrport or ("", 8000)
        wsgi.server(eventlet.listen(addr), self.get_app())

    def get_app(self):
        return SCSMediaHandler(AdminMediaHandler(djwsgi.WSGIHandler()))



class Other(gThread):

    def run(self):
        from time import sleep
        while 1:
            sleep(1)


class Agent(gThread):

    def __init__(self, addrport="", loglevel=logging.INFO, logfile=None,
            without_httpd=False, **kwargs):
        self.addrport = addrport
        self.without_httpd = without_httpd
        self.logfile = logfile
        self.loglevel = loglevel
        self.threads = []
        self.httpd = HttpServer(addrport) if not self.without_httpd else None

        components = [self.httpd, supervisor, Other()]
        self.components = list(filter(None, components))
        super(Agent, self).__init__()

    def run(self):
        celery.log.setup_logging_subsystem(loglevel=self.loglevel,
                                           logfile=self.logfile)
        threads = []
        for component in self.components:
            threads.append(component.start())
        threads[-1].wait()


class Command(CeleryCommand):
    args = '[optional port number, or ipaddr:port]'
    option_list = CeleryCommand.option_list + (
        Option("--without-httpd",
               default=False,
               action="store_true",
               dest="without_httpd",
               help="Disable HTTP server"),
       Option('-l', '--loglevel',
              default="INFO",
              action="store",
              dest="loglevel",
              help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL"),
       Option('-f', '--logfile',
              default=None,
              action="store", dest="logfile",
              help="Path to log file, stderr by default."),
    )

    help = 'Starts Django Admin instance and celerycam in the same process.'
    # see http://code.djangoproject.com/changeset/13319.
    stdout, stderr = sys.stdout, sys.stderr

    def handle(self, *args, **kwargs):
        """Handle the management command."""
        loglevel = kwargs.get("loglevel")
        if not isinstance(loglevel, int):
            try:
                loglevel = LOG_LEVELS[loglevel.upper()]
            except KeyError:
                self.die("Unknown level %r. Please use one of %s." % (
                            loglevel, "|".join(l for l in LOG_LEVELS.keys()
                                        if isinstance(l, basestring))))
        kwargs["loglevel"] = loglevel
        Agent(*args, **kwargs).start().wait()


    def die(self, msg, exitcode=1):
        sys.stderr.write("Error: %s\n" % (msg, ))
        sys.exit(exitcode)

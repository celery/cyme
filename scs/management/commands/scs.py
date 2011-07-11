from __future__ import absolute_import

import eventlet
import logging
import os
import sys
import threading

from celery import current_app as celery
from djcelery.management.base import CeleryCommand
from eventlet import wsgi

from django.core.handlers import wsgi as djwsgi
from django.core.servers.basehttp import AdminMediaHandler

from scs.supervisor import supervisor


class HttpServer(object):

    def __init__(self, addrport=None):
        self.addrport = addrport

    def start(self):
        return eventlet.spawn(self.serve)

    def serve(self):
        addr = self.addrport or ("", 8000)
        wsgi.server(eventlet.listen(addr), self.get_app())

    def get_app(self):
        return AdminMediaHandler(djwsgi.WSGIHandler())


def other():
    from time import sleep
    while 1:
        sleep(1)


class Command(CeleryCommand):
    args = '[optional port number, or ipaddr:port]'
    option_list = CeleryCommand.option_list
    help = 'Starts Django Admin instance and celerycam in the same process.'
    # see http://code.djangoproject.com/changeset/13319.
    stdout, stderr = sys.stdout, sys.stderr

    def handle(self, addrport="", *args, **options):
        """Handle the management command."""
        celery.log.setup_logging_subsystem(logging.INFO)
        server = HttpServer(addrport)
        server_t = server.start()
        supervisor_t = supervisor.start()
        agent_t = eventlet.spawn(other)
        agent_t.wait()


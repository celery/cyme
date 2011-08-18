"""scs.httpd"""

from __future__ import absolute_import

from eventlet import listen
from eventlet import wsgi

from django.core.handlers import wsgi as djwsgi
from django.core.servers.basehttp import AdminMediaHandler

from .thread import gThread


class HttpServer(gThread):

    def __init__(self, addrport=None):
        self.addrport = addrport
        super(HttpServer, self).__init__()

    def run(self):
        wsgi.server(listen(self.addrport or ("", 8000)),
                    AdminMediaHandler(djwsgi.WSGIHandler()))

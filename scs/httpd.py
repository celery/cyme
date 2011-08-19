"""scs.httpd"""

from __future__ import absolute_import

from eventlet import listen
from eventlet import wsgi

from django.core.handlers import wsgi as djwsgi
from django.core.servers.basehttp import AdminMediaHandler

from .thread import gThread
from .signals import httpd_ready


class HttpServer(gThread):

    def __init__(self, addrport=None):
        self.addrport = addrport
        super(HttpServer, self).__init__()

    def run(self):
        addrport = self.addrport or ("", 8000)
        handler = AdminMediaHandler(djwsgi.WSGIHandler())
        sock = listen(addrport)
        g = self.spawn(wsgi.server, sock, handler)
        httpd_ready.send(sender=self, addrport=addrport,
                         handler=handler, sock=sock)
        return g.wait()

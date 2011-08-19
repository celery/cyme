"""scs.httpd"""

from __future__ import absolute_import

from time import sleep

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
        sock = listen(addrport)
        handler = AdminMediaHandler(djwsgi.WSGIHandler())
        # can't hook into eventlet.wsgi to add ready event,
        # so have to do this little dance.
        g = self.spawn(wsgi.server, sock, handler)
        sleep(0.5)
        httpd_ready.send(sender=self, addrport=addrport,
                         handler=handler, sock=sock)
        return g.wait()


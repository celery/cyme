import eventlet
import os

from eventlet import wsgi

from django.core.handlers import wsgi as djwsgi
from django.core.servers.basehttp import AdminMediaHandler

from scs.thread import gThread


class SCSMediaHandler(AdminMediaHandler):

    def get_base_dir(self):
        import scs
        return os.path.join(scs.__path__[0], "media")

    def get_base_url(self):
        return "/static/"

    def _should_handle(self, path):
        if "/admin" in path:
            return False
        return super(SCSMediaHandler, self)._should_handle(path)


class HttpServer(gThread):

    def __init__(self, addrport=None):
        self.addrport = addrport
        super(HttpServer, self).__init__()

    def run(self):
        addr = self.addrport or ("", 8000)
        wsgi.server(eventlet.listen(addr), self.get_app())

    def get_app(self):
        return SCSMediaHandler(AdminMediaHandler(djwsgi.WSGIHandler()))

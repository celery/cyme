"""cyme.branch.httpd

- Our embedded WSGI server used to serve the HTTP API.

"""

from __future__ import absolute_import

from eventlet import listen
from eventlet import wsgi

from django.core.handlers import wsgi as djwsgi
from django.core.servers.basehttp import AdminMediaHandler
from requests import get

from .thread import gThread
from .signals import httpd_ready


class HttpServer(gThread):
    joinable = False

    def __init__(self, addrport=None):
        host, port = addrport or ("", 8000)
        if host == "localhost":
            # dnspython bug?
            host = "127.0.0.1"
        self.host, self.port = self.addrport = (host, port)
        super(HttpServer, self).__init__()

    def server(self, sock, handler):
        return wsgi.server(sock, handler,
                           log=self.create_log(),
                           protocol=self.create_http_protocol())

    def run(self):
        handler = AdminMediaHandler(djwsgi.WSGIHandler())
        sock = listen(self.addrport)
        g = self.spawn(self.server, sock, handler)
        self.info("ready")
        httpd_ready.send(sender=self, addrport=self.addrport,
                         handler=handler, sock=sock)
        return g.wait()

    def _do_ping(self, timeout):
        return get(self.url + "/ping/", timeout=timeout).ok

    def create_log(self):
        logger = self

        class _Log(object):

            def write(self, message):
                message = message.rstrip('\n')
                (logger.debug if '/ping/' in message else logger.info)(message)

        return _Log()

    def create_http_protocol(self):
        logger = self

        class HttpProtocol(wsgi.HttpProtocol):

            def get_format_args(self, format, *args):
                return ["%s - - [%s] %s", self.address_string(),
                                          self.log_date_time_string(),
                                          format] + args

            def log_message(self, format, *args):
                return logger.info(*self.get_format_args(format, *args))

            def log_error(self, format, *args):
                return logger.error(*self.get_format_args(format, *args))

        return HttpProtocol

    @property
    def url(self):
        addr, port = self.addrport
        if not addr or addr in ("0.0.0.0", ):
            addr = "127.0.0.1"
        return "http://%s:%s" % (addr, port)

    @property
    def logger_name(self):
        return "wsgi"

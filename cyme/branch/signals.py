"""cyme.branch.signals"""

from __future__ import absolute_import

from celery.utils.dispatch import Signal

#: Sent when the http server is ready to accept requests.
#: Arguments:
#:
#:     :sender: the :class:`~cyme.httpd.HttpServer` instance.
#:     :addrport: the ``(hostname, port)`` tuple.
#:     :handler: the WSGI handler used.
#:     :sock: the socket used.
httpd_ready = Signal(providing_args=["addrport", "handler", "sock"])

#: Sent when the supervisor is ready.
#: Arguments:
#:
#:     :sender: is the :class:`~cyme.supervisor.Supervisor` instance.
supervisor_ready = Signal()

#: Sent when a controller is ready.
#:
#: Arguments:
#:     :sender: is the :class:`~cyme.controller.Controller` instance.
controller_ready = Signal()

#: Sent when the branch and all its components are ready to serve.
#:
#: Arguments:
#:     :sender: is the :class:`~cyme.branch.Branch` instance.
branch_ready = Signal()

branch_shutdown_complete = Signal()


branch_startup_request = Signal()
branch_shutdown_request = Signal()


thread_pre_shutdown = Signal()
thread_pre_join = Signal(providing_args=["timeout"])
thread_exit = Signal()
thread_post_join = Signal()
thread_post_shutdown = Signal()
thread_shutdown_step = Signal()

thread_pre_start = Signal()
thread_post_start = Signal()
thread_startup_step = Signal()
presence_ready = Signal()

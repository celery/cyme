"""scs.agent.signals"""

from __future__ import absolute_import

from celery.utils.dispatch import Signal

#: Sent when the http server is ready to accept requests.
#: Arguments:
#:
#:     :sender: the :class:`~scs.httpd.HttpServer` instance.
#:     :addrport: the ``(hostname, port)`` tuple.
#:     :handler: the WSGI handler used.
#:     :sock: the socket used.
httpd_ready = Signal(providing_args=["addrport", "handler", "sock"])

#: Sent when the supervisor is ready.
#: Arguments:
#:
#:     :sender: is the :class:`~scs.supervisor.Supervisor` instance.
supervisor_ready = Signal()

#: Sent when a controller is ready.
#:
#: Arguments:
#:     :sender: is the :class:`~scs.controller.Controller` instance.
controller_ready = Signal()

#: Sent when the agent and all its components are ready to serve.
#:
#: Arguments:
#:     :sender: is the :class:`~scs.agent.Agent` instance.
agent_ready = Signal()

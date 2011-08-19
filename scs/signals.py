from celery.utils.dispatch import Signal


httpd_ready = Signal(providing_args=["addrport", "handler", "sock"])
supervisor_ready = Signal()
controller_ready = Signal()

agent_ready = Signal()

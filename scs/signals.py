"""scs.signals"""

from __future__ import absolute_import

from celery.utils.dispatch import Signal

node_started = Signal(providing_args=["instance"])
node_stopped = Signal(providing_args=["instance"])

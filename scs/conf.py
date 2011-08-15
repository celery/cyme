"""scs.conf"""

from __future__ import absolute_import

from django.conf import settings

SCS_INSTANCE_DIR = getattr(settings, "SCS_INSTSANCE_DIR", "/var/run/scs")

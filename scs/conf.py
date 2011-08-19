"""scs.conf"""

from __future__ import absolute_import

import os

from django.conf import settings

SCS_INSTANCE_DIR = os.path.abspath(getattr(settings,
                        "SCS_INSTANCE_DIR", "/var/run/scs"))

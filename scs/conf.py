"""scs.conf"""

from __future__ import absolute_import

from django.conf import settings

from .utils import Path

SCS_INSTANCE_DIR = Path(getattr(settings,
                        "SCS_INSTANCE_DIR", "instances")).absolute()
SCS_DEFAULT_POOL = getattr(settings, "SCS_DEFAULT_POOL", "processes")

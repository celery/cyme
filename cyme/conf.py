"""cyme.conf"""

from __future__ import absolute_import

from django.conf import settings

from .utils import Path

CYME_INSTANCE_DIR = Path(getattr(settings,
                        "CYME_INSTANCE_DIR", "instances")).absolute()
CYME_DEFAULT_POOL = getattr(settings, "CYME_DEFAULT_POOL", "processes")

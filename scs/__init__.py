"""Simple Celery Service"""
from __future__ import absolute_import

import os

VERSION = (0, 0, 1, "a1")

__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "VMware, Inc."
__contact__ = "info@rabbitmq.com"
__homepage__ = "http://rabbitmq.com",
__docformat__ = "restructuredtext"
__license__ = "BSD (3 clause)"

DEBUG = os.environ.get("SCS_DEBUG")
DEBUG_BLOCK = os.environ.get("SCS_DEBUG_BLOCK")
DEBUG_READERS = os.environ.get("SCS_DEBUG_READERS")
if not os.environ.get("SCS_NO_EVAL"):
    from .client import Client  # noqa

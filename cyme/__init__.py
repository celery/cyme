"""Cyme - Celery instance manager"""
from __future__ import absolute_import

import os

VERSION = (0, 0, 5)

__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"
__license__ = "BSD (3 clause)"

DEBUG = os.environ.get("CYME_DEBUG")
DEBUG_BLOCK = os.environ.get("CYME_DEBUG_BLOCK")
DEBUG_READERS = os.environ.get("CYME_DEBUG_READERS")
if not os.environ.get("CYME_NO_EVAL"):
    from .client import Client  # noqa

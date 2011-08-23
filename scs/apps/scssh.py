"""scs.apps.scssh"""

from __future__ import absolute_import, with_statement

import os

from django.core import management

from .base import app, Env
from ..utils import setup_logging


@app()
def scssh(env, argv):
    env.setup()
    if os.environ.get("SCS_LOG_DEBUG", False):
        setup_logging(logging.DEBUG)
    env.management.call_command("shell")


if __name__ == "__main__":
    scssh()

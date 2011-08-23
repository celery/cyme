"""scs.apps.scssh"""

from __future__ import absolute_import, with_statement

import os

from .base import app
from ..utils import setup_logging


@app()
def scssh(env, argv):
    env.syncdb()
    env.setup()
    if os.environ.get("SCS_LOG_DEBUG", False):
        setup_logging("DEBUG")
    env.management.call_command("shell")


if __name__ == "__main__":
    scssh()

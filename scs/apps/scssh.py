"""scs.apps.scssh"""

from __future__ import absolute_import

import os

from django.core import management

from .base import app


@app()
def scssh(argv):
    if os.environ.get("SCS_LOG_DEBUG", False):
        from celery import current_app as celery
        from logging import DEBUG
        celery.log.setup_logging_subsystem(loglevel=DEBUG)
    management.call_command("shell")


if __name__ == "__main__":
    scssh()

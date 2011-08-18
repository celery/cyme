"""scs.apps.base"""

from __future__ import absolute_import, with_statement

from .. import DEBUG, DEBUG_BLOCK

import eventlet
import eventlet.debug
eventlet.monkey_patch()
#eventlet.debug.hub_prevent_multiple_readers(False)
if DEBUG_BLOCK:
    eventlet.debug.hub_blocking_detection(True)
    print("+++ BLOCING DETECTION ENABLED +++")

import getpass
import logging
import os
import sys

from .. import settings as default_settings
from ..utils import imerge_settings

from django.conf import settings
from django.core import management


class BaseApp(object):

    def __call__(self, argv=None):
        return self.run_from_argv(argv)

    def configure(self):
        if not settings.configured:
            management.setup_environ(default_settings)
        else:
            imerge_settings(settings, default_settings)

    def syncdb(self):
        gp, getpass.getpass = getpass.getpass, getpass.fallback_getpass
        try:
            management.call_command("syncdb")
        finally:
            getpass.getpass = gp

    def run_from_argv(self, argv=None):
        if DEBUG:
            from celery.apps.worker import install_cry_handler
            install_cry_handler(logging.getLogger())
        argv = sys.argv if argv is None else argv
        try:
            self.configure()
            self.syncdb()
            return self.run(argv)
        except KeyboardInterrupt:
            if DEBUG:
                raise


def app(fun):

    def run(self, *args, **kwargs):
        return fun(*args, **kwargs)

    return type(fun.__name__, (BaseApp, ), {
        "run": run, "__module__": fun.__module__, "__doc__": fun.__doc__})()

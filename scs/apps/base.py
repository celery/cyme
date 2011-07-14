from __future__ import with_statement

import eventlet
eventlet.monkey_patch()

import sys
import getpass

from scs import settings as default_settings
from scs.utils import imerge_settings

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
        if argv is None:
            argv = sys.argv
        try:
            self.configure()
            self.syncdb()
            return self.run(argv)
        except KeyboardInterrupt:
            pass


def app(fun):

    def run(self, *args, **kwargs):
        return fun(*args, **kwargs)

    return type(fun.__name__, (BaseApp, ), {"run": run,
                                            "__module__": fun.__module__,
                                            "__doc__": fun.__doc__})()


def run_scs(argv):
    from scs.management.commands import scs
    scs.Command().run_from_argv([argv[0], "scs"] + argv[1:])

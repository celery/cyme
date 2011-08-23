"""scs.apps.base"""

from __future__ import absolute_import, with_statement

from .. import __version__, DEBUG, DEBUG_BLOCK, DEBUG_READERS

import getpass
import logging
import sys

from kombu.utils import cached_property


class Env(object):

    def __init__(self, needs_syncdb=True, instance_dir=None):
        self.needs_syncdb = needs_syncdb
        self.instance_dir = instance_dir

    def setup_eventlet(self):
        import eventlet
        import eventlet.debug
        eventlet.monkey_patch()
        if DEBUG_READERS:
            eventlet.debug.hub_prevent_multiple_readers(False)
            print("+++ MULTIPLE READERS ALLOWED +++")
        if DEBUG_BLOCK:
            eventlet.debug.hub_blocking_detection(True)
            print("+++ BLOCKING DETECTION ENABLED +++")

    def configure(self):
        from django.conf import settings
        from .. import settings as default_settings
        from ..utils import imerge_settings

        if not settings.configured:
            self.management.setup_environ(default_settings)
        else:
            imerge_settings(settings, default_settings)
        if self.instance_dir:
            settings.SCS_INSTANCE_DIR = self.instance_dir

    def syncdb(self, interactive=True):
        gp, getpass.getpass = getpass.getpass, getpass.fallback_getpass
        try:
            self.management.call_command("syncdb", interactive=interactive)
        finally:
            getpass.getpass = gp
            self.needs_syncdb = False

    @cached_property
    def management(self):
        from django.core import management
        return management

    def before(self):
        self.setup_eventlet()
        self.configure()

    def setup(self):
        if DEBUG:
            from celery.apps.worker import install_cry_handler
            install_cry_handler(logging.getLogger())
        from cl import pools
        from celery import current_app as celery
        pools.set_limit(celery.conf.BROKER_POOL_LIMIT)
        celery._pool = pools.connections[celery.broker_connection()]
        return self


class BaseApp(object):
    env = None

    def __call__(self, argv=None):
        return self.run_from_argv(argv)

    def get_version(self):
        return "scs v%s" % (__version__, )

    def run_from_argv(self, argv=None):
        argv = sys.argv if argv is None else argv
        if '--version' in argv:
            print(self.get_version())
            sys.exit(0)
        try:
            env = self.env or Env()
            env.before()
            env.setup()
            return self.run(env, argv)
        except KeyboardInterrupt:
            if DEBUG:
                raise
            raise SystemExit()


def app(**attrs):
    x = attrs

    def _app(fun):

        def run(self, *args, **kwargs):
            return fun(*args, **kwargs)

        attrs = dict({"run": run, "__module__": fun.__module__,
                      "__doc__": fun.__doc__}, **x)

        return type(fun.__name__, (BaseApp, ), attrs)()

    return _app

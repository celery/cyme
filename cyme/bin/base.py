"""cyme.bin.base

- Utilities used by command line applications,
  basically just to set up Django without having an actual project.

"""

from __future__ import absolute_import
from __future__ import with_statement

from .. import __version__, DEBUG, DEBUG_BLOCK, DEBUG_READERS

import django
import getpass
import os
import sys

from importlib import import_module

from kombu.utils import cached_property

from ..utils import Path


class Env(object):

    def __init__(self, needs_eventlet=False, instance_dir=None):
        self.needs_eventlet = needs_eventlet
        self.instance_dir = instance_dir

    def __enter__(self):
        if self.needs_eventlet:
            self.setup_eventlet()
        self.configure()
        self.setup_pool_limit()
        return self

    def __exit__(self, *exc_info):
        pass

    def setup_eventlet(self):
        import eventlet
        import eventlet.debug
        eventlet.monkey_patch()
        if DEBUG_READERS:
            eventlet.debug.hub_prevent_multiple_readers(False)
            print("+++ MULTIPLE READERS ALLOWED +++")    # noqa+
        if DEBUG_BLOCK:
            eventlet.debug.hub_blocking_detection(True)
            print("+++ BLOCKING DETECTION ENABLED +++")  # noqa+

    def configure(self):
        from django.conf import settings
        from .. import settings as default_settings
        from ..utils import imerge_settings
        mod = os.environ.setdefault("DJANGO_SETTINGS_MODULE",
                                    default_settings.__name__)

        if not settings.configured:
            if django.VERSION < (1, 4):
                self.management.setup_environ(import_module(mod))
        else:
            imerge_settings(settings, default_settings)
        if self.instance_dir:
            settings.CYME_INSTANCE_DIR = self.instance_dir

    def setup_pool_limit(self, **kwargs):
        from kombu import pools
        from celery import current_app as celery
        limit = kwargs.get("limit", celery.conf.BROKER_POOL_LIMIT)
        pools.set_limit(limit if self.needs_eventlet else 1)
        celery._pool = pools.connections[celery.broker_connection()]

    def syncdb(self, interactive=True):
        from django.conf import settings
        from django.db.utils import DEFAULT_DB_ALIAS
        dbconf = settings.DATABASES[DEFAULT_DB_ALIAS]
        if dbconf["ENGINE"] == "django.db.backends.sqlite3":
            if Path(dbconf["NAME"]).absolute().exists():
                return
        gp, getpass.getpass = getpass.getpass, getpass.fallback_getpass
        try:
            self.management.call_command("syncdb", interactive=interactive)
        finally:
            getpass.getpass = gp

    @cached_property
    def management(self):
        from django.core import management
        return management


class BaseApp(object):
    env = None
    needs_eventlet = False
    instance_dir = None

    def get_version(self):
        return "cyme v%s" % (__version__, )

    def run_from_argv(self, argv=None):
        argv = sys.argv if argv is None else argv
        if '--version' in argv:
            print(self.get_version())
            sys.exit(0)
        try:
            with (self.env or
                    Env(self.needs_eventlet, self.instance_dir)) as env:
                return self.run(env, argv)
        except KeyboardInterrupt:
            if DEBUG:
                raise
            raise SystemExit()
    __call__ = run_from_argv


def app(**attrs):
    x = attrs

    def _app(fun):

        def run(self, *args, **kwargs):
            return fun(*args, **kwargs)

        attrs = dict({"run": run, "__module__": fun.__module__,
                      "__doc__": fun.__doc__}, **x)

        return type(fun.__name__, (BaseApp, ), attrs)()

    return _app

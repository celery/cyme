"""scs.management.commands.base"""

from __future__ import absolute_import

import os
import sys

from optparse import make_option as Option  # noqa

from celery.utils import LOG_LEVELS, get_cls_by_name, instantiate
from django.conf import settings
from djcelery.management.base import CeleryCommand

from scs import __version__
from scs.utils import cached_property, Path


def die(msg, exitcode=1):
    sys.stderr.write("Error: %s\n" % (msg, ))
    sys.exit(exitcode)


class SCSCommand(CeleryCommand):
    __version__ = __version__
    LOG_LEVELS = LOG_LEVELS

    # see http://code.djangoproject.com/changeset/13319.
    stdout, stderr = sys.stdout, sys.stderr

    default_detach_pidfile = None
    default_detach_logfile = None

    def __init__(self, env=None, *args, **kwargs):
        if env is None:
            env = instantiate("scs.apps.base.Env")
            env.before()
        self.env = env

    def get_version(self):
        return "scs v%s" % (self.__version__, )

    def enter_instance_dir(self):
        self.instance_dir.mkdir(parents=True)
        self.instance_dir.chdir()

    def syncdb(self):
        from django.db.utils import DEFAULT_DB_ALIAS
        dbconf = settings.DATABASES[DEFAULT_DB_ALIAS]
        if dbconf["ENGINE"] == "django.db.backends.sqlite3":
            if Path(dbconf["NAME"]).absolute().exists():
                return
        self.env.syncdb()

    def prepare_options(self, broker=None, loglevel=None, logfile=None,
            pidfile=None, detach=None, instance_dir=None, **kwargs):
        if detach:
            logfile = logfile or self.default_detach_logfile
            pidfile = pidfile or self.default_detach_pidfilE
        if broker:
            settings.BROKER_HOST = broker
        if instance_dir:
            settings.SCS_INSTANCE_DIR = instance_dir
        if pidfile and not os.path.isabs(pidfile):
            pidfile = os.path.join(self.instance_dir, pidfile)
        if logfile and not os.path.isabs(logfile):
            logfile = os.path.join(self.instance_dir, logfile)
        if not isinstance(loglevel, int):
            try:
                loglevel = self.LOG_LEVELS[loglevel.upper()]
            except KeyError:
                self.die("Unknown level %r. Please use one of %s." % (
                        loglevel, "|".join(l for l in self.LOG_LEVELS.keys()
                                    if isinstance(l, basestring))))
        return dict(kwargs, loglevel=loglevel, detach=detach,
                            logfile=logfile, pidfile=pidfile)

    def print_help(self, prog_name=None, subcommand=None):
        return super(SCSCommand, self).print_help("scs", "")

    @cached_property
    def instance_dir(self):
        return get_cls_by_name("scs.conf.SCS_INSTANCE_DIR")

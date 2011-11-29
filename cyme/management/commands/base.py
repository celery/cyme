"""cyme.management.commands.base"""

from __future__ import absolute_import

import logging
import os
import sys

from optparse import make_option as Option  # noqa

from celery.utils import get_cls_by_name, instantiate
from django.conf import settings
from djcelery.management.base import CeleryCommand
from kombu.log import LOG_LEVELS

from cyme import __version__
from cyme.utils import (cached_property, setup_logging,
                        redirect_stdouts_to_logger)


def die(msg, exitcode=1):
    sys.stderr.write("Error: %s\n" % (msg, ))
    sys.exit(exitcode)


class CymeCommand(CeleryCommand):
    __version__ = __version__
    LOG_LEVELS = LOG_LEVELS

    # see http://code.djangoproject.com/changeset/13319.
    stdout, stderr = sys.stdout, sys.stderr

    default_detach_pidfile = None
    default_detach_logfile = None

    def __init__(self, env=None, *args, **kwargs):
        if env is None:
            env = instantiate("cyme.bin.base.Env")
            self.setup_default_env(env)
        self.env = env

    def setup_default_env(self, env):
        pass

    def get_version(self):
        return "cyme v%s" % (self.__version__, )

    def enter_instance_dir(self):
        self.instance_dir.mkdir(parents=True)
        self.instance_dir.chdir()

    def install_cry_handler(self):
        from celery.apps.worker import install_cry_handler
        return install_cry_handler(logging.getLogger())

    def install_rdb_handler(self):
        from celery.apps.worker import install_rdb_handler
        return install_rdb_handler(envvar="CYME_RDBSIG")

    def redirect_stdouts_to_logger(self, loglevel="INFO", logfile=None,
            redirect_level="WARNING"):
        return redirect_stdouts_to_logger(loglevel, logfile, redirect_level)

    def setup_logging(self, loglevel="WARNING", logfile=None, **kwargs):
        return setup_logging(loglevel, logfile)

    def prepare_options(self, broker=None, loglevel=None, logfile=None,
            pidfile=None, detach=None, instance_dir=None, **kwargs):
        if detach:
            logfile = logfile or self.default_detach_logfile
            pidfile = pidfile or self.default_detach_pidfilE
        if broker:
            settings.BROKER_HOST = broker
        if instance_dir:
            settings.CYME_INSTANCE_DIR = instance_dir
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
        return super(CymeCommand, self).print_help("cyme", "")

    @cached_property
    def instance_dir(self):
        return get_cls_by_name("cyme.conf.CYME_INSTANCE_DIR")

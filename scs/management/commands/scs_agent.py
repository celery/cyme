"""

.. program:: scs-agent

``scs-agent``
=============

Starts the SCS agent service.

Options
-------

.. cmdoption:: -i, --id

    Set agent id, if not provided one will be automatically generated.

.. cmdoption:: --without-httpd

    Disable the HTTP server thread.

.. cmdoption:: -l, --loglevel

    Set custom log level. One of DEBUG/INFO/WARNING/ERROR/CRITICAL.
    Default is INFO.

.. cmdoption:: -f, --logfile

    Set custom logfile path. Default is :file:`<stderr>`

.. cmdoption:: -D, --instance-dir

    Custom instance directory (deafult is /var/run/scs)
    Must be writeable by the user scs-agent runs as.

"""

from __future__ import absolute_import

import sys

from optparse import make_option as Option

from celery.utils import LOG_LEVELS
from djcelery.management.base import CeleryCommand

from django.conf import settings


class Command(CeleryCommand):
    args = '[optional port number, or ipaddr:port]'
    option_list = CeleryCommand.option_list + (
        Option("-i", "--id",
               default=None,
               action="store",
               dest="id",
               help="Set explicit agent id."),
        Option("--without-httpd",
               default=False,
               action="store_true",
               dest="without_httpd",
               help="Disable HTTP server"),
       Option('-l', '--loglevel',
              default="INFO",
              action="store",
              dest="loglevel",
              help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL"),
       Option('-f', '--logfile',
              default=None,
              action="store", dest="logfile",
              help="Path to log file, stderr by default."),
       Option('-D', '--instance-dir',
              default=None,
              action="store", dest="instance_dir",
              help="Custom instance dir. Default is /var/run/scs"),
    )

    help = 'Starts the SCS agent'
    # see http://code.djangoproject.com/changeset/13319.
    stdout, stderr = sys.stdout, sys.stderr

    def handle(self, *args, **kwargs):
        """Handle the management command."""
        loglevel = kwargs.get("loglevel")
        instance_dir = kwargs.get("instance_dir")
        if instance_dir:
            settings.SCS_INSTANCE_DIR = instance_dir
        if not isinstance(loglevel, int):
            try:
                loglevel = LOG_LEVELS[loglevel.upper()]
            except KeyError:
                self.die("Unknown level %r. Please use one of %s." % (
                            loglevel, "|".join(l for l in LOG_LEVELS.keys()
                                        if isinstance(l, basestring))))
        kwargs["loglevel"] = loglevel
        from scs.agent import Agent
        Agent(*args, **kwargs).start().wait()

    def die(self, msg, exitcode=1):
        sys.stderr.write("Error: %s\n" % (msg, ))
        sys.exit(exitcode)

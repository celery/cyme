from __future__ import absolute_import

import sys

from optparse import make_option as Option

from celery.utils import LOG_LEVELS
from djcelery.management.base import CeleryCommand

from scs.agent import Agent


class Command(CeleryCommand):
    args = '[optional port number, or ipaddr:port]'
    option_list = CeleryCommand.option_list + (
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
    )

    help = 'Starts the SCS agent'
    # see http://code.djangoproject.com/changeset/13319.
    stdout, stderr = sys.stdout, sys.stderr

    def handle(self, *args, **kwargs):
        """Handle the management command."""
        loglevel = kwargs.get("loglevel")
        if not isinstance(loglevel, int):
            try:
                loglevel = LOG_LEVELS[loglevel.upper()]
            except KeyError:
                self.die("Unknown level %r. Please use one of %s." % (
                            loglevel, "|".join(l for l in LOG_LEVELS.keys()
                                        if isinstance(l, basestring))))
        kwargs["loglevel"] = loglevel
        Agent(*args, **kwargs).start().wait()

    def die(self, msg, exitcode=1):
        sys.stderr.write("Error: %s\n" % (msg, ))
        sys.exit(exitcode)

"""

.. program:: cyme-agent

``cyme-agent``
=============

Starts the cyme agent service.

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

    Custom instance directory (default is :file:`instances/``)
    Must be writeable by the user cyme-agent runs as.

.. cmdoption:: -C, --numc

    Number of controllers to start, to handle simultaneous
    requests.  Each controller requires one AMQP connection.
    Default is 2.

.. cmdoption:: --sup-interval

    Supervisor schedule Interval in seconds.  Default is 5.

"""

from __future__ import absolute_import

import atexit
import os


from celery.bin.base import daemon_options
from celery.platforms import (create_pidlock, detached,
                              signals, set_process_title)
from celery.log import colored
from celery.utils import get_cls_by_name, instantiate
from cl.utils import shortuuid

from .base import CymeCommand, Option

BANNER = """
 -------------- cyme@%(id)s v%(version)s
---- **** -----
--- * ***  * -- [Configuration]
-- * - **** ---   . url:         http://%(addr)s:%(port)s
- ** ----------   . broker:      %(broker)s
- ** ----------   . logfile:     %(logfile)s@%(loglevel)s
- ** ----------   . sup:         interval=%(sup.interval)s
- ** ----------   . presence:    interval=%(presence.interval)s
- *** --- * ---   . controllers: #%(controllers)s
-- ******* ----   . instancedir: %(instance_dir)s
--- ***** -----
 -------------- http://celeryproject.org
"""


class Command(CymeCommand):
    agent_ready_sig = "cyme.agent.signals.agent_ready"
    agent_cls = "cyme.agent.Agent"
    default_detach_logfile = "agent.log"
    default_detach_pidfile = "agent.pid"
    name = "cyme-agent"
    args = '[optional port number, or ipaddr:port]'
    option_list = CymeCommand.option_list + (
        Option('--broker', '-b',
            default=None, action="store", dest="broker",
            help="""Broker URL to use for agent connection.\
                    Default is amqp://guest:guest@localhost:5672//"""),
        Option('--detach',
            default=False, action="store_true", dest="detach",
            help="Detach and run in the background."),
        Option("-i", "--id",
               default=None, action="store", dest="id",
               help="Set explicit agent id."),
        Option("--without-httpd",
               default=False, action="store_true", dest="without_httpd",
               help="Disable HTTP server"),
       Option('-l', '--loglevel',
              default="WARNING", action="store", dest="loglevel",
              help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL"),
       Option('-D', '--instance-dir',
              default=None, action="store", dest="instance_dir",
              help="Custom instance dir. Default is instances/"),
       Option('-C', '--numc',
              default=2, action="store", type="int", dest="numc",
              help="Number of controllers to start.  Default is 2"),
       Option('--sup-interval',
              default=60, action="store", type="int", dest="sup_interval",
              help="Supervisor schedule interval.  Default is every minute."),
    ) + daemon_options(default_detach_pidfile)

    help = 'Starts the cyme agent'

    def handle(self, *args, **kwargs):
        """Handle the management command."""
        kwargs = self.prepare_options(**kwargs)
        self.enter_instance_dir()
        self.setup_logging()
        self.env.syncdb()
        self.install_cry_handler()
        self.install_rdb_handler()
        self.colored = colored(kwargs.get("logfile"))
        self.agent = instantiate(self.agent_cls, *args,
                                 colored=self.colored, **kwargs)
        print(str(self.colored.cyan(self.banner())))
        get_cls_by_name(self.agent_ready_sig).connect(self.on_agent_ready)
        self.detached = kwargs.get("detach", False)

        return (self._detach if self.detached else self._start)(**kwargs)

    def setup_default_env(self, env):
        env.setup_eventlet()
        env.setup_pool_limit()

    def stop(self):
        self.set_process_title("shutdown...")

    def on_agent_ready(self, sender=None, **kwargs):
        pid = os.getpid()
        self.set_process_title("ready")
        if not self.detached and \
                not self.agent.is_enabled_for("INFO"):
            print("(%s) agent ready" % (pid, ))
        sender.info("[READY] (%s)" % (pid, ))

    def _detach(self, logfile=None, pidfile=None, uid=None, gid=None,
            umask=None, working_directory=None, **kwargs):
        print("detaching... [pidfile=%s logfile=%s]" % (pidfile, logfile))
        with detached(logfile, pidfile, uid, gid, umask, working_directory):
            return self._start(pidfile=pidfile)

    def _start(self, pidfile=None, **kwargs):
        self.set_process_title("boot")
        self.install_signal_handlers()
        if pidfile:
            pidlock = create_pidlock(pidfile).acquire()
            atexit.register(pidlock.release)
        try:
            return self.agent.start().wait()
        except SystemExit:
            self.agent.stop()

    def banner(self):
        agent = self.agent
        addr, port = agent.addrport
        con = agent.controllers
        try:
            pres_interval = con[0].thread.presence.interval
        except AttributeError:
            pres_interval = "(disabled)"
        sup = agent.supervisor.thread
        return BANNER % {"id": agent.id,
                         "version": self.__version__,
                         "broker": agent.connection.as_uri(),
                         "loglevel": self.LOG_LEVELS[agent.loglevel],
                         "logfile": agent.logfile or "[stderr]",
                         "addr": addr or "localhost",
                         "port": port or 8000,
                         "sup.interval": sup.interval,
                         "presence.interval": pres_interval,
                         "controllers": len(con),
                         "instance_dir": self.instance_dir}

    def install_signal_handlers(self):

        def raise_SystemExit(signum, frame):
            raise SystemExit()

        for signal in ("TERM", "INT"):
            signals[signal] = raise_SystemExit

    def set_process_title(self, info):
        set_process_title("%s#%s" % (self.name, shortuuid(self.agent.id)),
                          "%s (-D %s)" % (info, self.instance_dir))

    def repr_controller_id(self, c):
        return shortuuid(c) + c[-2:]

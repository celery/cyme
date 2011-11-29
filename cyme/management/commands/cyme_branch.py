"""

.. program:: cyme-branch

``cyme-branch``
===============

Starts the cyme branch service.

Options
-------

.. cmdoption:: -i, --id

    Set branch id, if not provided one will be automatically generated.

.. cmdoption:: --without-httpd

    Disable the HTTP server thread.

.. cmdoption:: -l, --loglevel

    Set custom log level. One of DEBUG/INFO/WARNING/ERROR/CRITICAL.
    Default is INFO.

.. cmdoption:: -f, --logfile

    Set custom log file path. Default is :file:`<stderr>`

.. cmdoption:: -D, --instance-dir

    Custom instance directory (default is :file:`instances/``)
    Must be writeable by the user cyme-branch runs as.

.. cmdoption:: -C, --numc

    Number of controllers to start, to handle simultaneous
    requests.  Each controller requires one AMQP connection.
    Default is 2.

.. cmdoption:: --sup-interval

    Supervisor schedule Interval in seconds.  Default is 5.

"""

from __future__ import absolute_import
from __future__ import with_statement

import atexit
import os

from importlib import import_module

from celery import current_app as celery
from celery.bin.base import daemon_options
from celery.platforms import (create_pidlock, detached,
                              signals, set_process_title)
from celery.utils import instantiate
from cl.utils import cached_property, shortuuid

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
    branch_cls = "cyme.branch.Branch"
    default_detach_logfile = "b ranch.log"
    default_detach_pidfile = "branch.pid"
    name = "cyme-branch"
    args = '[optional port number, or ipaddr:port]'
    help = 'Starts a cyme branch'
    option_list = tuple(CymeCommand().option_list) + (
        Option('--broker', '-b',
            default=None, action="store", dest="broker",
            help="""Broker URL to use for the cyme message bus.\
                    Default is amqp://guest:guest@localhost:5672//"""),
        Option('--detach',
            default=False, action="store_true", dest="detach",
            help="Detach and run in the background."),
        Option("-i", "--id",
               default=None, action="store", dest="id",
               help="Set explicit branch id."),
        Option("-X", "--no-interaction",
               default=False, action="store_true", dest="no_interaction",
               help="Don't ask questions"),
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

    _startup_pbar = None
    _shutdown_pbar = None

    def handle(self, *args, **kwargs):
        kwargs = self.prepare_options(**kwargs)
        self.loglevel = kwargs.get("loglevel")
        self.logfile = kwargs.get("logfile")
        self.enter_instance_dir()
        self.env.syncdb(interactive=False)
        self.install_cry_handler()
        self.install_rdb_handler()
        self.colored = celery.log.colored(kwargs.get("logfile"))
        self.branch = instantiate(self.branch_cls, *args,
                                 colored=self.colored, **kwargs)
        self.connect_signals()
        print(str(self.colored.cyan(self.banner())))

        self.detached = kwargs.get("detach", False)
        return (self._detach if self.detached else self._start)(**kwargs)

    def setup_default_env(self, env):
        env.setup_eventlet()
        env.setup_pool_limit()

    def stop(self):
        self.set_process_title("shutdown...")

    def on_branch_ready(self, sender=None, **kwargs):
        if self._startup_pbar:
            self._startup_pbar.finish()
            self._startup_pbar = None
        pid = os.getpid()
        self.set_process_title("ready")
        if not self.detached and \
                not self.branch.is_enabled_for("INFO"):
            print("(%s) branch ready" % (pid, ))
        sender.info("[READY] (%s)" % (pid, ))

    def on_branch_shutdown(self, sender=None, **kwargs):
        if self._shutdown_pbar:
            self._shutdown_pbar.finish()
            self._shutdown_pbar = None

    def _detach(self, logfile=None, pidfile=None, uid=None, gid=None,
            umask=None, working_directory=None, **kwargs):
        print("detaching... [pidfile=%s logfile=%s]" % (pidfile, logfile))
        with detached(logfile, pidfile, uid, gid, umask, working_directory):
            return self._start(pidfile=pidfile)

    def _start(self, pidfile=None, **kwargs):
        self.setup_logging(logfile=self.logfile, loglevel=self.loglevel)
        self.set_process_title("boot")
        self.install_signal_handlers()
        if pidfile:
            pidlock = create_pidlock(pidfile).acquire()
            atexit.register(pidlock.release)
        try:
            return self.branch.start().wait()
        except SystemExit:
            self.branch.stop()

    def banner(self):
        branch = self.branch
        addr, port = branch.addrport
        con = branch.controllers
        try:
            pres_interval = con[0].thread.presence.interval
        except AttributeError:
            pres_interval = "(disabled)"
        sup = branch.supervisor.thread
        return BANNER % {"id": branch.id,
                         "version": self.__version__,
                         "broker": branch.connection.as_uri(),
                         "loglevel": self.LOG_LEVELS[branch.loglevel],
                         "logfile": branch.logfile or "[stderr]",
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
        set_process_title("%s#%s" % (self.name, shortuuid(self.branch.id)),
                          "%s (-D %s)" % (info, self.instance_dir))

    def repr_controller_id(self, c):
        return shortuuid(c) + c[-2:]

    def connect_signals(self):
        sigs = self.signals
        sigmap = {sigs.branch_startup_request: (self.setup_startup_progress,
                                                self.setup_shutdown_progress),
                  sigs.branch_ready: (self.on_branch_ready, ),
                  sigs.branch_shutdown_complete: (self.on_branch_shutdown, )}
        for sig, handlers in sigmap.iteritems():
            for handler in handlers:
                sig.connect(handler, sender=self.branch)

    def setup_shutdown_progress(self, sender=None, **kwargs):
        from cyme.utils import LazyProgressBar
        if sender.is_enabled_for("DEBUG"):
            return
        c = self.colored
        sigs = (self.signals.thread_pre_shutdown,
                self.signals.thread_pre_join,
                self.signals.thread_post_join,
                self.signals.thread_post_shutdown)
        estimate = (len(sigs) * ((len(sender.components) + 1) * 2)
                    + sum(c.thread.extra_shutdown_steps
                            for c in sender.components))
        text = c.white("Shutdown...").embed()
        p = self._shutdown_pbar = LazyProgressBar(estimate, text,
                                                  c.reset().embed())
        [sig.connect(p.step) for sig in sigs]

    def setup_startup_progress(self, sender=None, **kwargs):
        from cyme.utils import LazyProgressBar
        if sender.is_enabled_for("INFO"):
            return
        c = self.colored
        tsigs = (self.signals.thread_pre_start,
                 self.signals.thread_post_start)
        osigs = (self.signals.httpd_ready,
                 self.signals.supervisor_ready,
                 self.signals.controller_ready,
                 self.signals.branch_ready)

        estimate = (len(tsigs) + ((len(sender.components) + 10) * 2)
                     + len(osigs))
        text = c.white("Startup...").embed()
        p = self._startup_pbar = LazyProgressBar(estimate, text,
                                                 c.reset().embed())
        [sig.connect(p.step) for sig in tsigs + osigs]

    @cached_property
    def signals(self):
        return import_module("cyme.branch.signals")

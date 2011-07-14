from __future__ import absolute_import, with_statement

import errno
import os

from threading import Lock

from anyjson import deserialize
from celery import current_app as celery
from celery import platforms
from celery.bin.celeryd_multi import MultiTool
from kombu.utils import cached_property

from django.db import models
from django.utils.translation import ugettext_lazy as _

from scs.managers import NodeManager, QueueManager
from scs.utils import shellquote


CWD = "/var/run/scs"


class Queue(models.Model):
    objects = QueueManager()

    name = models.CharField(_(u"name"), max_length=128, unique=True)
    exchange = models.CharField(_(u"exchange"), max_length=128,
                                default=None, null=True)
    exchange_type = models.CharField(_(u"exchange type"), max_length=128,
                                     default=None, null=True)
    routing_key = models.CharField(_(u"routing key"), max_length=128,
                                   default=None, null=True)
    is_enabled = models.BooleanField(_("is enabled"), default=True)
    options = models.TextField(null=True)

    class Meta:
        verbose_name = _(u"queue")
        verbose_name_plural = _(u"queues")

    def __unicode__(self):
        return self.name


class Node(models.Model):
    Queue = Queue
    objects = NodeManager()
    mutex = Lock()
    cwd = CWD

    name = models.CharField(_(u"name"), max_length=128, unique=True)
    queues = models.ManyToManyField(Queue, null=True)
    max_concurrency = models.IntegerField(_(u"max concurrency"), default=1)
    min_concurrency = models.IntegerField(_(u"min concurrency"), default=1)
    is_enabled = models.BooleanField(_(u"is enabled"), default=True)

    class Meta:
        verbose_name = _(u"node")
        verbose_name_plural = _(u"nodes")

    def __unicode__(self):
        return self.name

    def enable(self):
        self.is_enabled = True
        self.save()

    def disable(self):
        self.is_enabled = False
        self.save()

    def start(self, **kwargs):
        return self._action("start", **kwargs)

    def stop(self, **kwargs):
        return self._action("stop", **kwargs)

    def restart(self, **kwargs):
        return self._action("restart", **kwargs)

    def alive(self, timeout=1):
        return self.responds_to_signal() and self.responds_to_ping(timeout)

    def stats(self):
        return self._query("stats")

    def autoscale(self, max=None, min=None):
        if max is not None:
            self.max_concurrency = max
        if min is not None:
            self.min_concurrency = min
        self.save()
        return self._query("autoscale", max=max, min=min)

    def _action(self, action, multi="celeryd-multi"):
        with self.mutex:
            argv = ([multi, action, '--suffix=""', "--no-color", self.name]
                  + list(self.argv) + list(self.default_args))
            return self.multi.execute_from_commandline(argv)

    def responds_to_ping(self, timeout=1):
        return True if self._query("ping", timeout=timeout) else False

    def responds_to_signal(self):
        pid = self.getpid()
        if not pid:
            return False
        try:
            os.kill(pid, 0)
        except OSError, exc:
            if exc.errno == errno.ESRCH:
                return False
            raise
        return True

    def _query(self, cmd, **kwargs):
        name = self.name
        timeout = kwargs.pop("timeout", None) or 1
        r = celery.control.broadcast(cmd, arguments=kwargs,
                   destination=[name], reply=True, timeout=timeout)
        if r:
            for reply in r:
                if name in reply:
                    return reply[name]

    def consuming_from(self):
        queues = self._query("active_queues")
        if queues:
            return dict((q["name"], q) for q in queues)
        return {}

    def add_queue(self, name):
        q = self.queues.get(name=name)
        options = deserialize(q.options) if q.options else {}
        exchange = q.exchange if q.exchange is not None else name
        routing_key = q.routing_key if q.routing_key is not None else name
        self._query("add_consumer", queue=name,
                                    exchange=exchange,
                                    exchange_type=q.exchange_type,
                                    routing_key=routing_key,
                                    **options)

    def cancel_queue(self, queue):
        self._query("cancel_consumer", queue=queue.name)

    def getpid(self):
        pidfile = self.pidfile.replace("%n", self.name)
        return platforms.PIDFile(pidfile).read_pid()

    @property
    def strargv(self):
        return " ".join("%s %s" % (k, shellquote(str(v)))
                            for k, v in self.argtuple if v)

    @property
    def argv(self):
        acc = []
        for k, v in self.argtuple:
            if v:
                acc.append(k)
                acc.append(shellquote(str(v)))
        return acc

    @property
    def argtuple(self):
        return (("-Q", self.direct_queue, ), )

    @cached_property
    def multi(self):
        return MultiTool()

    @property
    def direct_queue(self):
        return "dq.%s" % (self.name, )

    @property
    def pidfile(self):
        return os.path.join(self.cwd, "celeryd@%n.pid")

    @property
    def logfile(self):
        return os.path.join(self.cwd, "celeryd@%n.log")

    @property
    def default_args(self):
        return ("--workdir=%s" % (self.cwd, ),
                "--pidfile=%s" % (self.pidfile, ),
                "--logfile=%s" % (self.logfile, ),
                "--autoscale=%s,%s" % (self.max_concurrency,
                                       self.min_concurrency))

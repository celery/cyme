"""cyme.models"""

from __future__ import absolute_import
from __future__ import with_statement

import errno
import os
import shlex
import warnings

from threading import Lock

from anyjson import deserialize
from celery import current_app as celery
from celery import platforms
from celery.bin.celeryd_multi import MultiTool
from celery.utils.encoding import safe_str
from eventlet import Timeout
from kombu.log import anon_logger
from kombu.pools import connections, producers

from django.db import models
from django.utils.translation import ugettext_lazy as _

from . import managers
from ..utils import cached_property, find_symbol

logger = anon_logger("Instance")


def shsplit(s):
    if s:
        return shlex.split(safe_str(s))
    return []


class Broker(models.Model):
    """Broker connection arguments."""
    # XXX I think this model can be removed now that it only contains the URL.
    objects = managers.BrokerManager()

    url = models.CharField(_(u"URL"), max_length=200, unique=True)

    _pool = None

    class Meta:
        verbose_name = _(u"broker")
        verbose_name_plural = _(u"brokers")

    def __unicode__(self):
        return unicode(self.connection.as_uri())

    def as_dict(self):
        """Returns this broker as a dictionary that can be Json encoded."""
        return {"url": self.url}

    @property
    def pool(self):
        """Connection pool for this connection."""
        return connections[self.connection]

    @property
    def producers(self):
        """Producer pool for this connection."""
        return producers[self.connection]

    @cached_property
    def connection(self):
        return celery.broker_connection(self.url)


class App(models.Model):
    """Application"""
    Broker = Broker
    objects = managers.AppManager()

    name = models.CharField(_(u"name"), max_length=128, unique=True)
    broker = models.ForeignKey(Broker, null=True, blank=True)
    arguments = models.TextField(_(u"arguments"), null=True, blank=True)
    extra_config = models.TextField(_(u"extra config"), null=True, blank=True)

    class Meta:
        verbose_name = _(u"app")
        verbose_name_plural = _(u"apps")

    def __unicode__(self):
        return self.name

    def get_broker(self):
        if self.broker is None:
            return self.Broker._default_manager.get_default()
        return self.broker

    def as_dict(self):
        return {"name": self.name,
                "broker": self.get_broker().url,
                "arguments": self.arguments,
                "extra_config": self.extra_config}


class Queue(models.Model):
    """An AMQP queue that can be consumed from by one or more instances."""
    objects = managers.QueueManager()

    name = models.CharField(_(u"name"), max_length=128, unique=True)
    exchange = models.CharField(_(u"exchange"), max_length=128,
                                default=None, null=True, blank=True)
    exchange_type = models.CharField(_(u"exchange type"), max_length=128,
                                     default=None, null=True, blank=True)
    routing_key = models.CharField(_(u"routing key"), max_length=128,
                                   default=None, null=True, blank=True)
    options = models.TextField(null=True, blank=True)
    is_enabled = models.BooleanField(_(u"is enabled"), default=True)
    created_at = models.DateTimeField(_(u"created at"), auto_now_add=True)

    class Meta:
        verbose_name = _(u"queue")
        verbose_name_plural = _(u"queues")

    def __unicode__(self):
        return self.name

    def as_dict(self):
        """Returns dictionary representation of this queue that can be
        Json encoded."""
        return {"name": self.name,
                "exchange": self.exchange,
                "exchange_type": self.exchange_type,
                "routing_key": self.routing_key,
                "options": self.options}


class Instance(models.Model):
    """A celeryd instance."""
    App = App
    Broker = Broker
    Queue = Queue
    MultiTool = MultiTool

    objects = managers.InstanceManager()
    mutex = Lock()

    app = models.ForeignKey(App)
    name = models.CharField(_(u"name"), max_length=128, unique=True)
    _queues = models.TextField(_(u"queues"), null=True, blank=True)
    max_concurrency = models.IntegerField(_(u"max concurrency"), default=1)
    min_concurrency = models.IntegerField(_(u"min concurrency"), default=1)
    pool = models.CharField(_(u"pool"), max_length=128, blank=True, null=True)
    is_enabled = models.BooleanField(_(u"is enabled"), default=True)
    created_at = models.DateTimeField(_(u"created at"), auto_now_add=True)
    _broker = models.ForeignKey(Broker, null=True, blank=True)
    arguments = models.TextField(_(u"arguments"), null=True, blank=True)
    extra_config = models.TextField(_(u"extra config"), null=True, blank=True)

    class Meta:
        verbose_name = _(u"instance")
        verbose_name_plural = _(u"instances")

    def __unicode__(self):
        return self.name

    def __init__(self, *args, **kwargs):
        app = kwargs.get("app")
        if app is None:
            app = kwargs["app"] = self.App._default_manager.get_default()
        if not isinstance(app, self.App):
            kwargs["app"] = self.App._default_manager.get(name=app)
        super(Instance, self).__init__(*args, **kwargs)

    def as_dict(self):
        """Returns dictionary representation of this instance that
        can be Json encoded."""
        return {"name": self.name,
                "queues": self.queues,
                "max_concurrency": self.max_concurrency,
                "min_concurrency": self.min_concurrency,
                "is_enabled": self.is_enabled,
                "broker": self.broker.url,
                "pool": self.pool,
                "arguments": self.arguments,
                "extra_config": self.extra_config}

    def enable(self):
        """Enables this instance (model-only)."""
        self.is_enabled = True
        self.save()

    def disable(self):
        """Disables this instance (model-only)."""
        self.is_enabled = False
        self.save()

    def start(self, **kwargs):
        """Starts the instance."""
        return self._action("start", **kwargs)

    def stop(self, **kwargs):
        """Shuts down the instance."""
        return self._action("stop", **kwargs)

    def restart(self, **kwargs):
        """Restarts the instance."""
        return self._action("restart", **kwargs)

    def stop_verify(self, **kwargs):
        """Shuts down the instance, waiting until the worker exits"""
        return self._action("stop_verify", **kwargs)

    def alive(self, **kwargs):
        """Returns :const:`True` if the pid responds to signals,
        and the instance responds to ping broadcasts."""
        return self.responds_to_signal() and self.responds_to_ping(**kwargs)

    def stats(self, **kwargs):
        """Returns instance statistics (like ``celeryctl inspect stats``)."""
        return self._query("stats", **kwargs)

    def _update_autoscale(self, max=None, min=None):
        if max is not None:
            self.max_concurrency = max
        if min is not None:
            self.min_concurrency = min
        self.save()
        return [self.max_concurrency, self.min_concurrency]

    def autoscale(self, max=None, min=None, **kwargs):
        """Set max/min autoscale settings."""
        self._update_autoscale(max, min)
        return self._query("autoscale", dict(max=max, min=min), **kwargs)

    def responds_to_ping(self, **kwargs):
        """Returns :const:`True` if the instance responds to
        broadcast ping."""
        return True if self._query("ping", **kwargs) else False

    def responds_to_signal(self):
        """Returns :const:`True` if the pid file exists and the pid
        responds to signals."""
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

    def consuming_from(self, **kwargs):
        """Returns the queues the instance is currently consuming from."""
        queues = self._query("active_queues", **kwargs)
        return dict((q["name"], q) for q in queues) if queues else {}

    def add_queue_eventually(self, q):
        """Add queue to instance (model-only)."""
        self.queues.add(q)
        self.save()
        return self

    def remove_queue_eventually(self, q):
        """Remove queue from instance (model-only)."""
        self.queues.remove(q)
        self.save()
        return self

    def add_queue(self, q, **kwargs):
        """Add queue for this instance by name."""
        if isinstance(q, self.Queue):
            q = q.as_dict()
        else:
            queues = find_symbol(self, "..branch.controller.queues")
            try:
                q = queues.get(q)
            except queues.NoRouteError:
                self.queues.remove(q)
                self.save()
                warnings.warn(
                    "Removed unknown consumer: %r from %r" % (q, self.name))
                return
        name = q["name"]
        options = deserialize(q["options"]) if q.get("options") else {}
        exchange = q["exchange"] if q["exchange"] else name
        routing_key = q["routing_key"] if q["routing_key"] else name
        return self._query("add_consumer",
                           dict(queue=q["name"],
                                exchange=exchange,
                                exchange_type=q["exchange_type"],
                                routing_key=routing_key,
                                **options), **kwargs)

    def cancel_queue(self, queue, **kwargs):
        """Cancel queue for this instance by :class:`Queue`."""
        queue = queue.name if isinstance(queue, self.Queue) else queue
        return self._query("cancel_consumer", dict(queue=queue), **kwargs)

    def getpid(self):
        """Get the process id for this instance by reading its pid file.

        Returns :const:`None` if the pid file does not exist.

        """
        return platforms.PIDFile(
                self.pidfile.replace("%n", self.name)).read_pid()

    def get_arguments(self):
        return (list(self.default_args)
              + shsplit(self.app.arguments)
              + shsplit(self.arguments))

    def get_extra_config(self):
        return (list(self.default_config)
              + shsplit(self.app.extra_config)
              + shsplit(self.extra_config))

    def _action(self, action, multi="celeryd-multi"):
        """Execute :program:`celeryd-multi` command."""
        with self.mutex:
            argv = ([multi, action, "--nosplash", '--suffix=""', "--no-color"]
                  + [self.name]
                  + self.get_arguments()
                  + ["--"]
                  + self.get_extra_config())
            logger.info(" ".join(argv))
            return self.multi.execute_from_commandline(argv)

    def _query(self, cmd, args={}, **kwargs):
        """Send remote control command and wait for this instances reply."""
        timeout = kwargs.setdefault("timeout", 3)
        name = self.name
        producer = None
        if "connection" not in kwargs:
            producer = self.broker.producers.acquire(block=True, timeout=3)
            kwargs.update(connection=producer.connection,
                          channel=producer.channel)
        try:
            try:
                with Timeout(timeout):
                    r = celery.control.broadcast(cmd,
                                                 arguments=args, reply=True,
                                                 destination=[name], **kwargs)
            except Timeout:
                return None
            return self.my_reply(r)
        finally:
            if producer is not None:
                producer.release()

    def my_reply(self, replies):
        name = self.name
        for reply in replies or []:
            if name in reply:
                return reply[name]

    @cached_property
    def multi(self):
        env = os.environ.copy()
        env.pop("CELERY_LOADER", None)
        return self.MultiTool(env=env)

    @property
    def direct_queue(self):
        return "dq.%s" % (self.name, )

    @property
    def pidfile(self):
        return self.instance_dir / "worker.pid"

    @property
    def logfile(self):
        return self.instance_dir / "worker.log"

    @property
    def statedb(self):
        return self.instance_dir / "worker.statedb"

    @property
    def default_args(self):
        return ("--broker='%s'" % (self.broker.url, ),
                "--workdir='%s'" % (self.instance_dir, ),
                "--pidfile=%s" % (self.pidfile, ),
                "--logfile='%s'" % (self.logfile, ),
                "--queues='%s'" % (self.direct_queue, ),
                "--statedb='%s'" % (self.statedb, ),
                "--events",
                "--pool='%s'" % (self.pool or self.default_pool, ),
                "--loglevel=INFO",
                "--include=cyme.tasks",
                "--autoscale='%s,%s'" % (self.max_concurrency,
                                       self.min_concurrency))

    @property
    def default_config(self):
        return ("celeryd.prefetch_multiplier=10       \
                 celery.acks_late=yes                 \
                 celery.task_result_expires=3600      \
                 celery.send_task_sent_event=yes".split())

    @property
    def broker(self):
        if self._broker is None:
            return self.app.get_broker()
        return self._broker

    def _get_queues(self):
        instance = self

        class Queues(list):

            def add(self, queue):
                if queue not in self:
                    self.append(queue)
                    self._update_obj()

            def remove(self, queue):
                try:
                    list.remove(self, queue)
                except ValueError:
                    pass
                self._update_obj()

            def as_str(self):

                def mq(q):
                    if isinstance(q, Queue):
                        return q.name
                    return q

                return ",".join(map(mq, self))

            def _update_obj(self):
                instance.queues = self.as_str()

        return Queues(q for q in (self._queues or "").split(",") if q)

    def _set_queues(self, queues):
        if not isinstance(queues, basestring):
            queues = ",".join(queues)
        self._queues = queues

    queues = property(_get_queues, _set_queues)

    @cached_property
    def default_pool(self):
        return find_symbol(self, "..conf.CYME_DEFAULT_POOL")

    @cached_property
    def instance_dir(self):
        dir = find_symbol(self, "..conf.CYME_INSTANCE_DIR") / self.name
        dir.mkdir()
        return dir

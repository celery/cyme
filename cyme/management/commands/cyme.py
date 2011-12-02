"""

.. program:: cyme

``cyme``
========

Cyme management utility

Options
-------

.. cmdoption:: -a, --app

    Application to use.  Required for all operations except for
    when creating, deleting or listing apps.

.. cmdoption:: -n, --nowait

    Don't want for operations to complete (async).

.. cmdoption:: -F, --format

    Output format: pretty (default) or json.

.. cmdoption:: -L, --local

    Flag that if set means that operations will be performed
    locally for the current branch only.
    This means the instance directory must be properly set.

.. cmdoption:: -l, --loglevel

    Set custom log level. One of DEBUG/INFO/WARNING/ERROR/CRITICAL.
    Default is INFO.

.. cmdoption:: -f, --logfile

    Set custom logfile path. Default is :file:`<stderr>`

.. cmdoption:: -D, --instance-dir

    Custom instance directory (default is :file:`instances/`)
    Must be readable by the current user.

    This needs to be properly specified if the :option:`-L` option
    is used.

.. cmdoption:: -b, --broker

    Broker to use for a local `branches` request.

"""

from __future__ import absolute_import

import anyjson
import os
import pprint

from functools import partial
from inspect import getargspec


from celery import current_app as celery
from cyme.client import Client
from cyme.client.base import Model
from cyme.utils import cached_property, instantiate

from .base import CymeCommand, Option, die


try:
    import json as _json   # Python 2.6+
    json_pretty = partial(_json.dumps, indent=4)
except ImportError:
    json_pretty = anyjson.serialize  # noqa


class I(object):

    def __init__(self, app=None, format=None, nowait=False,
            url=None, **kwargs):
        self.url = url
        self.app = app
        self.format = format or "pretty"
        self.nowait = nowait
        self.actions = {
            "branches": {
                "all": self.all_branches,
            },
            "apps": {
                "all": self.all_apps,
                "get": self.get_app,
                "add": self.add_app,
                "delete": self.delete_app},
            "instances": {
                "all": self.all_instances,
                "get": self.get_instance,
                "add": self.add_instance,
                "delete": self.delete_instance,
                "stats": self.instance_stats,
                "autoscale": self.instance_autoscale},
            "queues": {
                "all": self.all_queues,
                "get": self.get_queue,
                "add": self.add_queue,
                "delete": self.delete_queue},
            "consumers": {
                "all": self.all_consumers,
                "add": self.add_consumer,
                "delete": self.delete_consumer},
        }
        self.needs_app = ("instances", "queues")
        self.formats = {"jsonp": json_pretty,
                        "json": anyjson.serialize,
                        "pprint": pprint.pformat}

    def getsig(self, fun, opt_args=None):
        spec = getargspec(fun)
        args = spec.args[:-len(spec.defaults) if spec.defaults else None]
        if args[0] == "self":
            args = args[1:]
        if spec.defaults:
            opt_args = dict(zip(spec.args[len(spec.defaults):], spec.defaults))
        return len(args), args, opt_args

    def _ni(self, *args, **kwargs):
        raise NotImplementedError("subclass responsibility")
    all_apps = get_app = add_app = delete_app = \
        all_instances = get_instances = add_instance = delete_instance = \
            instance_stats = instance_autoscale = \
                all_queues = get_queue = add_queue = delete_queue = \
                    all_consumers = add_consumer = delete_consumer = _ni

    def DISPATCH(self, fqdn, *args):
        type, _, action = fqdn.partition('.')
        if not self.app and type in self.needs_app:
            die("Need to specify --app")
        try:
            handler = self.actions[type][action or "all"]
        except KeyError:
            if type:
                die("No action %r for type %r" % (action, type))
            die("Missing type")
        try:
            response = handler(*args)
        except TypeError:
            raise
            arity, args, optargs = self.getsig(handler)
            die("%s.%s requires %s argument%s: %s %s" % (
                type, action, arity, "s" if arity > 1 else "",
                " ".join(args), self.format_optargs(optargs)))
        return self.format_response(self.prepare_response(response))

    def format_optargs(self, optargs):
        if optargs:
            return " ".join("[%s]" % (k, ) for k in optargs.keys())
        return ''

    def format_response(self, ret):
        return self.formats[self.format](ret)

    def prepare_response(self, ret):
        return ret


class WebI(I):

    def all_branches(self):
        return list(self.client.branches)

    def all_apps(self):
        return list(self.client.all())

    def get_app(self, name):
        return self.client.get(name).info

    def add_app(self, name, broker=None, arguments=None, extra_config=None):
        return self.client.add(name, nowait=self.nowait,
                               broker=broker, arguments=arguments,
                               extra_config=extra_config).info

    def delete_app(self, name):
        return self.client.delete(name, nowait=self.nowait)

    def all_instances(self):
        return list(self.client.instances.all())

    def get_instance(self, name):
        return self.client.instances.get(name)

    def add_instance(self, name=None, broker=None, arguments=None,
            extra_config=None):
        return self.client.instances.add(name=name, broker=broker,
                                         arguments=arguments,
                                         extra_config=extra_config,
                                         nowait=self.nowait)

    def delete_instance(self, name):
        return self.client.instances.get(name).delete(nowait=self.nowait)

    def instance_stats(self, name):
        return self.client.instances.get(name).stats()

    def instance_autoscale(self, name, max=None, min=None):
        return self.client.instances.get(name).autoscale(max=max, min=min)

    def all_consumers(self, instance_name):
        return list(self.client.instances.get(instance_name).consumers)

    def add_consumer(self, instance_name, queue_name):
        return self.client.instances.get(instance_name)\
                    .consumers.add(queue_name, nowait=self.nowait)

    def delete_consumer(self, instance_name, queue_name):
        return self.client.instances.get(instance_name)\
                    .consumers.delete(queue_name, nowait=self.nowait)

    def all_queues(self):
        return list(self.client.queues.all())

    def get_queue(self, name):
        return self.client.queues.get(name)

    def add_queue(self, name, exchange=None, exchange_type=None,
            routing_key=None, options=None):
        options = anyjson.deserialize(options) if options else {}
        return self.client.queues.add(name, exchange=exchange,
                                            exchange_type=exchange_type,
                                            routing_key=routing_key,
                                            nowait=self.nowait,
                                            **options)

    def delete_queue(self, name):
        return self.client.queues.delete(name, nowait=self.nowait)

    def _part(self, p):
        if isinstance(p, Model):
            return dict((k, v) for k, v in p.to_python().iteritems()
                            if not k.startswith("_"))
        return p

    def prepare_response(self, ret):
        if isinstance(ret, (list, tuple)):
            return map(self._part, ret)
        return self._part(ret)

    @cached_property
    def client(self):
        client = Client(self.url)
        if self.app:
            return client.get(self.app)
        return client


class LocalI(I):

    def __init__(self, *args, **kwargs):
        super(LocalI, self).__init__(*args, **kwargs)
        self.broker = kwargs.get("broker")
        self.limit = kwargs.get("limit")
        from cyme.branch.controller import apps, instances, queues
        self.get_app = apps.get
        self.apps = apps.state
        self.instances = instances.state
        self.queues = queues.state

    def all_branches(self):
        from cyme.branch.controller import Branch
        args = [self.broker] if self.broker else []
        conn = celery.broker_connection(*args)
        return Branch(connection=conn).all(limit=self.limit)

    def all_apps(self):
        return [app.as_dict() for app in self.apps.objects.all()]

    def get_app(self):
        return self.apps.objects.get(app=self.app)

    def add_app(self, name, broker=None, arguments=None, extra_config=None):
        return self.apps.add(name, broker=broker, arguments=arguments,
                                   extra_config=extra_config)

    def delete_app(self, name):
        self.apps.delete(name)
        return {"ok": "ok"}

    def all_instances(self):
        return [instance.as_dict()
                    for instance in self.instances.objects.filter(
                        app=self.get_app(self.app))]

    def get_instance(self, name):
        return self.instances.get(name, app=self.app)

    def add_instance(self, name=None, broker=None, arguments=None,
            extra_config=None):
        return self.instances.add(name, broker=broker, app=self.app,
                                  arguments=arguments,
                                  extra_config=extra_config)

    def delete_instance(self, name):
        return {"ok": self.instances.remove(name)}

    def instance_stats(self, name):
        return self.instances.stats(name)

    def _get_instance(self, name):
        return self.instances.objects.get(name=name)

    def instance_autoscale(self, name, max=None, min=None):
        return dict(zip(["max", "min"],
                    self._get_instance(name=name)._update_autoscale(max, min)))

    def all_consumers(self, instance_name):
        return self._get_instance(name=instance_name).consuming_from()

    def add_consumer(self, instance_name, queue_name):
        self._get_instance(name=instance_name)\
                .add_queue_eventually(queue_name)
        return {"ok": "ok"}

    def delete_consumer(self, instance_name, queue_name):
        self._get_instance(name=instance_name)\
                .remove_queue_eventually(queue_name)
        return {"ok": "ok"}

    def all_queues(self):
        return [queue.as_dict()
                    for queue in self.queues.objects.all()]

    def get_queue(self, name):
        return self.queues.get(name)

    def add_queue(self, name, exchange=None, exchange_type=None,
            routing_key=None, options=None):
        options = anyjson.deserialize(options) if options else {}
        return self.queues.add(name, exchange=exchange,
                                     exchange_type=exchange_type,
                                     routing_key=routing_key,
                                     **options)

    def delete_queue(self, name):
        self.instances.remove_queue_from_all(name)
        return {"ok": self.queues.delete(name)}


class Command(CymeCommand):
    name = "cyme"
    args = """type command [args]
E.g.:
    cyme apps
    cyme apps.add <name> [broker URL] [arguments] [extra config]
    cyme apps.[get|delete] <name>

    cyme -a <app> instances
    cyme -a <app> instances.add [name] [broker URL] [arguments] [extra config]
    cyme -a <app> instances.[get|delete|stats] <name>
    cyme -a <app> instances.autoscale <name> [max] [min]

    cyme -a <app> queues
    cyme -a <app> queues.add <name> [exchange] [type] [rkey] [opts]
    cyme -a <app> queues.[get|delete] <name>

    cyme start-all
    cyme shutdown-all

    cyme createsuperuser

    cyme shell
    """
    option_list = tuple(CymeCommand().option_list) + (
       Option('-a', '--app',
              default=None, action="store", dest="app",
              help="application to use"),
       Option("-n", "--nowait",
              default=False, action="store_true", dest="nowait",
              help="Don't want for operations to complete (async)."),
       Option("-F", "--format",
              default="jsonp", action="store", dest="format",
              help="Output format: jsonp (default) json or pprint"),
       Option("-L", "--local",
              default=False, action="store_true", dest="local",
              help="Perform operations locally for this branch only."),
       Option('-l', '--loglevel',
              default="WARNING", action="store", dest="loglevel",
              help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL"),
       Option('-D', '--instance-dir',
              default=None, action="store", dest="instance_dir",
              help="Custom instance dir. Default is instances/"),
       Option('-u', '--url',
              default="http://localhost:8000", dest="url",
              help="Custom URL"),
       Option('-b', '--broker',
              default="None", dest="broker",
              help="Broker to use for a local branches request"),
    )

    help = 'Cyme management utility'

    def handle(self, *args, **kwargs):
        local = kwargs.pop("local", False)
        kwargs = self.prepare_options(**kwargs)
        self.commands = {"shell": self.drop_into_shell,
                         "sh": self.drop_into_shell,
                         "start-all": self.start_all,
                         "restart-all": self.restart_all,
                         "shutdown-all": self.shutdown_all,
                         "createsuperuser": self.create_superuser}
        self.setup_logging(**kwargs)
        args = list(args)
        self.enter_instance_dir()
        if local:
            self.env.syncdb()
        if args:
            if args[0] in self.commands:
                return self.commands[args[0]]()
            print((LocalI if local else WebI)(**kwargs).DISPATCH(*args))
        else:
            self.print_help()

    def start_all(self):
        self.status.start_all()

    def restart_all(self):
        self.status.restart_all()

    def shutdown_all(self):
        self.status.shutdown_all()

    def drop_into_shell(self):
        from cyme.utils import setup_logging
        if os.environ.get("CYME_LOG_DEBUG"):
            setup_logging("DEBUG")
        self.env.management.call_command("shell")

    def create_superuser(self):
        self.env.management.call_command("createsuperuser")

    @cached_property
    def status(self):
        return instantiate(self, "cyme.status.Status")

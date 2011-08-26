"""

.. program:: scs-agent

``scs-agent``
=============

Starts the SCS agent service.

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
    locally for the current agent only.
    This means the instance directory must be properly set.

.. cmdoption:: -l, --loglevel

    Set custom log level. One of DEBUG/INFO/WARNING/ERROR/CRITICAL.
    Default is INFO.

.. cmdoption:: -f, --logfile

    Set custom logfile path. Default is :file:`<stderr>`

.. cmdoption:: -D, --instance-dir

    Custom instance directory (deafult is :file:`instances/`)
    Must be readable by the current user.

    This needs to be properly specified if the :option:`-L` option
    is used.

"""

from __future__ import absolute_import

import anyjson
import os
import pprint

from inspect import getargspec

from scs.client import Client
from scs.client.base import Model
from scs.utils import cached_property

from .base import SCSCommand, Option, die


class I(object):

    def __init__(self, app=None, format=None, nowait=False, **kwargs):
        self.app = app
        self.format = format or "pretty"
        self.nowait = nowait
        self.actions = {
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
        self.formats = {"json": anyjson.serialize,
                        "pretty": pprint.pformat}

    def getsig(self, fun, opt_args=None):
        spec = getargspec(fun)
        args = spec.args[:len(spec.defaults) if spec.defaults else None][1:]
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
            raise  # XXX
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

    def all_apps(self):
        return list(self.client.all())

    def get_app(self, name):
        return self.client.get(name).info

    def add_app(self, name, broker=None):
        return self.client.add(name, nowait=self.nowait, broker=broker).info

    def delete_app(self, name):
        return self.client.delete(name, nowait=self.nowait)

    def all_instances(self):
        return list(self.client.instances.all())

    def get_instance(self, name):
        return self.client.instances.get(name)

    def add_instance(self, name=None, broker=None):
        return self.client.instances.add(name=name, broker=broker,
                                         nowait=self.nowait)

    def delete_instance(self, name):
        return self.client.delete(name, nowait=self.nowait)

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
        client = Client()
        if self.app:
            return client.get(self.app)
        return client


class LocalI(I):

    def __init__(self, *args, **kwargs):
        super(LocalI, self).__init__(*args, **kwargs)
        from scs.agent.controller import apps, nodes, queues
        self.get_app = apps.get
        self.apps = apps.state
        self.nodes = nodes.state
        self.queues = queues.state

    def all_apps(self):
        return [app.as_dict() for app in self.apps.objects.all()]

    def get_app(self):
        return self.apps.objects.get(app=self.app)

    def add_app(self, name):
        return self.apps.add(name)

    def delete_app(self, name):
        self.apps.delete(name)
        return {"ok": "ok"}

    def all_instances(self):
        return [node.as_dict()
                    for node in self.nodes.objects.filter(
                        app=self.get_app(self.app))]

    def get_instance(self, name):
        return self.nodes.get(name, app=self.app)

    def add_instance(self, name=None, broker=None):
        return self.nodes.add(name, broker=broker, app=self.app)

    def delete_instance(self, name):
        return {"ok": self.nodes.remove(name)}

    def instance_stats(self, name):
        return self.nodes.stats(name)

    def _get_node(self, name):
        return self.nodes.objects.get(name=name)

    def instance_autoscale(self, name, max=None, min=None):
        return dict(zip(["max", "min"],
                    self._get_node(name=name)._update_autoscale(max, min)))

    def all_consumers(self, instance_name):
        return self._get_node(name=instance_name).consuming_from()

    def add_consumer(self, instance_name, queue_name):
        self._get_node(name=instance_name).add_queue_eventually(queue_name)
        return {"ok": "ok"}

    def delete_consumer(self, instance_name, queue_name):
        self._get_node(name=instance_name).remove_queue_eventually(queue_name)
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
        self.nodes.remove_queue_from_all(name)
        return {"ok": self.queues.delete(name)}


class Command(SCSCommand):
    name = "scs"
    args = """type command [args]
E.g.:
    scs apps
    scs apps.add <name> [broker URL]
    scs apps.[get|delete] <name>

    scs -a <app> instances
    scs -a <app> instances.add [name] [broker URL]
    scs -a <app> instances.[get|delete|stats] <name>
    scs -a <app> instances.autoscale <name> [max] [min]

    scs -a <app> queues
    scs -a <app> queues.add <name> [exchange] [type] [rkey] [opts]
    scs -a <app> queues.[get|delete] <name>

    scs shell
    """
    option_list = SCSCommand.option_list + (
       Option('-a', '--app',
              default=None, action="store", dest="app",
              help="application to use"),
       Option("-n", "--nowait",
              default=False, action="store_true", dest="nowait",
              help="Don't want for operations to complete (async)."),
       Option("-F", "--format",
              default="pretty", action="store", dest="format",
              help="Output format: pretty (default) or json"),
       Option("-L", "--local",
              default=False, action="store_true", dest="local",
              help="Perform operations locally for this agent only."),
       Option('-l', '--loglevel',
              default="WARNING", action="store", dest="loglevel",
              help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL"),
       Option('-D', '--instance-dir',
              default=None, action="store", dest="instance_dir",
              help="Custom instance dir. Default is instances/"),
    )

    help = 'SCS management utility'

    def handle(self, *args, **kwargs):
        local = kwargs.pop("local", False)
        kwargs = self.prepare_options(**kwargs)
        args = list(args)
        self.enter_instance_dir()
        if local:
            self.env.syncdb()
        if args:
            if args[0] in ("shell", "sh"):
                return self.drop_into_shell()
            print((LocalI if local else WebI)(**kwargs).DISPATCH(*args))
        else:
            self.print_help()

    def drop_into_shell(self):
        from scs.utils import setup_logging
        if os.environ.get("SCS_LOG_DEBUG", False):
            setup_logging("DEBUG")
        self.env.management.call_command("shell")

"""cyme.api.views"""

from __future__ import absolute_import
from __future__ import with_statement

import re

from celery import current_app as celery
from celery.result import AsyncResult

from . import web
from ..branch.controller import apps, branches, instances, queues
from ..tasks import webhook
from ..utils import uuid


class Branch(web.ApiView):

    def get(self, request, branch=None):
        return branches.get(branch) if branch else branches.all()


class App(web.ApiView):

    def get(self, request, app=None):
        return apps.get(app).as_dict() if app else apps.all()

    def put(self, request, app=None):
        return self.Created(apps.add(app or uuid(),
                            **self.params("broker", "arguments",
                                          "extra_config")))
    post = put

    def delete(self, request, app):
        return apps.delete(app)


class Instance(web.ApiView):

    def get(self, request, app, name=None, nowait=False):
        return instances.get(name) if name else instances.all(app=app)

    def delete(self, request, app, name, nowait=False):
        return self.Ok(instances.remove(name, nowait=nowait))

    def post(self, request, app, name=None, nowait=False):
        return self.Created(instances.add(name=name, app=app,
                                      nowait=nowait,
                                      **self.params("broker", "pool",
                                                    "arguments",
                                                    "extra_config")))

    def put(self, *args, **kwargs):
        return self.NotImplemented("Operation is not idempotent: use POST")


class Consumer(web.ApiView):

    def get(self, request, app, name, queue=None, nowait=False):
        return instances.consuming_from(name)

    def put(self, request, app, name, queue, nowait=False):
        return self.Created(instances.add_consumer(name, queue, nowait=nowait))
    post = put

    def delete(self, request, app, name, queue, nowait=False):
        return self.Ok(instances.cancel_consumer(name, queue, nowait=nowait))


class Queue(web.ApiView):

    def get(self, request, app, name=None):
        return queues.get(name) if name else queues.all()

    def delete(self, request, app, name, nowait=False):
        return self.Ok(queues.delete(name))

    def put(self, request, app, name, nowait=False):
        return self.Created(queues.add(name, nowait=nowait,
                      **self.params("exchange", "exchange_type",
                                    "routing_key", "options")))
    post = put


class apply(web.ApiView):
    get_methods = frozenset(["GET", "HEAD"])
    re_find_queue = re.compile(r'/?(.+?)/?$')
    re_url_in_path = re.compile(r'(.+?/)(\w+://)(.+)')

    def prepare_path(self, rest):
        path, url = self._parse_path_containing_url(rest)
        if path:
            m = self.re_find_queue.match(path)
            if m:
                return m.groups()[0], url
        return None, url

    def dispatch(self, request, app, rest):
        gd = lambda m: getattr(request, m)
        queue, url = self.prepare_path(rest)
        app = apps.get(app)
        broker = app.get_broker()
        method = request.method.upper()
        pargs = {}
        if queue:
            queue = queues.get(queue)
            pargs.update(exchange=queue["exchange"],
                         exchange_type=queue["exchange_type"],
                         routing_key=queue["routing_key"])
        params = gd(method) if method in self.get_methods else gd("GET")
        data = gd(method) if method not in self.get_methods else None

        with broker.producers.acquire(block=True) as producer:
            publisher = celery.amqp.TaskPublisher(
                            connection=producer.connection,
                            channel=producer.channel)
            result = webhook.apply_async((url, method, params, data),
                                         publisher=publisher, retry=True,
                                         **pargs)
            return self.Accepted({"uuid": result.task_id, "url": url,
                                  "queue": queue, "method": method,
                                  "params": params, "data": data,
                                  "broker": producer.connection.as_uri()})

        def _parse_path_containing_url(self, rest):
            m = self.re_url_in_path.match(rest)
            if m:
                first, scheme, last = m.groups()
                if scheme:
                    return first, scheme + last
                return first, None
            return rest, None


class autoscale(web.ApiView):

    def get(self, request, app, name):
        instance = instances.get(name)
        return {"max": instance["max_concurrency"],
                "min": instance["min_concurrency"]}

    def post(self, request, app, name, nowait=False):
        return self.Ok(instances.autoscale(name, nowait=nowait,
                        **self.params(("max", int), ("min", int))))


@web.simple_get
def instance_stats(self, request, app, name):
    return instances.stats(name)


@web.simple_get
def task_state(self, request, app, uuid):
    return {"state": AsyncResult(uuid).state}


@web.simple_get
def task_result(self, request, app, uuid):
    return {"result": AsyncResult(uuid).result}


@web.simple_get
def task_wait(self, request, app, uuid):
    return {"result": AsyncResult(uuid).get()}


@web.simple_get
def ping(self, request):
    return {"ok": "pong"}

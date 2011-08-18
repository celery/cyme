"""scs.views"""

from __future__ import absolute_import

import httplib as http
import re
import sys

from functools import partial
from traceback import format_exception

from django.http import HttpResponse
from django.views.generic.base import View

from anyjson import serialize
from celery import current_app as celery
from celery.result import AsyncResult
from cl.exceptions import NoReplyError
from cl.common import uuid
from cl.pools import producers
from kombu.utils.encoding import safe_repr

from .controller import apps, nodes, queues
from .tasks import webhook

GET_METHODS = frozenset(["GET", "HEAD"])
RE_URL_IN_PATH = re.compile(r'(.+?/)(\w+://)(.+)')


class HttpResponseTimeout(HttpResponse):
    status_code = http.REQUEST_TIMEOUT


def JsonResponse(data, status=http.OK, **kwargs):
    if isinstance(data, (basestring, int, float, bool)):
        data = {"ok": data}
    if data is None or not isinstance(data, (dict, list, tuple)):
        return data
    kwargs.setdefault("content_type", "application/json")
    response = HttpResponse(serialize(data),
                            status=status, **kwargs)
    response.csrf_exempt = True
    return response

Accepted = partial(JsonResponse, status=http.ACCEPTED)
Created = partial(JsonResponse, status=http.CREATED)
Error = partial(JsonResponse, status=http.INTERNAL_SERVER_ERROR)

def short(fun):
    return type(fun.__name__, (JsonView, ), {
        "__module__": fun.__module__, "__doc__": fun.__doc__, "get": fun})


def _parse_path_containing_url(rest):
    m = RE_URL_IN_PATH.match(rest)
    if m:
        first, scheme, last = m.groups()
        if scheme:
            return first, scheme + last
        return first, None
    return rest, None


def get_or_post(request, key, default=None):
    for d in (request.GET, request.POST):
        try:
            return d[key]
        except KeyError:
            pass
    return default


class JsonView(View):

    def dispatch(self, *args, **kwargs):
        try:
            data = super(JsonView, self).dispatch(*args, **kwargs)
        except NoReplyError:
            return HttpResponseTimeout()
        except Exception, exc:
            return Error({"nok": [safe_repr(exc),
                                  "".join(format_exception(*sys.exc_info()))]})
        return JsonResponse(data)


class App(JsonView):

    def get(self, request, app=None):
        return apps.get(app).as_dict() if app else apps.all()

    def put(self, request, app=None):
        port = get_or_post(request, "port")
        return Created(apps.add(app or uuid(),
                hostname=get_or_post(request, "hostname"),
                port=int(port) if port else None,
                userid=get_or_post(request, "userid"),
                password=get_or_post(request, "password"),
                virtual_host=get_or_post(request, "virtual_host")))
    post = put


class Instance(JsonView):

    def get(self, request, app, name=None):
        return nodes.get(name) if name else nodes.all(app=app)

    def delete(self, request, app, name):
        return nodes.remove(name)

    def put(self, request, app, name=None):
        return Created(nodes.add(name=name, app=app))
    post = put


@short
def InstanceStats(self, request, app, name):
    return nodes.stats(name)


class Autoscale(JsonView):

    def get(self, request, app, name):
        node = nodes.get(name)
        return {"max": node["max_concurrency"], "min": node["min_concurrency"]}

    def post(self, request, app, name):
        return nodes.autoscale(name, max=get_or_post(request, "max"),
                                     min=get_or_post(request, "min"))


class Consumer(JsonView):

    def get(self, request, app, name, queue=None):
        return nodes.consuming_from(name)

    def put(self, request, app, name, queue):
        return Created(nodes.add_consumer(name, queue))
    post = put

    def delete(self, request, app, name, queue):
        return nodes.cancel_consumer(name, queue)


@short
def State(self, request, app, uuid):
    return {"state": AsyncResult(uuid).state}



@short
def Result(self, request, app, uuid):
    return {"result": AsyncResult(uuid).result}


@short
def Wait(self, request, app, uuid):
    return {"result": AsyncResult(uuid).get()}


class Queue(JsonView):

    def get(self, request, app, name=None):
        return queues.get(name) if name else queues.all()

    def delete(self, request, app, name):
        return queues.delete(name)

    def put(self, request, app, name):
        return Created(queues.add(name,
                    exchange=get_or_post(request, "exchange"),
                    exchange_type=get_or_post(request, "exchange_type"),
                    routing_key=get_or_post(request, "routing_key"),
                    options=get_or_post(request, "options")))
    post = put


class Apply(JsonView):
    status_code = http.ACCEPTED
    re_find_queue = re.compile(r'/?(.+?)/?$')

    def prepare_path(self, rest):
        path, url = _parse_path_containing_url(rest)
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
        params = gd(method) if method in GET_METHODS else gd("GET")
        data = gd(method) if method not in GET_METHODS else None

        with producers[broker.connection()].acquire(block=True) as producer:
            publisher = celery.amqp.TaskPublisher(
                            connection=producer.connection,
                            channel=producer.channel)
            result = webhook.apply_async((url, method, params, data),
                                         publisher=publisher, retry=True,
                                         **pargs)
            return Accepted({"uuid": result.task_id, "url": url,
                             "queue": queue, "method": method,
                             "params": params, "data": data,
                             "broker": producer.connection.as_uri()})

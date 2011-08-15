"""scs.views"""

from __future__ import absolute_import

import re
import httplib as http

from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.views.decorators.csrf import csrf_exempt
from django.views.generic.base import View

from anyjson import serialize
from celery.result import AsyncResult
from cl.exceptions import NoReplyError

from .controller import nodes, queues

RE_URL_IN_PATH = re.compile(r'(.+?/)(\w+://)(.+)')


class HttpResponseTimeout(HttpResponse):
    status_code = http.REQUEST_TIMEOUT


def JsonResponse(data, status=http.OK, **kwargs):
    if isinstance(data, basestring):
        data = {"ok": data}
    kwargs.setdefault("content_type", "application/json")
    response = HttpResponse(serialize(data),
                            status=status, **kwargs)
    response.csrf_exempt = True
    return response


def _parse_path_containing_url(rest):
    m = RE_URL_IN_PATH.match(rest)
    if m:
        first, scheme, last = m.groups()
        if scheme:
            return first, scheme + last
        else:
            return first, None
    return rest, None


def get_or_post(request, key, default=None):
    try:
        return request.GET[key]
    except KeyError:
        try:
            return request.POST[key]
        except KeyError:
            return default


class JsonView(View):
    status_code = http.OK

    def dispatch(self, *args, **kwargs):
        try:
            data = super(JsonView, self).dispatch(*args, **kwargs)
        except NoReplyError:
            return HttpResponseTimeout()
        return self.Response(data)

    def Response(self, data):
        return JsonResponse(data, status=self.status_code)



class Instance(JsonView):

    def get(self, request, app, name=None):
        if name:
            return nodes.get(name)
        return nodes.all()

    def delete(self, request, app, name):
        return nodes.remove(name)

    def put(self, request, app, name=None):
        node = nodes.add(name=name)
        self.status_code = http.CREATED
        return node
    post = put


# TODO Must use broker of app!
class Consumer(JsonView):

    def put(self, request, app, name, queue):
        ret = nodes.add_consumer(name, queue)
        self.status_code = http.CREATED
        return ret
    post = put

    def delete(self, request, app, name, queue):
        return nodes.cancel_consumer(name, queue)


class State(JsonView):

    def get(self, request, app, uuid):
        return {"state": AsyncResult(uuid).state}


class Result(JsonView):

    def get(self, request, app, uuid):
        return {"result": AsyncResult(uuid).result}


class Wait(JsonView):

    def get(self, request, app, uuid):
        return {"result": AsyncResult(uuid).get()}


class Queue(JsonView):

    def get(self, request, app, name=None):
        if name:
            return queues.get(name)
        return queues.all()

    def delete(self, request, app, name):
        return queues.delete(name)

    def put(self, request, app, name):
        print(request.POST.copy())
        queue = queues.add(name,
                    exchange=get_or_post(request, "exchange"),
                    exchange_type=get_or_post(request, "exchange_type"),
                    routing_key=get_or_post(request, "routing_key"),
                    options=get_or_post(request, "options"))
        self.status_code = http.CREATED
        return queue
    post = put


class Apply(JsonView):
    status_code = http.ACCEPTED
    re_find_queue = re.compile(r'/(.+?)/?$')

    def prepare_path(self, rest):
        path, url = parse_path_containing_url(rest)
        if path:
            m = self.re_find_queue.match(path)
            if m:
                return m.groups()[0], url
        return None, url

    def dispatch(self, request, app, rest):
        queue, url = self.prepare_path()
        method = self.method
        data = getattr(request, method.upper())

        return self.Response({"url": url, "queue": queue,
                              "method": method, "data": data})

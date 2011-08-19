"""scs.web"""

from __future__ import absolute_import

import httplib as http
import sys

from functools import partial
from traceback import format_exception

from django.http import HttpResponse, HttpResponseNotFound
from django.views.generic.base import View

from anyjson import serialize
from cl.exceptions import NoReplyError, NoRouteError
from kombu.utils.encoding import safe_repr


class HttpResponseTimeout(HttpResponse):
    """The operation timed out."""
    status_code = http.REQUEST_TIMEOUT


class HttpResponseNotImplemented(HttpResponse):
    """The requested action is not implemented.
    Used for async requests when the operation is inherently sync."""
    status_code = http.NOT_IMPLEMENTED


def JsonResponse(data, status=http.OK, **kwargs):
    """Returns a JSON encoded response."""
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


class ApiView(View):
    nowait = False
    _semipredicate = object()

    def dispatch(self, *args, **kwargs):
        self.nowait = kwargs.get("nowait", False)
        try:
            data = super(ApiView, self).dispatch(*args, **kwargs)
        except NoRouteError:
            return HttpResponseNotFound()
        except NoReplyError:
            return HttpResponseTimeout()
        except Exception, exc:
            return Error({"nok": [safe_repr(exc),
                                  "".join(format_exception(*sys.exc_info()))]})
        return self.Response(data)

    def Response(self, *args, **kwargs):
        return JsonResponse(*args, **kwargs)

    def Accepted(self, *args, **kwargs):
        return Accepted(*args, **kwargs)

    def Ok(self, data, *args, **kwargs):
        if self.nowait:
            return self.Accepted({"ok": "operation scheduled"}, **kwargs)
        return self.Response(data, *args, **kwargs)

    def Created(self, data, *args, **kwargs):
        if self.nowait:
            return self.Accepted({"ok": "operation scheduled"}, **kwargs)
        return Created(data, *args, **kwargs)

    def NotImplemented(self, *args, **kwargs):
        return HttpResponseNotImplemented(*args, **kwargs)

    def get_or_post(self, key, default=None):
        for d in (self.request.GET, self.request.POST):
            try:
                return d[key]
            except KeyError:
                pass
        return default

    def params(self, *keys):
        return dict((key, self.get_param(key)) for key in keys)

    def get_param(self, key, type=None):
        if isinstance(key, (list, tuple)):
            key, type = key
        value = self.get_or_post(key, self._semipredicate)
        if type and value != self._semipredicate:
            return type(value)
        return value


def simple_get(fun):
    return type(fun.__name__, (ApiView, ), {
        "__module__": fun.__module__, "__doc__": fun.__doc__, "get": fun})

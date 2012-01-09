"""cyme.api.web

- Contains utilities for creating our HTTP API.

"""

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

# Cross Origin Resource Sharing
# See: http://www.w3.org/TR/cors/
ACCESS_CONTROL = {
        "Allow-Origin": "*",
        "Allow-Methods": ["GET", "OPTIONS", "POST", "PUT", "DELETE"],
        "Max-Age": 86400,
}


class HttpResponseTimeout(HttpResponse):
    """The operation timed out."""
    status_code = http.REQUEST_TIMEOUT


class HttpResponseNotImplemented(HttpResponse):
    """The requested action is not implemented.
    Used for async requests when the operation is inherently sync."""
    status_code = http.NOT_IMPLEMENTED


def set_access_control_options(response, options=None):
    options = dict(ACCESS_CONTROL, **options or {})
    try:
        options["Allow-Methods"] = ", ".join(options["Allow-Methods"] or [])
    except KeyError:
        pass

    for key, value in ACCESS_CONTROL.iteritems():
        response["Access-Control-%s" % (key, )] = value


def JsonResponse(data, status=http.OK, access_control=None, **kwargs):
    """Returns a JSON encoded response."""
    if isinstance(data, (basestring, int, float, bool)):
        data = {"ok": data}
    if data is None or not isinstance(data, (dict, list, tuple)):
        return data
    kwargs.setdefault("content_type", "application/json")
    response = HttpResponse(serialize(data),
                            status=status, **kwargs)
    set_access_control_options(response, access_control)
    response.csrf_exempt = True
    return response
Accepted = partial(JsonResponse, status=http.ACCEPTED)
Created = partial(JsonResponse, status=http.CREATED)
Error = partial(JsonResponse, status=http.INTERNAL_SERVER_ERROR)


class ApiView(View):
    nowait = False  # should the current operation be async?
    typemap = {int: lambda i: int(i) if i else None,
               float: lambda f: float(f) if f else None}
    _semipredicate = object()

    def dispatch(self, request, *args, **kwargs):
        self.nowait = kwargs.get("nowait", False)
        if request.method.lower() == "get":
            kwargs.pop("nowait", None)
            if self.nowait:
                return self.NotImplemented("Operation can't be async.")
        try:
            data = super(ApiView, self).dispatch(request, *args, **kwargs)
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
            data = data or {"ok": "operation scheduled"}
            return self.Accepted(data, **kwargs)
        return self.Response(data, *args, **kwargs)

    def Created(self, data, *args, **kwargs):
        if self.nowait:
            data = data or {"ok": "operation scheduled"}
            return self.Accepted(data, **kwargs)
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
        return dict(self.get_param(key) for key in keys)

    def get_param(self, key, type=None):
        semipredicate = self._semipredicate
        if isinstance(key, (list, tuple)):
            key, type = key
            type = self.typemap.get(type, type)
        value = self.get_or_post(key, semipredicate)
        if type and value != semipredicate:
            return key, type(value)
        return key, None if value == semipredicate else value


def simple_get(fun):
    return type(fun.__name__, (ApiView, ), {"get": fun,
                    "__module__": fun.__module__, "__doc__": fun.__doc__})

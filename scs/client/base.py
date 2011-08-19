"""scs.client.base"""

from __future__ import absolute_import

import anyjson
import requests

from urllib import quote

from celery.datastructures import AttributeDict

from .. import __version__, DEBUG
from ..utils import cached_property


class Path(object):

    def __init__(self, s=None, stack=None):
        self.stack = (stack or []) + (s.stack if isinstance(s, Path) else [s])

    def __str__(self, s="/"):
        return (s + s.join(map(quote, filter(None, self.stack))).strip(s) + s)

    def __div__(self, other):
        return Path(other, self.stack)


class Base(object):

    def serialize(self, obj):
        return anyjson.serialize(obj)

    def deserialize(self, text):
        return anyjson.deserialize(text)

    def __getitem__(self, key):
        return self.get(key)

    def __delitem__(self, key):
        return self.delete(key)

    def keys(self):
        return self.all()

    def __iter__(self):
        return iter(self.keys())


class Model(object):
    repr = "Model"
    repr_field = "name"
    fields = ()

    def __init__(self, parent, *args, **kwargs):
        self.parent = parent
        if len(args) == 1 and isinstance(args[0], dict):
            kwargs.update(args[0])

        for field in self.fields:
            setattr(self, field, None)
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    def delete(self):
        return self.parent.delete(self.name)

    def __repr__(self):
        return "<%s: %r>" % (self.repr,
                             getattr(self, self.repr_field, None) or "(none)")


class Section(Base):
    Model = Model
    name = None
    path = None
    proxy = ["GET", "POST", "PUT", "DELETE"]

    def __init__(self, client):
        self.client = client
        if self.name is None:
            self.name = self.__class__.__name__.lower()
        for attr in self.proxy:
            setattr(self, attr, getattr(self.client, attr))
        self.path = Path(self.name) if self.path is None else self.path

    def all(self):
        return self.GET(self.path)

    def get(self, name):
        return self.GET(self.path / name, type=self.create_model)

    def add(self, name, **data):
        return self.PUT(self.path / name, type=self.create_model, data=data)

    def delete(self, name):
        return self.DELETE(self.path / name)

    def create_model(self, *args, **kwargs):
        return self.Model(self, *args, **kwargs)

    def __repr__(self):
        return repr(self.all())


class Client(Base):
    default_url = "http://127.0.0.1:8000"

    def __init__(self, url=None):
        self.url = url.rstrip("/") if url else self.default_url

    def GET(self, path, params=None, type=None):
        return self.request("GET", path, params, None, type)

    def PUT(self, path, params=None, data=None, type=None):
        return self.request("PUT", path, params, data, type)

    def POST(self, path, params=None, data=None, type=None):
        return self.request("POST", path, params, data, type)

    def DELETE(self, path, params=None, data=None, type=None):
        return self.request("DELETE", path, params, data, type)

    def request(self, method, path, params=None, data=None, type=None):
        return self._request(method, self.build_url(path), params, data, type)

    def _prepare(self, d):
        if d:
            return dict((key, value if value is not None else "")
                            for key, value in d.iteritems())

    def _request(self, method, url, params=None, data=None, type=None):
        if DEBUG:
            print("<REQ> %s %r data=%r params=%r" % (method, url,
                                                     data, params))
        data = self._prepare(data)
        params = self._prepare(params)
        type = type or AttributeDict
        r = requests.request(method, str(url),
                             headers=self.headers,
                             params=params, data=data)
        data = None
        if DEBUG:
            data = r.read()
            print("<RES> %r" % (data, ))
        if r.ok:
            if data is None:
                data = r.read()
            ret = self.deserialize(data)
            if isinstance(ret, dict):
                return type(ret)
            return ret
        r.raise_for_status()

    def root(self, method, path=None, params=None, data=None):
        return self._request(method,
                             self.url + str(Path(path) if path else ""),
                             params, data)

    def __repr__(self):
        return "<Client: %r>" % (self.url, )

    @cached_property
    def headers(self):
        return {"Accept": "application/json",
                "User-Agent": "scs-client %r" % (__version__, )}

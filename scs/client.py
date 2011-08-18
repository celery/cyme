import anyjson
import requests

from functools import partial
from urllib import quote

from celery.datastructures import AttributeDict
from kombu.utils import cached_property

from . import __version__





class Path(object):

    def __init__(self, s=None, stack=None):
        self.stack = (stack or []) + (s.stack if isinstance(s, Path) else [s])

    def __str__(self, s="/"):
        return (s + s.join(map(quote, filter(None, self.stack))).strip(s) + s)

    def __div__(self, other):
        return Path(other, self.stack)

class Base(object):

    def __getitem__(self, key):
        return self.get(key)

    def __delitem__(self, key):
        return self.delete(key)

    def keys(self):
        return self.all()

    def __iter__(self):
        return iter(self.keys())


class Section(Base):
    name = None
    path = None

    def __init__(self, client):
        self.client = client
        if self.name is None:
            self.name = self.__class__.__name__.lower()
        if self.path is None:
            self.path = Path(self.name)

    def GET(self, *args, **kwargs):
        return self.client.GET(*args, **kwargs)

    def PUT(self, *args, **kwargs):
        return self.client.PUT(*args, **kwargs)

    def POST(self, *args, **kwargs):
        return self.client.POST(*args, **kwargs)

    def DELETE(self, *args, **kwargs):
        return self.client.DELETE(*args, **kwargs)




class Instances(Section):

    def all(self):
        return self.GET(self.path)

    def add(self, name=None):
        return self.PUT(self.path / name)

    def get(self, name):
        return self.GET(self.path / name)

    def delete(self, name):
        return self.DELETE(self.path / name)


class Queues(Section):

    def all(self):
        return self.GET(self.path)

    def get(self, name):
        return self.GET(self.path / name)

    def delete(self, name):
        return self.DELETE(self.path / name)

    def add(self, name, exchange=None, exchange_type=None,
            routing_key=None, **options):
        options = anyjson.serialize(options) if options else None
        return self.PUT(self.path / name,
                        data={"exchange": exchange,
                              "exchange_type": exchange_type,
                              "routing_key": routing_key,
                              "options": options})

    def stats(self, name):
        return self.GET(self.path / name / "stats")

    def autoscale(self, name, max=None, min=None):
        return self.POST(self.path / name / "autoscale",
                         params={"max": max, "min": min})


class Consumers(Section):
    path = Path("instances")

    def add(self, instance, queue):
        return self.PUT(self.path / instance, "queues" / queue)

    def remove(self, instance, queue):
        return self.PUT(self.path / instance / "queues" / queue)



class Client(Base):
    Consumers = Consumers
    Instances = Instances
    Queues = Queues

    app = "scs"

    def __init__(self, url=None, app=None, info=None):
        self.app = app
        self.url = url.rstrip("/") if url else "http://localhost:8000"
        self.instances = self.Instances(self)
        self.queues = self.Queues(self)
        self.consumers = self.Consumers(self)
        self.info = info or {}

    def __repr__(self):
        return "<Client: app=%s %r>" % (self.app or "(none)", self.info)

    def clone(self, app=None, info=None):
        return self.__class__(url=self.url, app=app, info=info)

    def _to_app(self, name, info):
        return self.clone(app=name, info=info)

    def add(self, name, hostname=None, port=None, userid=None,
            password=None, virtual_host=None):
        return self._to_app(name, self.root("PUT", name,
                              data={"hostname": hostname,
                                    "port": port,
                                    "userid": userid,
                                    "password": password,
                                    "virtual_host": virtual_host}))

    def get(self, name=None):
        return self._to_app(name, self.root("GET", name or self.name))

    def delete(self, name=None):
        return self.root("DELETE", name or self.name)

    def all(self):
        return self.root("GET")

    def GET(self, path, params=None):
        return self.request("GET", path, params=params)

    def PUT(self, path, params=None, data=None):
        return self.request("PUT", path, params=params, data=data)

    def POST(self, path, params=None, data=None):
        return self.request("POST", path, params=params, data=data)

    def DELETE(self, path, params=None, data=None):
        return self.request("DELETE", path, params=params, data=data)

    def request(self, method, path, params=None, data=None):
        return self._request(method, self.build_url(path), params, data)

    def _request(self, method, url, params=None, data=None):
        print("REQ: %s" % (url, ))
        r = requests.request(method, str(url),
                             headers=self.headers,
                             params=params, data=data)
        if r.ok:
            ret = anyjson.deserialize(r.read())
            if isinstance(ret, dict):
                return AttributeDict(ret)
            return ret
        r.raise_for_status()

    def build_url(self, path):
        url = self.url
        if self.app:
            url += "/" + self.app
        return url + str(path)

    def root(self, method, path=None, params=None, data=None):
        return self._request(method,
                             self.url + str(Path(path) if path else ""),
                             params, data)

    @cached_property
    def headers(self):
        return {"Accept": "application/json",
                "User-Agent": "scs-client %r" % (__version__, )}


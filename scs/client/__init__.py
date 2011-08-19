"""scs.client"""

from __future__ import absolute_import

from . import base


# XXX `requests` does not currently seem to support using the
#     data argument with PUT requests.


class Instance(base.Model):
    pass


class Queue(base.Model):
    pass


class Client(base.Client):
    app = "scs"

    class Instances(base.Section):

        class Model(Instance):
            repr = "Instance"
            fields = ("name", "broker", "min_concurrency", "max_concurrency",
                      "queues", "is_enabled")

            class Consumers(base.Section):

                def __init__(self, client, name):
                    base.Section.__init__(self, client)
                    self.path = self.client.path / name / "queues"

                def add(self, queue):
                    if isinstance(queue, Queue):
                        queue = queue.name
                    return self.POST(self.path / queue)

                def delete(self, queue):
                    if isinstance(queue, Queue):
                        queue = queue.name
                    return self.DELETE(self.path / queue)

                def all(self):
                    return self.GET(self.path)

            def __init__(self, *args, **kwargs):
                base.Model.__init__(self, *args, **kwargs)
                self.consumers = self.Consumers(self.parent, name=self.name)
                self.path = self.parent.path / self.name

            def stats(self):
                return self.parent.stats(self.name)

            def autoscale(self, max=None, min=None):
                return self.parent.autoscale(self.name, max=max, min=min)

        def add(self, name=None):
            # name is optional for instances
            return base.Section.add(self, name)

        def stats(self, name):
            return self.GET(self.path / name / "stats")

        def autoscale(self, name, max=None, min=None):
            return self.POST(self.path / name / "autoscale",
                             params={"max": max, "min": min})

    class Queues(base.Section):

        class Model(Queue):
            repr = "Queue"
            fields = ("name", "exchange", "exchange_type",
                      "routing_key", "options")

        def delete(self, name):
            return self.DELETE(self.path / name)

        def add(self, name, exchange=None, exchange_type=None,
                routing_key=None, **options):
            options = self.serialize(options) if options else None
            return self.POST(self.path / name,
                            data={"exchange": exchange,
                                "exchange_type": exchange_type,
                                "routing_key": routing_key,
                                "options": options}, type=self.create_model)

    def __init__(self, url=None, app=None, info=None):
        super(Client, self).__init__(url)
        self.app = app
        self.instances = self.Instances(self)
        self.queues = self.Queues(self)
        self.info = info or {}

    def add(self, name, hostname=None, port=None, userid=None,
            password=None, virtual_host=None):
        return self.create_model(name, self.root("POST", name,
                                 data={"hostname": hostname,
                                       "port": port,
                                        "userid": userid,
                                        "password": password,
                                        "virtual_host": virtual_host}))

    def get(self, name=None):
        return self.create_model(name, self.root("GET", name or self.app))

    def delete(self, name=None):
        return self.root("DELETE", name or self.app)

    def all(self):
        return self.root("GET")

    def build_url(self, path):
        url = self.url
        if self.app:
            url += "/" + self.app
        return url + str(path)

    def create_model(self, name, info):
        return self.clone(app=name, info=base.AttributeDict(info))

    def clone(self, app=None, info=None):
        return self.__class__(url=self.url, app=app, info=info)

    def __repr__(self):
        return "<Client: %r>" % (self.build_url(""), )

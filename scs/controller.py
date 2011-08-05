from kombu import Exchange
from kombu.utils import cached_property

from scs import signals
from scs.consumer import gConsumer
from scs.messaging import ModelConsumer, RPC, Actions
from scs.models import Node, Queue as Q


class ModelRPC(RPC):
    model = None
    sigmap = {}

    def __init__(self, context=None):
        if not self.name:
            self.name = self.model.__name__
        super(ModelRPC, self).__init__(context)

    def Consumer(self, channel, **kwargs):
        return ModelConsumer(channel, self.exchange,
                             callbacks=[self.on_message],
                             sigmap=self.sigmap, model=self.model,
                             **kwargs)


class NodeRPC(ModelRPC):
    model = Node
    exchange = Exchange("scs.nodes")
    sigmap = {"on_create": signals.node_started,
              "on_delete": signals.node_stopped}

    class actions(Actions):

        def add_consumer(self, name, queue):
            pass


class QRPC(ModelRPC):
    model = Q
    exchange = Exchange("scs.queues")

    class actions(Actions):

        def get(self, name):
            try:
                return Q.objects.get(name=name).as_dict()
            except Q.DoesNotExist:
                raise KeyError(name)

        def add(self, name, **declaration):
            return Q.objects.add(name, **declaration).as_dict()

        def delete(self, name):
            Q.objects.filter(name=name).delete()

    def get(self, name):
        try:
            # see if we have the queue locally.
            return self.actions.get(name)
        except KeyError:
            # if not, ask the cluster.
            return self.call("get", args={"name": name})


class Controller(gConsumer):

    def __init__(self, id):
        self.id = id
        super(Controller, self).__init__()

    def get_consumers(self, Consumer, chan):
        return [cls.Consumer(chan) for cls in self.rpc_classes]

    @cached_property
    def nodes(self):
        return NodeRPC()

    @cached_property
    def qs(self):
        return QRPC()

    @cached_property
    def rpc_classes(self):
        return [self.nodes, self.qs]

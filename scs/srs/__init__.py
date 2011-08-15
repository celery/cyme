"""scs.srs"""

from __future__ import absolute_import, with_statement

from datetime import datetime

from kombu import Exchange, Queue
from kombu.utils import cached_property

from cl.common import maybe_declare, send_reply, uuid
from cl.consumers import ConsumerMixin
from cl.pools import producers

from .. import metrics
from ..messaging import ModelConsumer
from ..thread import gThread
from ..utils import rfc2822
from ..models import Node
from ..state import state


class NodeConsumer(ModelConsumer):
    model = Node


class SRSAgent(ConsumerMixin, gThread):
    """The SRS Agent enables the SRS controller to communicate with this
    agent via AMQP.

    :param id: The agent id.

    **Messaging basics**

    The message body must be JSON encoded
    (with a content type of ``application/json``),

    **Creating instances**

    An instance can be created by sending a message
    to the ``srs.create.%(id)s`` fanout exchange, where
    `id` is this agents id.

    The body must contain the name of the new instance (``id``),
    and include the details of a broker the new instance
    can connect to (``hostname``, ``port``, ``userid``, ``password``
    and ``virtual_host``).

    **Updating instances**

    The binds a queue to the ``srs.instance.update`` topic exchange
    for every instance it controls, using the instance id as routing key.

    Messages sent to this exchange must include a ``state`` key,
    where a ``state`` of ``"stopping"`` means that the instance
    will be disabled, and a ``state`` of ``"deleting"`` means
    that the instance will be forgotten about.

    **Querying instances**

    A message sent to the ``srs.agent.query-instances`` fanout exchange
    will result in a reply being sent back to the ``reply`` exchange
    containing a list of all the instances this agent controls
    and their details (broker, max/min concurrency, and queues)

    **Statistics**

    Statistics about this agent and its instances are sent to
    the ``srs.statistics`` fanout exchange every 15 seconds.

    Statistics is a JSON encoded dictionary containing the following
    items:

    .. code-block:: python

        {"agents": {
            my_agent_id: {
                "loadavg": [now, float load1, float load2, float load3],
                "instances": [now, int total_instances,
                                   int total_active_instances],
                "drive_used": [now, int capacity_percentage],
            },
         "instances": {
            instance_id_1: {
                "autoscaler": {
                    "current": int current_procs,
                    "max": int max_procs,
                    "min": int min_procs
                },
                "consumer": {
                    "broker": {
                        "connect_timeout": int,
                        "hostname": unicode,
                        "insist": bool,
                        "login_method": unicode,
                        "port": int,
                        "ssl": bool,
                        "transport": unicode,
                        "transport_options": dict,
                        "userid": unicode,
                        "virtual_host": unicode,
                    },
                },
                "pool": {
                    "max-concurrency": int current_procs,
                    "max-tasks-per-child": int or None,
                    "processes": [list of pids],
                    "put-guarded-by-semaphore": True,
                    "timeouts": [seconds soft_timeout, seconds hard_timeout],
                },
                "total": {task_types_and_total_count},
            }
        }}

    Where ``now`` is an RFC2822 formatted timestamp in the UTC timezone,
    and ``procs`` is a number of either processes, threads or green threads
    depending on the pool type used.

    """

    #: Manager object for all nodes managed by this agent.
    Nodes = Node._default_manager

    #: Exchange used to query available instances.
    query_exchange = Exchange("srs.agent.query-instances",
                              "fanout", auto_delete=True)

    #: Exchange used to request instance updates.
    update_exchange = Exchange("srs.instance.update",
                               "topic", auto_delete=True)

    #: Exchange we publish statistics to.
    stats_exchange = Exchange("srs.statistics",
                              "fanout", auto_delete=True)

    #: Exchange we publish replies to.
    reply_exchange = Exchange("reply", "direct")

    @property
    def create_exchange(self):
        """Create request exchange."""
        return Exchange("srs.create.%s" % (self.id, ),
                        "fanout", auto_delete=True)

    def __init__(self, connection, id):
        self.connection = connection
        self.id = id
        self._create = Queue(uuid(), self.create_exchange,
                             auto_delete=True)
        self._query = Queue(self.id, self.query_exchange, auto_delete=True)
        self.instance_update_consumer = None
        super(SRSAgent, self).__init__()

    def get_consumers(self, Consumer, channel):
        self.instance_update_consumer = NodeConsumer(channel,
                                            self.update_exchange,
                                            callbacks=[self.on_updating])
        return (Consumer(self._create, callbacks=[self.on_updating]),
                Consumer(self._query, callbacks=[self.on_query]),
                self.instance_update_consumer)

    def on_connection_revived(self):
        state.on_broker_revive()

    def before(self):
        self.start_periodic_timer(15, self.publish_stats)

    def get_instance_stats(self):
        return dict((node.name, node.stats())
                        for node in self.Nodes.enabled())

    def gather_stats(self):
        now = rfc2822(datetime.utcnow())
        return {"agents": {self.id: {
                    "loadavg": [now] + list(metrics.load_average()),
                    "instances": [now, self.Nodes.all().count(),
                                       self.Nodes.enabled().count()],
                    "drive_used": [now, metrics.df(Node.cwd).capacity]},
                "instances": self.get_instance_stats()}}

    def on_create(self, body, message):
        node = self.cluster.add(**body)
        self.instance_update_consumer.on_create(node)
        message.ack()

    def _disable_instance_updates_for(self, node):
        self.instance_update_consumer.on_delete(node)

    def on_updating(self, body, message):
        message.ack()

    def on_stopping(self, body, message):
        self._disable_instance_updates_for(
            self.cluster.disable(body["id"]))
        message.ack()

    def on_deleting(self, body, message):
        self._disable_instance_updates_for(
            self.cluster.remove(body["id"]))
        message.ack()

    def on_query(self, body, message):
        send_reply(self.connection, self.reply_exchange,
                   message, [n.as_dict() for n in self.Nodes.enabled()])
        message.ack()

    def publish_stats(self):
        with producers[self.connection].acquire(block=True) as producer:
            maybe_declare(self.stats_exchange, producer.channel)
            producer.publish(self.gather_stats(),
                             exchange=self.stats_exchange.name,
                             routing_key="")

    @cached_property
    def cluster(self):
        from ..agent import cluster
        return cluster

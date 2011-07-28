from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from contextlib import nested, contextmanager
from datetime import datetime
from functools import partial

from celery import current_app as celery
from kombu import Exchange, Queue, Consumer, Producer
from kombu.pools import ProducerPool
from kombu.utils import gen_unique_id, cached_property

from scs import metrics
from scs.thread import gThread
from scs.state import state
from scs.utils import rfc2822
from scs.models import Node


@contextmanager
def consume_from(*consumers):
    with nested(*consumers) as context:
        yield context


class SRSAgent(gThread):
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

    #: Reply exchange.
    reply_exchange = Exchange("reply", "direct")

    #: Exchange used to request instance updates.
    update_exchange = Exchange("srs.instance.update",
                               "topic", auto_delete=True)

    #: Exchange we publish statistics to.
    stats_exchange = Exchange("srs.statistics",
                              "fanout", auto_delete=True)

    #: set of entities we have already declared.
    _declared = set()

    @property
    def create_exchange(self):
        """Create request exchange."""
        return Exchange("srs.create.%s" % (self.id, ),
                        "fanout", auto_delete=True)

    def __init__(self, id):
        self.id = id
        self._create = Queue(gen_unique_id(), self.create_exchange,
                             auto_delete=True)
        self._query = Queue(self.id, self.query_exchange, auto_delete=True)
        self.connection_errors = celery.broker_connection().connection_errors
        self.instance_updates = set()
        self.instance_update_consumer = None
        for node in self.Nodes.enabled():
            self.instance_updates.add(self.create_update_queue(node))
        super(SRSAgent, self).__init__()

    def before(self):
        self.start_periodic_timer(15, self.publish_stats)

    def run(self):
        while 1:
            try:
                self.consume_forever()
            except self.connection_errors:
                self.error("Connection to broker lost. "
                           "Trying to re-establish the connection...",
                           exc_info=sys.exc_info())

    def create_update_queue(self, node):
        return Queue(gen_unique_id(), self.update_exchange, node.name,
                     auto_delete=True)

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
        self.instance_updates.add(
                self.instance_update_consumer.add_queue(
                    self.create_update_queue(node)))
        self.instance_update_consumer.consume()
        message.ack()

    def _disable_instance_updates_for(self, node):
        found_queue = None
        for queue in self.instance_updates:
            if queue.routing_key == node.name:
                found_queue = queue
                break
        if found_queue:
            self.instance_update_consumer.cancel_by_queue(queue.name)

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

    def on_connection_error(self, exc, interval):
        self.error("Broker connection error: %r. "
                   "Trying again in %s seconds." % (exc, interval, ))

    def on_query(self, body, message):
        self.send_reply(message, [n.as_dict()
                                    for n in self.Nodes.enabled()])
        message.ack()

    def consume_forever(self):
        drain_events = self.drain_events

        with celery.broker_connection() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   celery.conf.BROKER_CONNECTION_MAX_RETRIES)
            state.on_broker_revive()
            self.info("Connected to %s" % (conn.as_uri(), ))
            with conn.channel() as channel:
                C = partial(Consumer, channel)
                self.instance_update_consumer = C(self.instance_updates,
                                                  callbacks=[self.on_updating])
                with consume_from(C(self._create, callbacks=[self.on_create]),
                                  C(self._query, callbacks=[self.on_query]),
                                  self.instance_update_consumer):
                    while 1:
                        drain_events(conn, timeout=1)

    def drain_events(self, connection, *args, **kwargs):
        try:
            connection.drain_events(*args, **kwargs)
        except socket.timeout:
            pass
        except socket.error:
            raise

    def maybe_declare(self, entity, channel):
        if entity not in self._declared:
            entity(channel).declare()
            self._declared.add(entity)

    def publish_stats(self):
        with self.producers.acquire(block=True) as producer:
            self.maybe_declare(self.stats_exchange, producer.channel)
            producer.publish(self.gather_stats(),
                             exchange=self.stats_exchange.name,
                             routing_key="")

    def send_reply(self, req, msg, **props):
        with self.producers.acquire(block=True) as producer:
            self.maybe_declare(self.reply_exchange, producer.channel)
            producer.publish(msg, exchange=self.reply_exchange.name,
                **dict({"routing_key": req.properties["reply_to"],
                        "correlation_id": req.properties["correlation_id"]},
                       **props))

    @cached_property
    def producers(self):
        """producer pool"""
        return ProducerPool(connections=celery.pool, limit=celery.pool.limit)

    @cached_property
    def cluster(self):
        from scs.agent import cluster
        return cluster

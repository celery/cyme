from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from functools import partial

from celery import current_app as celery
from kombu import Exchange, Queue, Consumer, Producer
from kombu.pools import ProducerPool
from kombu.utils import gen_unique_id, cached_property

from scs.thread import gThread
from scs.state import state
from scs.models import Node


class SRSAgent(gThread):
    create_exchange = "srs.create.%s"
    query_exchange = Exchange("srs.agent.query-instances",
                              "fanout", auto_delete=True)
    reply_exchange = Exchange("reply", "direct")
    update_exchange = Exchange("srs.instance.update",
                               "topic", auto_delete=True)
    declared = {}

    def __init__(self, id):
        self.id = id
        create_name = self.create_exchange % (self.id, )
        self._create = Queue(gen_unique_id(),
                             Exchange(create_name, "fanout",
                                      auto_delete=True),
                             auto_delete=True)
        self._query = Queue(self.id, self.query_exchange, auto_delete=True)
        self.connection_errors = celery.broker_connection().connection_errors
        self.instance_updates = set()
        self.instance_update_consumer = None
        for node in Node.objects.enabled():
            self.instance_updates.add(self.create_update_queue(node))
        super(SRSAgent, self).__init__()

    def create_update_queue(self, node):
        return Queue(gen_unique_id(), self.update_exchange, node.name,
                     auto_delete=True)

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
                                    for n in Node.objects.enabled()])
        message.ack()

    def consume(self):
        with celery.broker_connection() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   celery.conf.BROKER_CONNECTION_MAX_RETRIES)
            state.on_broker_revive()
            self.info("Connected to %s" % (conn.as_uri(), ))
            with conn.channel() as channel:
                C = partial(Consumer, channel)
                self.instance_update_consumer = C(self.instance_updates,
                                                  callbacks=[self.on_updating])
                consumers = [C(self._create, callbacks=[self.on_create]),
                             C(self._query, callbacks=[self.on_query]),
                             self.instance_update_consumer]
                [consumer.consume() for consumer in consumers]
                while 1:
                    try:
                        conn.drain_events(timeout=1)
                    except socket.timeout:
                        pass
                    except socket.error:
                        raise

    def maybe_declare(self, entity, channel):
        if entity not in self.declared:
            entity(channel).declare()

    def send_reply(self, req, msg, **props):
        with self.producers.acquire(block=True) as producer:
            self.maybe_declare(self.reply_exchange, producer.channel)
            props = dict({"routing_key": req.properties["reply_to"],
                          "correlation_id": req.properties["correlation_id"]},
                         **props)
            producer.publish(msg, exchange=self.reply_exchange.name, **props)

    def run(self):
        while 1:
            try:
                self.consume()
            except self.connection_errors:
                self.error("Connection to broker lost. "
                           "Trying to re-establish the connection...",
                           exc_info=sys.exc_info())

    @cached_property
    def producers(self):
        return ProducerPool(connections=celery.pool,
                            limit=celery.pool.limit)

    @cached_property
    def cluster(self):
        from scs.agent import cluster
        return cluster

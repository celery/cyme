from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from contextlib import nested, contextmanager
from functools import partial

from celery import current_app as celery
from kombu import Consumer, Exchange
from kombu.pools import ProducerPool
from kombu.utils import cached_property, gen_unique_id

from scs.state import state
from scs.thread import gThread


@contextmanager
def consume_from(*consumers):
    with nested(*consumers) as context:
        yield context


class gConsumer(gThread):
    #: Reply exchange.
    reply_exchange = Exchange("reply", "direct")

    #: set of entities we have already declared.
    _declared = set()

    def __init__(self, *args, **kwargs):
        self.connection_errors = celery.broker_connection().connection_errors
        super(gConsumer, self).__init__(*args, **kwargs)

    def get_consumers(self, Consumer):
        raise NotImplementedError("subclass responsibility")

    def run(self):
        while 1:
            try:
                self.consume_forever()
            except self.connection_errors:
                self.error("Connection to broker lost. "
                           "Trying to re-establish the connection...",
                           exc_info=sys.exc_info())

    def on_connection_error(self, exc, interval):
        self.error("Broker connection error: %r. "
                   "Trying again in %s seconds." % (exc, interval, ))

    def consume_forever(self):
        drain_events = self.drain_events

        with celery.broker_connection() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   celery.conf.BROKER_CONNECTION_MAX_RETRIES)
            state.on_broker_revive()
            self.info("Connected to %s" % (conn.as_uri(), ))
            with conn.channel() as channel:
                with consume_from(
                        *self.get_consumers(partial(Consumer, channel))):
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

    def send_reply(self, req, msg, **props):
        with self.producers.acquire(block=True) as producer:
            self.maybe_declare(self.reply_exchange, producer.channel)
            producer.publish(msg, exchange=self.reply_exchange.name,
                **dict({"routing_key": req.properties["reply_to"],
                        "correlation_id": req.properties["correlation_id"]},
                       **props))

    def find_queue_by_rkey(self, needle, haystack):
        for queue in haystack:
            if queue.routing_key == needle:
                return queue

    def sync_model_queues(self, expected, queues, create):
        for name in expected:
            queues.add(create(name))
        for queue in queues:
            if queue.routing_key not in expected:
                queues.discard(queue)

    def uuid(self):
        return gen_unique_id()

    @cached_property
    def producers(self):
        """producer pool"""
        return ProducerPool(connections=celery.pool, limit=celery.pool.limit)


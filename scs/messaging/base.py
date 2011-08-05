from __future__ import absolute_import
from __future__ import with_statement

import socket

from itertools import count

from celery import current_app as celery
from kombu import Consumer, Exchange, Queue
from kombu.pools import ProducerPool
from kombu.utils import cached_property, gen_unique_id


class MessagingBase(object):

    #: Reply exchange.
    reply_exchange = Exchange("reply", "direct")

    #: set of entities we have already declared.
    _declared = set()

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

    def uuid(self):
        return gen_unique_id()

    def collect_reply(self, conn, channel, ticket, limit=1, timeout=None,
            callback=None):
        responses = []
        q = self.reply_queue(ticket)

        def on_reply(body, message):
            if callback:
                callback(body)
            responses.append(body)

        with Consumer(channel, [q], callbacks=[on_reply], no_ack=True):
            for i in limit and xrange(limit) or count():
                try:
                    conn.drain_events(timeout=timeout)
                except socket.timeout:
                    break
            channel.after_reply_message_received(q.name)
            return responses

    def reply_queue(self, ticket):
        return Queue(ticket, self.reply_exchange, ticket, auto_delete=True)

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
    def connection_errors(self):
        self.connection_errors = celery.broker_connection().connection_errors

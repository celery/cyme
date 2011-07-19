from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from functools import partial

from celery import current_app as celery
from kombu import Exchange, Queue, Consumer, Producer
from kombu.utils import gen_unique_id

from scs.thread import gThread
from scs.state import state


class AMQAgent(gThread):
    create_exchange = "srs.create.%s"
    query_exchange = "srs.agent.query-instances"

    def __init__(self, id):
        self.id = id
        create_name = self.create_exchange % (self.id, )
        self._create = Queue(gen_unique_id(),
                             Exchange(create_name, "fanout",
                                      auto_delete=True),
                             auto_delete=True)
        self._query = Queue(self.id,
                            Exchange(self.query_exchange, "fanout",
                                     auto_delete=True),
                            auto_delete=True)
        self.connection_errors = celery.broker_connection().connection_errors
        super(AMQAgent, self).__init__()

    def on_create(self, body, message):
        print("GOT CREATE MESSAGE")
        message.ack()

    def on_query(self, body, message):
        print("GOT QUERY MESSAGE")
        message.ack()

    def on_connection_error(self, exc, interval):
        self.error("Broker connection error: %r. "
                   "Trying again in %s seconds." % (exc, interval, ))

    def consume(self):
        with celery.broker_connection() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   celery.conf.BROKER_CONNECTION_MAX_RETRIES)
            state.on_broker_revive()
            self.info("Connected to %s" % (conn.as_uri(), ))
            with conn.channel() as channel:
                C = partial(Consumer, channel)
                consumers = [C(self._create, callbacks=[self.on_create]),
                             C(self._query, callbacks=[self.on_query])]
                [consumer.consume() for consumer in consumers]
                while 1:
                    try:
                        conn.drain_events(timeout=1)
                    except socket.timeout:
                        pass
                    except socket.error:
                        raise

    def run(self):
        while 1:
            try:
                self.consume()
            except self.connection_errors:
                self.error("Connection to broker lost. "
                           "Trying to re-establish the connection...",
                           exc_info=sys.exc_info())

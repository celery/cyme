from __future__ import with_statement

from functools import partial

from celery import current_app as celery
from kombu import Exchange, Queue, Consumer, Producer
from kombu.utils import gen_unique_id

from scs.thread import gThread


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


    def on_create(self, body, message):
        print("GOT CREATE MESSAGE")
        message.ack()

    def on_query(self, body, message):
        print("GOT QUERY MESSAGE")
        message.ack()

    def run(self):
        with celery.pool.acquire(block=True) as connection:
            with connection.channel() as channel:
                C = partial(Consumer, channel)
                consumers = [C(self._create, callbacks=[self.on_create]),
                             C(self._query, callbacks=[self.on_query])]
                [consumer.consume() for consumer in consumers]
                while 1:
                    connection.drain_events()

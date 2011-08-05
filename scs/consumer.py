from __future__ import absolute_import
from __future__ import with_statement

import sys

from contextlib import nested, contextmanager
from functools import partial

from celery import current_app as celery
from kombu import Consumer

from scs.state import state
from scs.thread import gThread
from scs.messaging import MessagingBase


class gConsumer(gThread, MessagingBase):

    def get_consumers(self, Consumer, channel):
        raise NotImplementedError("subclass responsibility")

    def run(self):
        while 1:
            try:
                self.consume_forever()
            except self.connection_errors:
                self.error("Connection to broker lost. "
                           "Trying to re-establish the connection...",
                           exc_info=sys.exc_info())

    def consume_forever(self):
        drain_events = self.drain_events

        with celery.broker_connection() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   celery.conf.BROKER_CONNECTION_MAX_RETRIES)
            state.on_broker_revive()
            self.info("Connected to %s" % (conn.as_uri(), ))
            with conn.channel() as channel:
                with self._consume_from(
                        *self.get_consumers(partial(Consumer, channel),
                                            channel)):
                    while 1:
                        drain_events(conn, timeout=1)

    def on_connection_error(self, exc, interval):
        self.error("Broker connection error: %r. "
                   "Trying again in %s seconds." % (exc, interval, ))

    @contextmanager
    def _consume_from(self, *consumers):
        with nested(*consumers) as context:
            yield context

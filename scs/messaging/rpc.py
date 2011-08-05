from __future__ import absolute_import
from __future__ import with_statement

from kombu import Consumer, Exchange
from kombu.utils import kwdict

from scs.messaging.base import MessagingBase


class Actions(object):

    def __init__(self, context=None):
        if context is None:
            context = {}
        self.context = context


class RPC(MessagingBase):
    name = None
    exchange = None

    class actions(Actions):
        pass

    def __init__(self, context=None):
        self.context = context
        self.actions = self.actions(self.context)

    def get_queue(self):
        return Queue(self.uuid(), self.exchange, auto_delete=True)

    def Consumer(self, channel, **kwargs):
        return self.Consumer(channel, self.get_queue,
                             callbacks=[self.on_message], **kwargs)

    def cast(self, method, args, before=None, after=None, **props):
        exchange = self.exchange
        with self.producers.acquire(block=True) as producer:
            if before is not None:
                before(producer.connection, producer.channel)
            self.maybe_declare(exchange, producer.channel)
            producer.publish({"class": self.name,  "method": method, "args": args},
                             exchange=exchange, **props)
            if after is not None:
                return after(producer.connection, producer.channel)

    def call(self, method, args, timeout=None, callback=None, **props):
        ticket = self.uuid()
        reply_q = self.reply_queue(ticket)

        def before(connection, channel):
            reply_q(channel).declare()

        def after(connection, channel):
            return self.collect_reply(connection, channel, ticket,
                                      timeout=timeout, callback=callback)

        return self.cast(method, args, before, after,
                         **dict(props, reply_to=ticket))

    def on_message(self, body, message):
        if message.properties.get("reply_to"):
            self.handle_call(body, message)
        else:
            self.handle_cast(body, message)
        message.ack()

    def handle_cast(self, body, message):
        self._DISPATCH(body)

    def handle_call(self, body, message):
        self.send_reply(message, self._DISPATCH(body))

    def _DISPATCH(self, cls, body):
        try:
            if name.startswith("_"):
                raise AttributeError(name)
            return getattr(self.actions, name)(**kwdict(kwargs))
        except Exception, exc:
            return {"nok": repr(exc)}
        else:
            return {"ok": r}

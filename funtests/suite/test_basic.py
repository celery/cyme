import os
import sys

from time import sleep

# funtest config
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))

from scs.utils import cached_property, uuid
from nose import SkipTest

from .utils import unittest


class ClientTestCase(unittest.TestCase):

    @cached_property
    def Client(self):
        from scs import Client
        return Client


class test_create_app(ClientTestCase):

    def test_create(self):
        client = self.Client()
        app = client.add(uuid())
        self.assertTrue(app)
        self.assertTrue(app.info)
        self.assertTrue(app.info.broker)
        self.assertIn(app.app, app.all())
        app = client.get(app.app)
        self.assertTrue(app)
        app.delete()
        self.assertNotIn(app.app, app.all())


class test_basic(ClientTestCase):

    def setUp(self):
        self.app = self.Client().add(uuid())

    def tearDown(self):
        self.app.delete()

    def test_basic(self):
        app = self.app
        instance = app.instances.add()
        #sleep(5)
        self.assertTrue(instance)
        #instance = app.instances.get(instance.name)
        self.assertTrue(instance)
        self.assertIn(instance.name, app.instances)

        q = uuid()
        expected = dict(exchange=q, exchange_type="topic", routing_key=q)
        queue = app.queues.add(q, **expected)
        print("QUEUE: %r" % (queue, ))
        #queue = app.queues.get(q)
        self.assertTrue(queue)
        #for key, value in expected.items():
        #    self.assertEqual(queue[key], value)
        self.assertIn(q, app.queues)

        app.consumers.add(instance.name, q)
        app.consumers.delete(instance.name, q)

        app.queues.delete(q)
        self.assertNotIn(q, app.queues)

        app.instances.delete(instance.name)


if __name__ == "__main__":
    unittest.main()

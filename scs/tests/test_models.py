from celery.tests.utils import unittest
from mock import Mock

from scs.models import Node, Queue


class test_Queue(unittest.TestCase):

    def test__unicode__(self):
        self.assertTrue(unicode(Queue(name="foo")))

    def test_enabled(self):
        q1 = Queue.objects.add("foo", options={"x": 1})
        q2 = Queue.objects.add("bar")
        q2.is_enabled = False
        q2.save()

        x = Queue.objects.enabled()
        self.assertEqual(x[0], q1)


class test_Node(unittest.TestCase):

    def setUp(self):
        self._mt, Node.MultiTool = Node.MultiTool, Mock()
        self._query, Node._query = Node._query, Mock()

    def tearDown(self):
        Node.MultiTool = self._mt
        Node._query = self._query
        Queue.objects.all().delete()
        Node.objects.all().delete()

    def test__unicode__(self):
        self.assertTrue(unicode(Node(name="foo")))

    def test_add(self):
        n1 = Node.objects.add()
        self.assertTrue(n1.name)
        self.assertTrue(n1.default_args)
        self.assertTrue(n1.direct_queue)
        self.assertTrue(n1.multi)
        self.assertTrue(n1.argtuple)
        self.assertTrue(n1.argv)
        self.assertIsNone(n1.getpid())

        n2 = Node.objects.add("foo")
        self.assertEqual(n2.name, "foo")

        n1.disable()
        self.assertFalse(Node.objects.get(name=n1.name).is_enabled)
        n1.enable()
        self.assertTrue(Node.objects.get(name=n1.name).is_enabled)

    def test_start_stop_restart(self):
        n = Node.objects.add()
        n.start()
        n.stop()
        n.restart()

    def test_add_cancel_queue(self):
        n = Node.objects.add()
        q = Queue.objects.create(name="xiziasd")
        q.save()

        n.add_queue(q)
        n._query.assert_called_with("add_consumer", dict(queue=q.name,
                                    exchange=q.name, routing_key=q.name,
                                    exchange_type=None))

        n.cancel_queue(q)
        n._query.assert_called_with("cancel_consumer", dict(queue=q.name))

    def test_objects(self):
        n = Node.objects.add()
        Node.objects.disable(n)
        Node.objects.enable(n)
        Node.objects.remove(n)

    def test_with_queues(self):
        Node.objects.add(queues="foo,bar,baz")
        Node.objects.add_queue("foo")
        Node.objects.add_queue("xaz")

        Node.objects.remove_queue("foo")
        Node.objects.remove_queue("xaz")

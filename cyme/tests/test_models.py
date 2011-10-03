from __future__ import absolute_import

from celery.tests.utils import unittest
from mock import Mock

from cyme.models import Instance, Queue


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


class test_Instance(unittest.TestCase):

    def setUp(self):
        self._mt, Instance.MultiTool = Instance.MultiTool, Mock()
        self._query, Instance._query = Instance._query, Mock()

    def tearDown(self):
        Instance.MultiTool = self._mt
        Instance._query = self._query
        Queue.objects.all().delete()
        Instance.objects.all().delete()

    def test__unicode__(self):
        self.assertTrue(unicode(Instance(name="foo")))

    def test_add(self):
        n1 = Instance.objects.add()
        self.assertTrue(n1.name)
        self.assertTrue(n1.default_args)
        self.assertTrue(n1.direct_queue)
        self.assertTrue(n1.multi)
        self.assertTrue(n1.argtuple)
        self.assertTrue(n1.argv)
        self.assertIsNone(n1.getpid())

        n2 = Instance.objects.add("foo")
        self.assertEqual(n2.name, "foo")

        n1.disable()
        self.assertFalse(Instance.objects.get(name=n1.name).is_enabled)
        n1.enable()
        self.assertTrue(Instance.objects.get(name=n1.name).is_enabled)

    def test_start_stop_restart(self):
        n = Instance.objects.add()
        n.start()
        n.stop()
        n.restart()

    def test_add_cancel_queue(self):
        n = Instance.objects.add()
        q = Queue.objects.create(name="xiziasd")
        q.save()

        n.add_queue(q)
        n._query.assert_called_with("add_consumer", dict(queue=q.name,
                                    exchange=q.name, routing_key=q.name,
                                    exchange_type=None))

        n.cancel_queue(q)
        n._query.assert_called_with("cancel_consumer", dict(queue=q.name))

    def test_objects(self):
        n = Instance.objects.add()
        Instance.objects.disable(n)
        Instance.objects.enable(n)
        Instance.objects.remove(n)

    def test_with_queues(self):
        Instance.objects.add(queues="foo,bar,baz")
        Instance.objects.add_queue("foo")
        Instance.objects.add_queue("xaz")

        Instance.objects.remove_queue("foo")
        Instance.objects.remove_queue("xaz")

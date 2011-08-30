import os
import sys

# funtest config
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))

from cyme.bin.base import app, Env
from cyme.utils import cached_property, Path, uuid
from eventlet.event import Event
from nose import SkipTest

from .utils import unittest

CYME_PORT = int(os.environ.get("CYME_PORT") or 8013)
CYME_URL = "http://127.0.0.1:%s" % (CYME_PORT, )
CYME_INSTANCE_DIR = Path("instances").absolute()

_branch = [None]


@app(needs_eventlet=True, instance_dir=CYME_INSTANCE_DIR)
def start_branch(env, argv=None):
    env.syncdb(interactive=False)
    from cyme.branch import Branch
    ready_event = Event()
    CYME_INSTANCE_DIR.mkdir()
    instance = Branch("127.0.0.1:%s" % (CYME_PORT, ), numc=1,
                      ready_event=ready_event)
    instance.start()
    ready_event.wait()
    return instance


def destroy_branch(branch):
    branch.stop()


def teardown():
    if _branch[0] is not None:
        destroy_branch(_branch[0])
    if CYME_INSTANCE_DIR.isdir():
        CYME_INSTANCE_DIR.rmtree()


class ClientTestCase(unittest.TestCase):

    @cached_property
    def Client(self):
        from cyme import Client
        return Client


class BranchTestCase(unittest.TestCase):

    def setUp(self):
        if _branch[0] is None:
            _branch[0] = start_branch()


class test_create_app(BranchTestCase, ClientTestCase):

    def test_create(self):
        client = self.Client(CYME_URL)
        app = client.add(uuid())
        self.assertTrue(repr(app))
        self.assertTrue(app)
        self.assertTrue(app.info)
        self.assertTrue(app.info.broker)
        self.assertIn(app.app, app.all())
        app = client.get(app.app)
        self.assertTrue(app)
        app.delete()
        self.assertNotIn(app.app, app.all())


class test_basic(BranchTestCase, ClientTestCase):

    def setUp(self):
        BranchTestCase.setUp(self)
        self.app = self.Client(CYME_URL).add(uuid())

    def tearDown(self):
        self.app.delete()
        BranchTestCase.tearDown(self)

    def test_basic(self):
        app = self.app
        instance = app.instances.add()
        self.assertTrue(repr(instance))
        self.assertTrue(instance)
        instance = app.instances.get(instance.name)
        self.assertTrue(instance)
        self.assertIn(instance, app.instances)

        q = uuid()
        expected = dict(exchange=q, exchange_type="topic", routing_key=q)
        queue = app.queues.add(q, **expected)
        self.assertTrue(repr(queue))
        queue = app.queues.get(queue.name)
        self.assertTrue(queue)
        for key, value in expected.items():
            self.assertEqual(getattr(queue, key), value)
        self.assertIn(queue, app.queues)

        self.assertTrue(instance.consumers.add(queue))
        self.assertTrue(instance.consumers.delete(queue))

        queue.delete()
        self.assertNotIn(queue, app.queues)

        instance.delete()
        self.assertNotIn(instance, app.instances)


if __name__ == "__main__":
    unittest.main()

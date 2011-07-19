from celery.tests.utils import unittest
from mock import Mock

from scs.agent import Agent
from scs.thread import gThread


class test_Agent(unittest.TestCase):

    def setUp(self):
        self._gstart, gThread.start = gThread.start, Mock()

    def tearDown(self):
        gThread.start = self._gstart

    def test_run(self):
        agent = Agent()
        self.assertTrue(agent.id)
        self.assertTrue(agent.components)
        agent2 = Agent(without_httpd=True, without_amqp=True)
        self.assertNotEqual(len(agent2.components), len(agent.components))
        agent.run()
        for component in agent.components:
            component.start.assert_called_with()

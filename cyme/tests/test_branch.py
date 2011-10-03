from __future__ import absolute_import

from celery.tests.utils import unittest
from mock import Mock

from cyme.branch import Branch
from cyme.thread import gThread


class test_Branch(unittest.TestCase):

    def setUp(self):
        self._gstart, gThread.start = gThread.start, Mock()

    def tearDown(self):
        gThread.start = self._gstart

    def test_run(self):
        branch = Branch()
        self.assertTrue(branch.id)
        self.assertTrue(branch.components)
        branch2 = Branch(without_httpd=True, without_amqp=True)
        self.assertNotEqual(len(branch2.components), len(branch.components))
        branch.run()
        for component in branch.components:
            component.start.assert_called_with()

"""Unit tests for the executor.
"""

import mysql.hub.executor as _executor
import unittest
from tests.utils import DummyManager

count = []
other = None

def test1():
    global count
    for cnt in range(10, 1, -1):
        count.append(cnt)

def test2():
    global other
    other = 47

class TestExecutor(unittest.TestCase):
    """Test executor.
    """

    def setUp(self):
        manager = DummyManager()
        self.executor = _executor.Executor(manager)

    def test_basic(self):
        global other
        global count
        self.executor.start()
        self.executor.enqueue(test1)
        self.executor.enqueue(test2)
        self.executor.enqueue(None)
        self.executor.join()
        for cnt in range(10, 1, -1):
            self.assertTrue(cnt in count)
        self.assertEqual(other, 47)

if __name__ == '__main__':
    unittest.main()


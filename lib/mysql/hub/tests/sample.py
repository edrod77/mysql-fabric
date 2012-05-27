"""Example test case.

Test case added to test the test runner. Remove this test when real
tests are added.
"""

import unittest
import random

class SampleTest(unittest.TestCase):
    """Example test, to check the test runner.
    """
    def setUp(self):
        self.seq = range(10)

    def test_sample(self):
        with self.assertRaises(ValueError):
            random.sample(self.seq, 20)
        for element in random.sample(self.seq, 5):
            self.assertTrue(element in self.seq)

    def test_failure(self):
        self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()


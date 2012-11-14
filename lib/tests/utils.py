"""Module holding support utilities for tests.
"""
class SkipTests(type):
    """Metaclass which is used to skip test cases as follows::

      import unittest
      import tests.utils as _utils

      class TestCaseClass(unittest.TestCase):
        __metaclass__ = _utils.SkipTests
    """
    def __new__(cls, name, bases, dct):
        """Create a new instance for SkipTests.
        """
        for name, item in dct.items():
            if callable(item) and name.startswith("test"):
                dct[name] = None
        return type.__new__(cls, name, bases, dct)

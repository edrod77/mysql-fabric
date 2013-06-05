"""Unit tests for administrative on servers.
"""
import unittest

import mysql.fabric.persistence as _persistence

import tests.utils

class TestLoggingServices(unittest.TestCase):
    "Test replication service interface."

    def setUp(self):
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()

    def tearDown(self):
        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_set_logging(self):
        self.assertFalse(
            self.proxy.manage.logging_level("unknown", "DEBUG")
        )
        self.assertTrue(
            self.proxy.manage.logging_level("mysql.fabric", "DEBUG")
        )

if __name__ == "__main__":
    unittest.main()

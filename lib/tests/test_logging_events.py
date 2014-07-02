#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#

"""Unit tests for administrative on servers.
"""
import unittest
import tests.utils

class TestLoggingServices(tests.utils.TestCase):
    "Test replication service interface."

    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_set_logging(self):
        """Test remotely setting logging configuration per file.
        """
        packet = self.proxy.manage.logging_level("unknown", "DEBUG")
        self.check_xmlrpc_simple(packet, {}, has_error=True)
        packet = self.proxy.manage.logging_level("mysql.fabric", "DEBUG")
        self.check_xmlrpc_simple(packet, {}, has_error=False)

if __name__ == "__main__":
    unittest.main()

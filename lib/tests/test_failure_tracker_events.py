#
# Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
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
"""Unit tests for testing failure events.
"""
import unittest
import uuid as _uuid
import tests.utils
import sys

import mysql.fabric.persistence as _persistence

from mysql.fabric.server import (
    MySQLServer,
)

from mysql.fabric.utils import (
    get_time,
    get_time_delta,
)

from mysql.fabric import (
    executor as _executor,
    server as _server,
)

from mysql.fabric.services import (
    failure_tracker as _failure_tracker,
)

OPTIONS = {
    "uuid" : None,
    "address"  : tests.utils.MySQLInstances().get_address(0),
    "user" : "root"
}

class TestFailureEvents(tests.utils.TestCase):
    """Unit test for testing FailureEvents.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

        from __main__ import config
        _failure_tracker.ReportError._NOTIFICATION_INTERVAL = \
            int(config.get("failure_tracking", "notification_interval"))
        _failure_tracker.ReportError._NOTIFICATIONS = \
            int(config.get("failure_tracking", "notifications"))
        _failure_tracker.ReportError._NOTIFICATION_CLIENTS = \
            int(config.get("failure_tracking", "notification_clients"))

    def test_configuration(self):
        """Test configuration options that are stored in ReportError.
        """
        from __main__ import config
        self.assertTrue(
            _failure_tracker.ReportError._NOTIFICATIONS,
            config.get("failure_tracking", "notifications")
        )
        self.assertTrue(
            _failure_tracker.ReportError._NOTIFICATION_CLIENTS,
            config.get("failure_tracking", "notification_clients")
        )
        self.assertTrue(
            _failure_tracker.ReportError._NOTIFICATION_INTERVAL,
            config.get("failure_tracking", "notification_interval")
        )

    def test_report_error(self):
        """Test the mechanism used to report server's issues (i.e. errors).
        """
        _failure_tracker.ReportError._NOTIFICATIONS = 1
        _failure_tracker.ReportError._NOTIFICATION_CLIENTS = 1

        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        status = self.proxy.group.add("group", address_1)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.add("group", address_2)
        self.check_xmlrpc_command_result(status)
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        info = self.check_xmlrpc_simple(status_uuid, {})
        uuid_1 = info['uuid']
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        info = self.check_xmlrpc_simple(status_uuid, {})
        uuid_2 = info['uuid']
        error_uuid = 'deadbeef-a007-feed-f00d-cab3fe13249e'

        # Try to report instability of a server does not exist.
        status = self.proxy.threat.report_error(error_uuid)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to report instability of a server that is already faulty.
        server = _server.MySQLServer.fetch(uuid_1)
        group = _server.Group.fetch("group")
        self.proxy.group.promote(group.group_id, str(server.uuid))
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.threat.report_error(uuid_1)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Report instability of a server that is primary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_error(uuid_1)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.FAULTY,
        }, index=0)
        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.PRIMARY,
        }, index=1)

        # Report instability of a server that is spare.
        # Note this is using HOST:PORT instead of UUID.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.SPARE
        status = self.proxy.threat.report_error(address_1)
        self.check_xmlrpc_command_result(status)

    def test_report_error_update_only(self):
        """Test the mechanism used to report server's issues (i.e. errors).
        """
        _failure_tracker.ReportError._NOTIFICATIONS = 1
        _failure_tracker.ReportError._NOTIFICATION_CLIENTS = 1

        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        self.proxy.group.add("group", address_1)
        self.proxy.group.add("group", address_2)
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        info = self.check_xmlrpc_simple(status_uuid, {})
        uuid_1 = info['uuid']
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        info = self.check_xmlrpc_simple(status_uuid, {})
        uuid_2 = info['uuid']
        error_uuid = 'deadbeef-a007-feed-f00d-cab3fe13249e'

        # Report instability of a server that is primary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_error(
            uuid_1, "unknown", "unknown", True
        )
        self.check_xmlrpc_command_result(status)

        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.FAULTY,
        }, rowcount=2, index=0)
        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.SECONDARY,
        }, rowcount=2, index=1)

    def test_report_failure(self):
        """Test the mechanism used to report server's issues (i.e. failures).
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        self.proxy.group.add("group", address_1)
        self.proxy.group.add("group", address_2)
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        info = self.check_xmlrpc_simple(status_uuid, {})
        error_uuid = 'deadbeef-a007-feed-f00d-cab3fe13249e'
        uuid_1 = info['uuid']

        # Try to report failure of a server does not exist.
        status = self.proxy.threat.report_failure(error_uuid)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to report failure of a server that is already faulty.
        server = _server.MySQLServer.fetch(uuid_1)
        group = _server.Group.fetch("group")
        self.proxy.group.promote(group.group_id, str(server.uuid))
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.threat.report_failure(uuid_1)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Report failure of a server that is primary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_failure(uuid_1)
        self.check_xmlrpc_command_result(status)

        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.FAULTY)

        # Report failure of a server that is a spare.
        # Note this is using HOST:PORT instead of UUID.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.SPARE
        status = self.proxy.threat.report_failure(address_1)
        self.check_xmlrpc_command_result(status)

    def test_report_failure_update_only(self):
        """Test the mechanism used to report server's issues (i.e. failures).
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        self.proxy.group.add("group", address_1)
        self.proxy.group.add("group", address_2)
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        info = self.check_xmlrpc_simple(status_uuid, {})
        uuid_1 = info['uuid']
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        info = self.check_xmlrpc_simple(status_uuid, {})
        uuid_2 = info['uuid']
        error_uuid = 'deadbeef-a007-feed-f00d-cab3fe13249e'

        # Report failure of a server that is primary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_failure(
            uuid_1, "unknown", "unknown", True
        )
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.health("group")

        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.FAULTY,
        }, rowcount=2, index=0)
        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.SECONDARY,
        }, rowcount=2, index=1)

if __name__ == "__main__":
    unittest.main()

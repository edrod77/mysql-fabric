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

class TestFailureEvents(unittest.TestCase):
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
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

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
        self.proxy.group.add("group", address_1)
        self.proxy.group.add("group", address_2)
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        uuid_1 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        uuid_2 = status_uuid[2]
        error_uuid = status_uuid[1]

        # Try to report instability of a server does not exist.
        status = self.proxy.threat.report_error(error_uuid)
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_report_error).")

        # Try to report instability of a server that is already faulty.
        server = _server.MySQLServer.fetch(uuid_1)
        group = _server.Group.fetch("group")
        self.proxy.group.promote(group.group_id, str(server.uuid))
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.threat.report_error(uuid_1)
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_report_error).")

        # Report instability of a server that is primary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_error(uuid_1)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")
        status = self.proxy.group.health("group")
        self.assertEqual(
            status[2][uuid_1]["status"], _server.MySQLServer.FAULTY
        )
        self.assertEqual(
            status[2][uuid_2]["status"], _server.MySQLServer.PRIMARY
        )

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
        uuid_1 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        uuid_2 = status_uuid[2]
        error_uuid = status_uuid[1]

        # Report instability of a server that is primary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_error(
            uuid_1, "unknown", "unknown", True
        )
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_report_error).")
        status = self.proxy.group.health("group")
        self.assertEqual(
            status[2][uuid_1]["status"], _server.MySQLServer.FAULTY
        )
        self.assertEqual(
            status[2][uuid_2]["status"], _server.MySQLServer.SECONDARY
        )

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
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")
        error_uuid = status_uuid[1]
        uuid_1 = status_uuid[2]

        # Try to report failure of a server does not exist.
        status = self.proxy.threat.report_failure(error_uuid)
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_report_failure).")

        # Try to report failure of a server that is already faulty.
        server = _server.MySQLServer.fetch(uuid_1)
        group = _server.Group.fetch("group")
        self.proxy.group.promote(group.group_id, str(server.uuid))
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.threat.report_failure(uuid_1)
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_report_failure).")

        # Report failure of a server that is secondary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_failure(uuid_1)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.FAULTY)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

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
        uuid_1 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        uuid_2 = status_uuid[2]
        error_uuid = status_uuid[1]

        # Report failure of a server that is primary.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.PRIMARY
        status = self.proxy.threat.report_failure(
            uuid_1, "unknown", "unknown", True
        )
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_report_failure).")
        status = self.proxy.group.health("group")
        self.assertEqual(
            status[2][uuid_1]["status"], _server.MySQLServer.FAULTY
        )
        self.assertEqual(
            status[2][uuid_2]["status"], _server.MySQLServer.SECONDARY
        )

if __name__ == "__main__":
    unittest.main()

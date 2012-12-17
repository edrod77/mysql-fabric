"""Unit tests for administrative on servers.
"""

import logging
import os
import threading
import time
import types
import unittest
import uuid as _uuid
import xmlrpclib
import sys

from mysql.hub import (
    config as _config,
    executor as _executor,
    events as _events,
    server as _server,
    persistence as _persistence,
    )

import tests.utils

class TestServerServices(unittest.TestCase):
    "Test server service interface"

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()

    def tearDown(self):
        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_create_group_events(self):
        # Look up groups.
        status = self.proxy.server.lookup_groups()
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(status[2], [])

        # Insert a new group.
        status = self.proxy.server.create_group("group", "Testing group...")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")

        # Try to insert a group twice.
        status = self.proxy.server.create_group("group", "Testing group...")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_group).")

        # Look up groups.
        status = self.proxy.server.lookup_groups()
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(status[2], [["group"]])

        # Look up a group.
        status = self.proxy.server.lookup_group("group")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_group).")
        self.assertEqual(status[2], {"group_id": "group", "description":
                                     "Testing group..."})

        # Try to look up a group that does not exist.
        status = self.proxy.server.lookup_group("group_1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_group).")
        self.assertEqual(status[2], False)

        # Update a group.
        status = self.proxy.server.update_group("group", "Test Test Test")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_update_group).")

        # Try to update group that does not exist.
        status = self.proxy.server.update_group("group_1", "Test Test Test")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_update_group).")

    def test_create_server_events(self):
        # Insert a new server.
        uri = tests.utils.MySQLInstances().get_uri(0)
        self.proxy.server.create_group("group_1", "Testing group...")
        status = self.proxy.server.create_server("group_1", uri, "root", "")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_server).")

        # Try to insert a server twice.
        status = self.proxy.server.create_server("group_1", uri, "root", "")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_server).")

        # Try to insert a server into a non-existing group.
        status = self.proxy.server.create_server("group_2", uri, "root", "")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_server).")

        # Look up servers.
        status_servers = self.proxy.server.lookup_servers("group_1")
        self.assertEqual(status_servers[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        status_uuid = self.proxy.server.lookup_uuid(uri, "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")
        self.assertEqual(status_servers[2], [[status_uuid[2], uri, False]])

        # Try to look up servers in a group that does not exist.
        status = self.proxy.server.lookup_servers("group_x")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_servers).")
        self.assertEqual(status[2], False)

        # Look up a server.
        status = self.proxy.server.lookup_server("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_server).")
        self.assertEqual(status[2], {"passwd": "", "uri": uri,
                                     "user": "root", "uuid": status_uuid[2]})

        # Try to look up a server in a group that does not exist.
        status = self.proxy.server.lookup_server("group_x", status_uuid[2])
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_server).")
        self.assertEqual(status[2], False)

        # Try to look up a server that does not exist.
        status = self.proxy.server.lookup_server("group_1",
            "cc75b12c-98d1-414c-96af-9e9d4b179678")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_server).")
        self.assertEqual(status[2], False)

        # Try to look up a server that does not exist
        status = self.proxy.server.lookup_uuid("unknown:15000", "root", "")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_uuid).")
        self.assertEqual(status[2], False)

    def test_remove_group_events(self):
        # Prepare group and servers
        uri = tests.utils.MySQLInstances().get_uri(0)
        self.proxy.server.create_group("group", "Testing group...")
        self.proxy.server.create_group("group_1", "Testing group...")
        self.proxy.server.create_server("group_1", uri, "root", "")

        # Remove a group.
        status = self.proxy.server.remove_group("group")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_group).")

        # Try to remove a group twice.
        status = self.proxy.server.remove_group("group")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_group).")

        # Try to remove a group where there are servers.
        status = self.proxy.server.remove_group("group_1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_group).")

        # Remove a group where there are servers.
        status = self.proxy.server.remove_group("group_1", True)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_group).")

    def test_remove_server_events(self):
        # Prepare group and servers
        uri = tests.utils.MySQLInstances().get_uri(0)
        self.proxy.server.create_group("group", "Testing group...")
        self.proxy.server.create_group("group_1", "Testing group...")
        self.proxy.server.create_server("group_1", uri, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(uri, "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")

        # Try to remove a server from a non-existing group.
        status = self.proxy.server.remove_server("group_2",
            "bb75b12b-98d1-414c-96af-9e9d4b179678")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server with an invalid uuid.
        status = self.proxy.server.remove_server("group_1",
            "bb-98d1-414c-96af-9")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server that does not exist.
        status = self.proxy.server.remove_server("group_1",
            "bb75b12c-98d1-414c-96af-9e9d4b179678")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server that is master within the group.
        group = _server.Group.fetch("group_1")
        group.master = _uuid.UUID(status_uuid[2])
        status = self.proxy.server.remove_server("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Remove a server.
        group = _server.Group.fetch("group_1")
        group.master = None
        status = self.proxy.server.remove_server("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_server).")

    def test_lookup_fabrics(self):
        from __main__ import xmlrpc_next_port
        status = self.proxy.server.lookup_fabrics()
        self.assertEqual(status, ["localhost:%d" % (xmlrpc_next_port, )])

if __name__ == "__main__":
    unittest.main()

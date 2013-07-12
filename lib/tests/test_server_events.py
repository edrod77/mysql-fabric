"""Unit tests for administrative on servers.
"""
import unittest
import uuid as _uuid

from mysql.fabric import (
    executor as _executor,
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

    def tearDown(self):
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_create_group_events(self):
        # Look up groups.
        status = self.proxy.group.lookup_groups()
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(status[2], [])

        # Insert a new group.
        status = self.proxy.group.create("group", "Testing group...")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")

        # Try to insert a group twice.
        status = self.proxy.group.create("group", "Testing group...")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_group).")

        # Look up groups.
        status = self.proxy.group.lookup_groups()
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(status[2], [["group"]])

        # Look up a group.
        status = self.proxy.group.lookup_groups("group")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(status[2], {"group_id": "group", "description":
                                     "Testing group..."})

        # Try to look up a group that does not exist.
        status = self.proxy.group.lookup_groups("group_1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_groups).")

        # Update a group.
        status = self.proxy.group.description("group", "Test Test Test")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_update_group_description).")

        # Try to update group that does not exist.
        status = self.proxy.group.description("group_1", "Test Test Test")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(
            status[1][-1]["description"],
           "Tried to execute action (_update_group_description)."
           )

    def test_add_server_events(self):
        # Insert a new server.
        address = tests.utils.MySQLInstances().get_address(0)
        self.proxy.group.create("group_1", "Testing group...")
        status = self.proxy.group.add("group_1", address, "root", "")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        # Try to insert a server twice.
        status = self.proxy.group.add("group_1", address, "root", "")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_server).")

        # Try to insert a server into a non-existing group.
        status = self.proxy.group.add("group_2", address, "root", "")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_server).")

        # Look up servers.
        status_servers = self.proxy.group.lookup_servers("group_1")
        self.assertEqual(status_servers[1][-1]["success"],
                         _executor.Job.SUCCESS)
        self.assertEqual(status_servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        status_uuid = self.proxy.server.lookup_uuid(address, "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")
        self.assertEqual(
            status_servers[2],
            [[status_uuid[2], address, False, _server.MySQLServer.RUNNING]]
            )

        # Try to look up servers in a group that does not exist.
        status = self.proxy.group.lookup_servers("group_x")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_servers).")

        # Look up a server.
        status = self.proxy.group.lookup_servers("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        self.assertEqual(status[2], {"passwd": "", "address": address,
                                     "user": "root", "uuid": status_uuid[2]})

        # Try to look up a server in a group that does not exist.
        status = self.proxy.group.lookup_servers("group_x", status_uuid[2])
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_servers).")

        # Try to look up a server that does not exist.
        status = self.proxy.group.lookup_servers("group_1",
            "cc75b12c-98d1-414c-96af-9e9d4b179678")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_servers).")

        # Try to look up a server that does not exist
        status = self.proxy.server.lookup_uuid("unknown:15000", "root", "")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_uuid).")

        # Add a server with a connection that does not have
        # root privileges.
        address = tests.utils.MySQLInstances().get_address(1)
        status_uuid = self.proxy.server.lookup_uuid(address, "root", "")
        server = _server.MySQLServer(
            _uuid.UUID(status_uuid[2]), address, "root", ""
            )
        server.connect()
        server.exec_stmt(
            "CREATE USER 'jeffrey'@'localhost' IDENTIFIED BY 'mypass'"
            )
        server.exec_stmt(
            "GRANT ALL ON mysql.* TO 'jeffrey'@'localhost'"
            )
        status = self.proxy.group.add("group_1", address, "jeffrey", "mypass")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        _server.ConnectionPool().purge_connections(_uuid.UUID(status_uuid[2]))

        # Drop temporary user.
        server.exec_stmt("DROP USER 'jeffrey'@'localhost'")

    def test_destroy_group_events(self):
        # Prepare group and servers
        address = tests.utils.MySQLInstances().get_address(0)
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.create("group_1", "Testing group...")
        self.proxy.group.add("group_1", address, "root", "")

        # Remove a group.
        status = self.proxy.group.destroy("group")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_destroy_group).")

        # Try to remove a group twice.
        status = self.proxy.group.destroy("group")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_destroy_group).")

        # Try to remove a group where there are servers.
        status = self.proxy.group.destroy("group_1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_destroy_group).")

        # Remove a group where there are servers.
        status = self.proxy.group.destroy("group_1", True)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_destroy_group).")

    def test_remove_server_events(self):
        # Prepare group and servers
        address = tests.utils.MySQLInstances().get_address(0)
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.create("group_1", "Testing group...")
        self.proxy.group.add("group_1", address, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(address, "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")

        # Try to remove a server from a non-existing group.
        status = self.proxy.group.remove(
            "group_2", "bb75b12b-98d1-414c-96af-9e9d4b179678"
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server with an invalid uuid.
        status = self.proxy.group.remove(
            "group_1", "bb-98d1-414c-96af-9"
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server that does not exist.
        status = self.proxy.group.remove(
            "group_1",
            "bb75b12c-98d1-414c-96af-9e9d4b179678"
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server that is master within the group.
        group = _server.Group.fetch("group_1")
        group.master = _uuid.UUID(status_uuid[2])
        status = self.proxy.group.remove("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Remove a server.
        group = _server.Group.fetch("group_1")
        group.master = None
        status = self.proxy.group.remove("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_server).")

    def test_group_status(self):
        # Prepare group and servers
        address = tests.utils.MySQLInstances().get_address(0)
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.add("group", address, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(address, "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")

        # Try to activate a non-existing group.
        status = self.proxy.group.activate("group-1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_activate_group).")

        # Activate group.
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.INACTIVE)
        status = self.proxy.group.activate("group")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_activate_group).")
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.ACTIVE)

        # Deactivate group.
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.ACTIVE)
        status = self.proxy.group.deactivate("group")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_deactivate_group).")
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.INACTIVE)

        # Try to deactivate a non-existing group.
        status = self.proxy.group.deactivate("group-1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_deactivate_group).")

    def test_server_status(self):
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        self.proxy.group.add("group", address_1, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(address_1, "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")
        uuid_1 = status_uuid[-1]
        self.proxy.group.add("group", address_2, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(address_2, "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")
        uuid_2 = status_uuid[-1]
        error_uuid = status_uuid[0]

        # Try to set a spare server when the server does not exist.
        status = self.proxy.server.set_status(error_uuid, "SPARE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set a spare server when the server is a master.
        group = _server.Group.fetch("group")
        group.master = _uuid.UUID(uuid_1)
        status = self.proxy.server.set_status(uuid_1, "SPARE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")
        group.master = None

        # Try to set a spare server when the server is offline.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.OFFLINE
        status = self.proxy.server.set_status(uuid_1, "SPARE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")
        server.status = _server.MySQLServer.RUNNING

        # Set a spare server.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.RUNNING)
        status = self.proxy.server.set_status(uuid_1, "SPARE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_status).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)

        # Try to set an offline server that does not exist.
        status = self.proxy.server.set_status(error_uuid, "OFFLINE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set an offline server when the server is a master.
        group = _server.Group.fetch("group")
        group.master = _uuid.UUID(uuid_1)
        status = self.proxy.server.set_status(uuid_1, "OFFLINE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")
        group.master = None

        # Set an offline server.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)
        status = self.proxy.server.set_status(uuid_1, "OFFLINE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_status).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.OFFLINE)

        # Try to set a running server that does not exist.
        status = self.proxy.server.set_status(error_uuid, "RUNNING")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Set a running server.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.OFFLINE)
        status = self.proxy.server.set_status(uuid_1, "RUNNING")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_status).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.RUNNING)

        # Set a faulty server (slave).
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.RUNNING)
        status = self.proxy.server.set_status(uuid_1, "FAULTY")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.FAULTY)

        # Set a faulty server twice (slave).
        status = self.proxy.server.set_status(uuid_1, "FAULTY")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set a faulty server while the group is activate.
        group.status = _server.Group.ACTIVE
        self.proxy.server.set_status(uuid_1, "RUNNING")
        status = self.proxy.server.set_status(uuid_1, "FAULTY")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.RUNNING)

        # Try to set a faulty server (master).
        group.status = _server.Group.INACTIVE
        group = _server.Group.fetch("group")
        self.assertEqual(group.master, None)
        self.proxy.group.promote(group.group_id, str(server.uuid))
        group = _server.Group.fetch("group")
        self.assertEqual(group.master, server.uuid)
        status = self.proxy.server.set_status(uuid_1, "FAULTY")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        group = _server.Group.fetch("group")
        self.assertNotEqual(group.master, server.uuid)
        self.assertNotEqual(group.master, None)

    def test_lookup_servers(self):
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_0 = tests.utils.MySQLInstances().get_address(0)
        address_1 = tests.utils.MySQLInstances().get_address(1)
        address_2 = tests.utils.MySQLInstances().get_address(2)
        self.proxy.group.add("group", address_0, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(address_0, "root", "")
        uuid_0 = status_uuid[-1]
        server_0 = _server.MySQLServer.fetch(uuid_0)
        self.proxy.group.add("group", address_1, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(address_1, "root", "")
        uuid_1 = status_uuid[-1]
        server_1 = _server.MySQLServer.fetch(uuid_1)
        self.proxy.group.add("group", address_2, "root", "")
        status_uuid = self.proxy.server.lookup_uuid(address_2, "root", "")
        uuid_2 = status_uuid[-1]
        server_2 = _server.MySQLServer.fetch(uuid_2)

        # Fetch all servers in a group.
        server =  self.proxy.group.lookup_servers("group")
        self.assertEqual(len(server[-1]), 3)

        # TODO: HOW TO FIX THIS (HAM-132)?
        # The return is temporarily placed here while
        # HAM-132 is not fixed.
        return
        # Fetch all running servers in a group.
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.RUNNING
            )
        self.assertEqual(len(server[-1]), 3)

        # Fetch all offline servers in a group.
        server_1.status = _server.MySQLServer.OFFLINE
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.OFFLINE
            )
        self.assertEqual(len(server[-1]), 1)

        # Fetch all running servers in a group.
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.RUNNING
            )
        self.assertEqual(len(server[-1]), 2)

        # Fetch all servers in a group.
        server =  self.proxy.group.lookup_servers("group")
        self.assertEqual(len(server[-1]), 3)

        # Try to fetch servers with a non-existing status.
        server =  self.proxy.group.lookup_servers(
            "group", 10
            )
        self.assertEqual(server[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(server[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(server[1][-1]["description"],
                         "Tried to execute action (_lookup_servers).")

    def test_lookup_fabrics(self):
        from __main__ import xmlrpc_next_port
        status = self.proxy.manage.lookup_fabrics()
        self.assertEqual(status, ["localhost:%d" % (xmlrpc_next_port, )])

if __name__ == "__main__":
    unittest.main()

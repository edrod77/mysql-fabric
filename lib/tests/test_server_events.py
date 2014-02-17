#
# Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
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
import uuid as _uuid
import tests.utils

from mysql.fabric import (
    executor as _executor,
    server as _server,
)

class TestServerServices(unittest.TestCase):
    """Test server service interface.
    """
    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_create_group_events(self):
        """Test creating a group by calling group.create().
        """
        # Look up groups.
        status = self.proxy.group.lookup_groups()
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
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
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        self.assertEqual(status[2], [["group"]])

        # Look up a group.
        status = self.proxy.group.lookup_groups("group")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        self.assertEqual(status[2], {"group_id": "group", "description":
                                     "Testing group..."})

        # Try to look up a group that does not exist.
        status = self.proxy.group.lookup_groups("group_1")
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2], True)

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
        """Test adding a server by calling group.add().
        """
        # Insert a new server.
        address = tests.utils.MySQLInstances().get_address(0)
        user = tests.utils.MySQLInstances().user
        root_user = tests.utils.MySQLInstances().root_user
        passwd = tests.utils.MySQLInstances().passwd
        root_passwd = tests.utils.MySQLInstances().root_passwd
        self.proxy.group.create("group_1", "Testing group...")
        status = self.proxy.group.add("group_1", address, user, passwd)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        # Try to insert a server twice.
        status = self.proxy.group.add("group_1", address, user, passwd)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_server).")

        # Try to insert a server into a non-existing group.
        status = self.proxy.group.add("group_2", address, user, passwd)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_server).")

        # Look up servers.
        status_servers = self.proxy.group.lookup_servers("group_1")
        self.assertEqual(status_servers[0], True)
        self.assertEqual(status_servers[1], "")
        obtained_server_list = status_servers[2]
        status_uuid = self.proxy.server.lookup_uuid(address, user, passwd)
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")
        self.assertEqual(
            status_servers[2],
            [[status_uuid[2], address, False,
            _server.MySQLServer.SECONDARY]]
            )

        # Try to look up servers in a group that does not exist.
        status = self.proxy.group.lookup_servers("group_x")
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2],  True)

        # Look up a server.
        status = self.proxy.group.lookup_servers("group_1", status_uuid[2])
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        self.assertEqual(status[2], {"passwd": passwd, "address": address,
                                     "user": user, "uuid": status_uuid[2]})

        # Try to look up a server in a group that does not exist.
        status = self.proxy.group.lookup_servers("group_x", status_uuid[2])
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2],  True)

        # Try to look up a server that does not exist.
        status = self.proxy.group.lookup_servers("group_1",
            "cc75b12c-98d1-414c-96af-9e9d4b179678")
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2],  True)

        # Try to look up a server that does not exist
        status = self.proxy.server.lookup_uuid("unknown:15000", user, passwd)
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2],  True)

        # Try to add a server with a connection that does not have
        # the appropriate privileges.
        address = tests.utils.MySQLInstances().get_address(1)
        status_uuid = self.proxy.server.lookup_uuid(address, user, passwd)
        server = _server.MySQLServer(
            _uuid.UUID(status_uuid[2]), address, root_user, root_passwd
        )
        _server.ConnectionPool().purge_connections(_uuid.UUID(status_uuid[2]))
        server.connect()
        server.set_session_binlog(False)
        server.exec_stmt(
            "CREATE USER 'jeffrey'@'%%' IDENTIFIED BY 'mypass'"
            )
        server.exec_stmt(
            "GRANT ALL ON mysql.* TO 'jeffrey'@'%%'"
            )
        status = self.proxy.group.add("group_1", address, "jeffrey", "mypass")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_server).")

        # Drop temporary user.
        server.exec_stmt("DROP USER 'jeffrey'@'%%'")
        server.set_session_binlog(True)
        server.disconnect()
        _server.ConnectionPool().purge_connections(_uuid.UUID(status_uuid[2]))

    def test_destroy_group_events(self):
        """Test destroying a group by calling group.destroy().
        """
        # Prepare group and servers
        address = tests.utils.MySQLInstances().get_address(0)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.create("group_1", "Testing group...")
        self.proxy.group.add("group_1", address, user, passwd)

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
        """Test removing a server by calling group.remove().
        """
        # Prepare group and servers
        address = tests.utils.MySQLInstances().get_address(0)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.create("group_1", "Testing group...")
        self.proxy.group.add("group_1", address, user, passwd)
        status_uuid = self.proxy.server.lookup_uuid(address, user, passwd)
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")

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
        tests.utils.configure_decoupled_master(group,
                                               _uuid.UUID(status_uuid[2]))
        status = self.proxy.group.remove("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Remove a server.
        group = _server.Group.fetch("group_1")
        tests.utils.configure_decoupled_master(group, None)
        status = self.proxy.group.remove("group_1", status_uuid[2])
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_server).")

    def test_group_status(self):
        """Test group's status by calling group.activate()/group.deactive().
        """
        # Prepare group and servers
        address = tests.utils.MySQLInstances().get_address(0)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.add("group", address, user, passwd)
        status_uuid = self.proxy.server.lookup_uuid(address, user, passwd)
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")

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
        """Test server's status by calling server.status().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.add("group", address_1, user, passwd)
        status_uuid = self.proxy.server.lookup_uuid(address_1, user, passwd)
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")
        uuid_1 = status_uuid[2]
        error_uuid = status_uuid[1]
        self.proxy.group.add("group", address_2, user, passwd)
        status_uuid = self.proxy.server.lookup_uuid(address_2, user, passwd)
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")

        # Try to set the status when the server's id is invalid.
        status = self.proxy.server.set_status("INVALID", "spare")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set the status when the server does not exist.
        status = self.proxy.server.set_status(error_uuid, "spare")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set the status when the server does not belong to a group.
        server = _server.MySQLServer.fetch(uuid_1)
        server.group_id = None
        status = self.proxy.server.set_status(uuid_1, "spare")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")
        server.group_id = "group"

        # Try to set a spare server when the server is a master.
        group = _server.Group.fetch("group")
        tests.utils.configure_decoupled_master(group, _uuid.UUID(uuid_1))
        status = self.proxy.server.set_status(uuid_1, "spare")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")
        tests.utils.configure_decoupled_master(group, None)

        # Set a spare server when the server is faulty.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.server.set_status(uuid_1, "spare")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_status).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)

        # Set a spare server when the server is secondary.
        server.status = _server.MySQLServer.SECONDARY
        status = self.proxy.server.set_status(uuid_1, "spare")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_status).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)

        # Try to set a status that does not exist.
        # Note though that this uses idx.
        status = self.proxy.server.set_status(uuid_1, 20)
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Set a spare server when the server is secondary.
        # Note though that this uses idx.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.server.set_status(uuid_1,
            _server.MySQLServer.get_status_idx("SPARE"))
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_status).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)

        # Try to set a secondary server that does not exist.
        status = self.proxy.server.set_status(error_uuid, "secondary")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set a secondary server when the server is a master.
        group = _server.Group.fetch("group")
        tests.utils.configure_decoupled_master(group, _uuid.UUID(uuid_1))
        status = self.proxy.server.set_status(uuid_1, "secondary")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")
        tests.utils.configure_decoupled_master(group, None)

        # Try to set a secondary server when the server is faulty.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.server.set_status(uuid_1, "secondary")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Set a faulty server (slave).
        server.status = _server.MySQLServer.SECONDARY
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SECONDARY)
        status = self.proxy.server.set_status(uuid_1, "faulty")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.FAULTY)

        # Set a faulty server twice (slave).
        status = self.proxy.server.set_status(uuid_1, "faulty")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set a faulty server while the group is activate.
        group.status = _server.Group.ACTIVE
        server.status = _server.MySQLServer.SECONDARY
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SECONDARY)
        status = self.proxy.server.set_status(uuid_1, "faulty")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SECONDARY)

        # Try to set a faulty server (master).
        group.status = _server.Group.INACTIVE
        group = _server.Group.fetch("group")
        self.proxy.group.promote(group.group_id, str(server.uuid))
        group = _server.Group.fetch("group")
        status = self.proxy.server.set_status(uuid_1, "faulty")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        group = _server.Group.fetch("group")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.FAULTY)

        # Try to set a primary server.
        status = self.proxy.server.set_status(uuid_1, "primary")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

        # Try to set an invalid status.
        status = self.proxy.server.set_status(uuid_1, "INVALID")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_status).")

    def test_server_weight(self):
        """Test server's weight by calling server.weight().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.add("group", address_1, user, passwd)
        status_uuid = self.proxy.server.lookup_uuid(address_1, user, passwd)
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")
        uuid_1 = status_uuid[2]
        error_uuid = status_uuid[1]

        # Try to set the weight when the server's id is invalid.
        status = self.proxy.server.set_weight("INVALID", "0.1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_weight).")

        # Try to set the weight when the server does not exist.
        status = self.proxy.server.set_weight(error_uuid, "0.1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_weight).")

        # Try to set the mode when the server does not belong to a group.
        server = _server.MySQLServer.fetch(uuid_1)
        server.group_id = None
        status = self.proxy.server.set_weight(uuid_1, "0.1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_weight).")
        server.group_id = "group"

        # Try to set the weight to zero.
        status = self.proxy.server.set_weight(uuid_1, "0.0")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_weight).")

        # Try to set the weight to a negative value.
        status = self.proxy.server.set_weight(uuid_1, "-1.0")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_weight).")

        # Try to set the weight to a string.
        status = self.proxy.server.set_weight(uuid_1, "error")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_weight).")

        # Set the weight to 0.1.
        status = self.proxy.server.set_weight(uuid_1, 0.1)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_weight).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.weight, 0.1)

    def test_server_mode(self):
        """Test server's mode by calling server.mode().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.add("group", address_1, user, passwd)
        status_uuid = self.proxy.server.lookup_uuid(address_1, user, passwd)
        self.assertEqual(status_uuid[0], True)
        self.assertEqual(status_uuid[1], "")
        uuid_1 = status_uuid[2]
        error_uuid = status_uuid[1]

        # Try to set the mode when the server's id is invalid.
        status = self.proxy.server.set_mode("INVALID", "read_write")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Try to set the mode when the server does not exist.
        status = self.proxy.server.set_mode(error_uuid, "read_write")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Try to set the mode when the server does not belong to a group.
        server = _server.MySQLServer.fetch(uuid_1)
        server.group_id = None
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")
        server.group_id = "group"

        # Try to set the READ_WRITE mode when the server is a secondary.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SECONDARY)
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Try to set the WRITE_ONLY mode when the server is a secondary.
        status = self.proxy.server.set_mode(uuid_1, "write_only")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Set the OFFLINE mode when the server is a secondary.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)
        status = self.proxy.server.set_mode(uuid_1, "offline")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.OFFLINE)

        # Set the READ_ONLY mode when the server is a secondary.
        status = self.proxy.server.set_mode(uuid_1, "read_only")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)

        # Try to set the READ_WRITE mode when the server is a spare.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.SPARE
        self.assertEqual(server.status, _server.MySQLServer.SPARE)
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Try to set the WRITE_ONLY mode when the server is a spare.
        status = self.proxy.server.set_mode(uuid_1, "write_only")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Set the OFFLINE mode when the server is a spare.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)
        status = self.proxy.server.set_mode(uuid_1, "offline")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.OFFLINE)

        # Set the READ_ONLY mode when the server is a spare.
        status = self.proxy.server.set_mode(uuid_1, "read_only")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)

        # Try to set the OFFLINE mode when the server is a primary.
        server = _server.MySQLServer.fetch(uuid_1)
        group = _server.Group.fetch(server.group_id)
        tests.utils.configure_decoupled_master(group, server)
        self.assertEqual(server.mode, _server.MySQLServer.READ_WRITE)
        self.assertEqual(server.status, _server.MySQLServer.PRIMARY)
        status = self.proxy.server.set_mode(uuid_1, "offline")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Try to set the READ_ONLY mode when the server is a primary.
        status = self.proxy.server.set_mode(uuid_1, "read_only")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")

        # Set the WRITE_ONLY mode when the server is a primary.
        status = self.proxy.server.set_mode(uuid_1, "write_only")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.WRITE_ONLY)

        # Set the READ_WRITE mode when the server is a primary.
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_WRITE)

        # Trye to set an invalid mode using idx.
        status = self.proxy.server.set_mode(uuid_1, 20)
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_WRITE)

        # Set the WRITE_ONLY mode when the server is a primary.
        # Note this uses idx
        status = self.proxy.server.set_mode(uuid_1,
            _server.MySQLServer.get_mode_idx("WRITE_ONLY"))
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_set_server_mode).")
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.WRITE_ONLY)

    def test_add_slave(self):
        """Test whether some servers are made slaves or not.
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_0 = tests.utils.MySQLInstances().get_address(0)
        address_1 = tests.utils.MySQLInstances().get_address(1)
        address_2 = tests.utils.MySQLInstances().get_address(2)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        status_uuid = self.proxy.server.lookup_uuid(address_0, user, passwd)
        uuid_0 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_1, user, passwd)
        uuid_1 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_2, user, passwd)
        uuid_2 = status_uuid[2]
        status = self.proxy.group.add("group", address_0, user, passwd)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        status = self.proxy.group.promote("group")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Add a servers and check that they are made slaves.
        self.proxy.group.add("group", address_1, user, passwd)
        self.proxy.group.add("group", address_2, user, passwd)
        status =  self.proxy.group.lookup_servers("group")
        retrieved = set(item for sublist in status[2] for item in sublist)
        expected = [
            [uuid_0, address_0, True, _server.MySQLServer.PRIMARY],
            [uuid_1, address_1, False, _server.MySQLServer.SECONDARY],
            [uuid_2, address_2, False, _server.MySQLServer.SECONDARY]
        ]
        expected = set(item for sublist in expected for item in sublist)
        self.assertEqual(retrieved, expected)

    def test_lookup_servers(self):
        """Test searching for servers by calling group.lookup_servers().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_0 = tests.utils.MySQLInstances().get_address(0)
        address_1 = tests.utils.MySQLInstances().get_address(1)
        address_2 = tests.utils.MySQLInstances().get_address(2)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.add("group", address_0, user, passwd)
        self.proxy.group.add("group", address_1, user, passwd)
        self.proxy.group.add("group", address_2, user, passwd)
        status_uuid = self.proxy.server.lookup_uuid(address_1, user, passwd)
        server_1 = _server.MySQLServer.fetch(status_uuid[2])

        # Fetch all servers in a group.
        server =  self.proxy.group.lookup_servers("group")
        self.assertEqual(len(server[2]), 3)

        #
        # It it not possible to specify only some of the optional
        # parameters. For that reason, this part of the test is
        # skipped as it requires to set the status without setting
        # the uuid parameter.
        # Note this is not possible though as the method has the
        #following signature:
        # lookup_servers(self, group_id, uuid=None, status=None, ...)
        #
        return
        # Fetch all running servers in a group.
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.SECONDARY
            )
        self.assertEqual(len(server[2]), 3)

        # Fetch all offline servers in a group.
        server_1.status = _server.MySQLServer.FAULTY
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.FAULTY
            )
        self.assertEqual(len(server[2]), 1)

        # Fetch all running servers in a group.
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.SECONDARY
            )
        self.assertEqual(len(server[2]), 2)

        # Fetch all servers in a group.
        server =  self.proxy.group.lookup_servers("group")
        self.assertEqual(len(server[2]), 3)

        # Try to fetch servers with a non-existing status.
        server =  self.proxy.group.lookup_servers(
            "group", 10
            )
        self.assertEqual(server[0], False)
        self.assertNotEqual(server[1], "")
        self.assertEqual(server[2],  True)

    def test_dump_fabric(self):
        """Test searching for fabric instances by calling
        dump.fabric_nodes().
        """
        from __main__ import xmlrpc_next_port
        status = self.proxy.dump.fabric_nodes()
        self.assertEqual(
            status, [0, 0, 0, ["localhost:%d" % (xmlrpc_next_port, )]]
        )

if __name__ == "__main__":
    unittest.main()

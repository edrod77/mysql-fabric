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
import uuid as _uuid
import tests.utils
import mysql.fabric.executor as _executor
import mysql.fabric.server as _server
import mysql.fabric.replication as _repl

from mysql.fabric.server import Group

class TestReplicationServices(tests.utils.TestCase):
    "Test replication service interface."

    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_promote_to(self):
        # Create topology: M1 ---> S2, M1 ---> S3
        instances = tests.utils.MySQLInstances()
        user = instances.user
        passwd = instances.passwd
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        # Try to use a group that does not exist.
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to use a slave that does not exist with the group.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to use a server that is already a master.
        self.proxy.group.add("group_id", master.address)
        self.proxy.group.add("group_id", slave_1.address)
        self.proxy.group.add("group_id", slave_2.address)
        group = _server.Group.fetch("group_id")
        tests.utils.configure_decoupled_master(group, slave_1)

        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to use a slave whose replication is not properly configured.
        tests.utils.configure_decoupled_master(group, master)
        _repl.stop_slave(slave_1, wait=True)
        _repl.reset_slave(slave_1, clean=True)
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to use a slave whose replication is not properly running.
        _repl.switch_master(slave_1, master, user, passwd)
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Start the slave.
        _repl.start_slave(slave_1, wait=True)

        # Look up servers.
        expected = tests.utils.make_servers_lookup_result([
            [str(master.uuid), master.address,_server.MySQLServer.PRIMARY,
             _server.MySQLServer.READ_WRITE, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_1.uuid), slave_1.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_2.uuid), slave_2.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
        ])
        servers = self.proxy.group.lookup_servers("group_id")
        self.check_xmlrpc_result(servers, expected)

        # Do the promote.
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.check_xmlrpc_command_result(status)

        # Look up servers.
        expected = tests.utils.make_servers_lookup_result([
            [str(master.uuid), master.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_1.uuid), slave_1.address, _server.MySQLServer.PRIMARY,
             _server.MySQLServer.READ_WRITE, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_2.uuid), slave_2.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
        ])
        servers = self.proxy.group.lookup_servers("group_id")
        self.check_xmlrpc_result(servers, expected)

        # Do the promote.
        # Note that it is using HOST:PORT instead of UUID.
        status = self.proxy.group.promote(
            "group_id", master.address
            )
        self.check_xmlrpc_command_result(status)

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        expected = tests.utils.make_servers_lookup_result([
            [str(master.uuid), master.address, _server.MySQLServer.PRIMARY,
             _server.MySQLServer.READ_WRITE, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_1.uuid), slave_1.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_2.uuid), slave_2.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
        ])
        self.check_xmlrpc_result(servers, expected)

    def test_promote(self):
        # Create topology: M1 ---> S2, M1 ---> S3, M1 ---> S4
        instances = tests.utils.MySQLInstances()
        user = instances.user
        passwd = instances.passwd
        instances.configure_instances({0 : [{1 : []}, {2 : []}, {3 : []}]},
                                      user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)
        slave_3 = instances.get_instance(3)

        # Try to use a group that does not exist.
        status = self.proxy.group.promote("group_id")
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to use a group without candidates.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.promote("group_id")
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to use a group with an invalid candidate (simulating that a
        # server went down).
        invalid_server = _server.MySQLServer(
            _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
            "unknown_host:32274"
            )
        _server.MySQLServer.add(invalid_server)
        group = _server.Group.fetch("group_id")
        group.add_server(invalid_server)
        status = self.proxy.group.promote("group_id")
        self.check_xmlrpc_command_result(status, has_error=True)
        group.remove_server(invalid_server)
        _server.MySQLServer.remove(invalid_server)

        # Configure master, an invalid candidate and make a slave point to
        # a different master.
        self.proxy.group.add("group_id", master.address)
        self.proxy.group.add("group_id", slave_1.address)
        self.proxy.group.add("group_id", slave_2.address)
        self.proxy.group.add("group_id", slave_3.address)
        tests.utils.configure_decoupled_master(group, master)
        invalid_server = _server.MySQLServer(
            _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
            "unknown_host:32274", user, passwd
            )
        _server.MySQLServer.add(invalid_server)
        group = _server.Group.fetch("group_id")
        group.add_server(invalid_server)
        _repl.stop_slave(slave_3, wait=True)
        _repl.switch_master(slave_3, slave_2, user, passwd)

        # Look up servers.
        expected = tests.utils.make_servers_lookup_result([
            [str(master.uuid), master.address, _server.MySQLServer.PRIMARY,
             _server.MySQLServer.READ_WRITE, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_1.uuid), slave_1.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_2.uuid), slave_2.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_3.uuid), slave_3.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(invalid_server.uuid), invalid_server.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
        ])
        servers = self.proxy.group.lookup_servers("group_id")
        self.check_xmlrpc_result(servers, expected)

        # Do the promote.
        status = self.proxy.group.promote("group_id")
        self.check_xmlrpc_command_result(status)

        # Look up servers.
        # servers = self.proxy.group.lookup_servers("group_id")
        # self.check_xmlrpc_result(servers, expected)

        # Do the promote without a current master.
        tests.utils.configure_decoupled_master(group, None)
        status = self.proxy.group.promote("group_id")
        self.check_xmlrpc_command_result(status)

    def test_promote_update_only(self):
        """Test promoting a master by calling group.promote.
        """
        # Create topology: M1 ---> S2, M1 ---> S3
        instances = tests.utils.MySQLInstances()
        user = instances.user
        passwd = instances.passwd
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)
        self.proxy.group.create("group_id")
        self.proxy.group.add("group_id", slave_1.address)
        self.proxy.group.add("group_id", slave_2.address)
        self.proxy.group.add("group_id", master.address)
        self.proxy.group.promote("group_id", str(master.uuid))

        # Try to promote a master, i.e. --update-only = True.
        status = self.proxy.group.promote("group_id", None, True)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Execute promote a master, i.e. --update-only = True.
        status = self.proxy.group.promote("group_id", str(slave_2.uuid), True)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.health("group_id")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
            "is_not_configured": True,
        }, index=0, rowcount=3)
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
        }, index=1, rowcount=3)
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.PRIMARY,
        }, index=2, rowcount=3)

    def test_demote(self):
        """Test demoting a master by calling group.demote.
        """
        # Create topology: M1 ---> S2, M1 ---> S3
        instances = tests.utils.MySQLInstances()
        user = instances.user
        passwd = instances.passwd
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        # Try to use a group that does not exist.
        status = self.proxy.group.demote("group_id")
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to demote when there is no master.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.demote("group_id")
        self.check_xmlrpc_command_result(status, has_error=True)

        # Configure masters and slaves.
        self.proxy.group.add("group_id", slave_1.address)
        self.proxy.group.add("group_id", slave_2.address)
        self.proxy.group.add("group_id", master.address)
        group = _server.Group.fetch("group_id")
        tests.utils.configure_decoupled_master(group, master)

        # Look up servers.
        expected = tests.utils.make_servers_lookup_result([
            [str(master.uuid), master.address, _server.MySQLServer.PRIMARY,
             _server.MySQLServer.READ_WRITE,_server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_1.uuid), slave_1.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_2.uuid), slave_2.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
        ])

        servers = self.proxy.group.lookup_servers("group_id")
        self.check_xmlrpc_result(servers, expected)
        
        self.assertTrue(_repl.is_slave_thread_running(slave_1))
        self.assertTrue(_repl.is_slave_thread_running(slave_2))

        # Demote master.
        status = self.proxy.group.demote("group_id")
        self.check_xmlrpc_command_result(status)

        # Look up servers.
        expected = tests.utils.make_servers_lookup_result([
            [str(master.uuid), master.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_1.uuid), slave_1.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
            [str(slave_2.uuid), slave_2.address, _server.MySQLServer.SECONDARY,
             _server.MySQLServer.READ_ONLY, _server.MySQLServer.DEFAULT_WEIGHT],
        ])
        servers = self.proxy.group.lookup_servers("group_id")
        self.check_xmlrpc_result(servers, expected)

        self.assertFalse(_repl.is_slave_thread_running(slave_1))
        self.assertFalse(_repl.is_slave_thread_running(slave_2))

    def test_demote_update_only(self):
        """Test demoting a master by calling group.demote.
        """
        # Create topology: M1 ---> S2, M1 ---> S3
        instances = tests.utils.MySQLInstances()
        user = instances.user
        passwd = instances.passwd
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)
        self.proxy.group.create("group_id")
        self.proxy.group.add("group_id", slave_1.address)
        self.proxy.group.add("group_id", slave_2.address)
        self.proxy.group.add("group_id", master.address)
        self.proxy.group.promote("group_id", str(master.uuid))

        # Demote a master, i.e. --update-only = True.
        status = self.proxy.group.demote("group_id", True)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.health("group_id")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
            'is_not_configured': True,
        }, index=0, rowcount=3)
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
        }, index=1, rowcount=3)
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
        }, index=2, rowcount=3)

if __name__ == "__main__":
    unittest.main()

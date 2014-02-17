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
import mysql.fabric.executor as _executor
import mysql.fabric.server as _server
import mysql.fabric.replication as _repl

from mysql.fabric.server import Group

class TestReplicationServices(unittest.TestCase):
    "Test replication service interface."

    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def test_import_topology(self):
        """Test importing topology by calling group.import_topology.
        """
        # Create topology M1 --> S2
        instances = tests.utils.MySQLInstances()
        user = instances.user
        passwd = instances.passwd
        instances.configure_instances({0 : [{1 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave = instances.get_instance(1)

        # Import topology.
        topology = self.proxy.group.import_topology(
            "group_id-0", "description...", master.address, user, passwd)

        self.assertStatus(topology, _executor.Job.SUCCESS)
        self.assertEqual(topology[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(topology[1][-1]["description"],
                         "Executed action (_import_topology).")
        expected_topology = {str(master.uuid): {"address": master.address,
                             "slaves": [{str(slave.uuid):
                             {"address": slave.address, "slaves": []}}]}}
        self.assertEqual(topology[2], expected_topology)

        # Look up a group.
        group = self.proxy.group.lookup_groups("group_id-1")
        self.assertEqual(group[0], True)
        self.assertEqual(group[1], "")
        self.assertEqual(group[2], {"group_id": "group_id-1", "description":
                                    "description..."})

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-1")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.PRIMARY],
            [str(slave.uuid), slave.address, False,
              _server.MySQLServer.SECONDARY]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

        # Create topology: M1 ---> S2, M1 ---> S3
        group_ = Group.fetch("group_id-1")
        tests.utils.configure_decoupled_master(group_, None)
        _server.MySQLServer.remove(master)
        master = None
        _server.MySQLServer.remove(slave)
        slave = None
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        # Import topology.
        topology = self.proxy.group.import_topology(
            "group_id-1", "description...", master.address, user, passwd)
        self.assertStatus(topology, _executor.Job.SUCCESS)
        self.assertEqual(topology[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(topology[1][-1]["description"],
                         "Executed action (_import_topology).")
        expected_topology = {
            str(master.uuid): {"address": master.address, "slaves": [
            {str(slave_1.uuid): {"address": slave_1.address, "slaves": []}},
            {str(slave_2.uuid): {"address": slave_2.address, "slaves": []}}]}}
        topology[2][str(master.uuid)]["slaves"].sort()
        expected_topology[str(master.uuid)]["slaves"].sort()
        self.assertEqual(topology[2], expected_topology)

        # Look up a group.
        group = self.proxy.group.lookup_groups("group_id-2")
        self.assertEqual(group[0], True)
        self.assertEqual(group[1], "")
        self.assertEqual(group[2], {"group_id": "group_id-2", "description":
                                    "description..."})

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-2")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.PRIMARY],
            [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.SECONDARY],
            [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.SECONDARY]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

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
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_define_ha_operation).")

        # Try to use a slave that does not exist with the group.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_fail).")

        # Try to use a server that is already a master.
        self.proxy.group.add("group_id", master.address, user, passwd)
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)
        group = _server.Group.fetch("group_id")
        tests.utils.configure_decoupled_master(group, slave_1)

        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Try to use a slave whose replication is not properly configured.
        tests.utils.configure_decoupled_master(group, master)
        _repl.stop_slave(slave_1, wait=True)
        _repl.reset_slave(slave_1, clean=True)
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Try to use a slave whose replication is not properly running.
        _repl.switch_master(slave_1, master, user, passwd)
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Start the slave.
        _repl.start_slave(slave_1, wait=True)

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
             _server.MySQLServer.PRIMARY],
            [str(slave_1.uuid), slave_1.address, False,
             _server.MySQLServer.SECONDARY],
            [str(slave_2.uuid), slave_2.address, False,
             _server.MySQLServer.SECONDARY]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

        # Do the promote.
        status = self.proxy.group.promote(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, False,
             _server.MySQLServer.SECONDARY],
            [str(slave_1.uuid), slave_1.address, True,
             _server.MySQLServer.PRIMARY],
            [str(slave_2.uuid), slave_2.address, False,
             _server.MySQLServer.SECONDARY]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

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
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_define_ha_operation).")

        # Try to use a group without candidates.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_find_candidate_fail).")

        # Try to use a group with an invalid candidate (simulating that a
        # server went down).
        invalid_server = _server.MySQLServer(
            _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
            "unknown_host:32274", user, passwd
            )
        _server.MySQLServer.add(invalid_server)
        group = _server.Group.fetch("group_id")
        group.add_server(invalid_server)
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_find_candidate_fail).")
        group.remove_server(invalid_server)
        _server.MySQLServer.remove(invalid_server)

        # Configure master, an invalid candidate and make a slave point to
        # a different master.
        self.proxy.group.add("group_id", master.address, user, passwd)
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)
        self.proxy.group.add("group_id", slave_3.address, user, passwd)
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
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]

        expected = \
            [[str(master.uuid), master.address, True,
             _server.MySQLServer.PRIMARY],
            [str(slave_1.uuid), slave_1.address, False,
             _server.MySQLServer.SECONDARY],
            [str(slave_2.uuid), slave_2.address, False,
             _server.MySQLServer.SECONDARY],
            [str(slave_3.uuid), slave_3.address, False,
             _server.MySQLServer.SECONDARY],
            [str(invalid_server.uuid), invalid_server.address, False,
             _server.MySQLServer.SECONDARY]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(expected, retrieved)

        # Do the promote.
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]
        retrieved.sort()
        self.assertNotEqual(expected, retrieved)

        # Do the promote without a current master.
        tests.utils.configure_decoupled_master(group, None)
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")


    def test_demote_master(self):
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
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_block_write_demote).")

        # Try to demote when there is no master.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.demote("group_id")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_block_write_demote).")

        # Configure masters and slaves.
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)
        self.proxy.group.add("group_id", master.address, user, passwd)
        group = _server.Group.fetch("group_id")
        tests.utils.configure_decoupled_master(group, master)

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.PRIMARY],
             [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.SECONDARY],
             [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.SECONDARY]
            ]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)
        self.assertTrue(_repl.is_slave_thread_running(slave_1))
        self.assertTrue(_repl.is_slave_thread_running(slave_2))

        # Demote master.
        status = self.proxy.group.demote("group_id")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_wait_slaves_demote).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[0], True)
        self.assertEqual(servers[1], "")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, False,
              _server.MySQLServer.SECONDARY],
             [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.SECONDARY],
             [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.SECONDARY]
            ]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)
        self.assertFalse(_repl.is_slave_thread_running(slave_1))
        self.assertFalse(_repl.is_slave_thread_running(slave_2))

if __name__ == "__main__":
    unittest.main()

"""Unit tests for administrative on servers.
"""
import unittest
import uuid as _uuid

import mysql.hub.executor as _executor
import mysql.hub.server as _server
import mysql.hub.replication as _repl
import mysql.hub.persistence as _persistence

import tests.utils

from mysql.hub.server import Group

class TestReplicationServices(unittest.TestCase):
    "Test replication service interface."

    def setUp(self):
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()

    def tearDown(self):
        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def test_import_topology(self):
        # Create topology M1 --> S2
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
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
        self.assertStatus(group, _executor.Job.SUCCESS)
        self.assertEqual(group[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(group[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(group[2], {"group_id": "group_id-1", "description":
                                    "description..."})

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-1")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.RUNNING],
            [str(slave.uuid), slave.address, False,
              _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

        # Create topology: M1 ---> S2, M1 ---> S3
        group_ = Group.fetch("group_id-1")
        group_.master = None
        group_.remove_server(master)
        _server.MySQLServer.remove(master)
        master = None
        group_.remove_server(slave)
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
        self.assertStatus(group, _executor.Job.SUCCESS)
        self.assertEqual(group[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(group[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(group[2], {"group_id": "group_id-2", "description":
                                    "description..."})

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-2")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.RUNNING],
            [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.RUNNING],
            [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

        group_ = Group.fetch("group_id-2")
        # Create topology: M1 ---> S2 ---> S3
        group_.master = None
        group_.remove_server(master)
        _server.MySQLServer.remove(master)
        master = None
        group_.remove_server(slave_1)
        _server.MySQLServer.remove(slave_1)
        slave_1 = None
        group_.remove_server(slave_2)
        _server.MySQLServer.remove(slave_2)
        slave_2 = None
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : [{2 : []}]}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        # Trying to import topology given a wrong group's id pattern.
        topology = self.proxy.group.import_topology(
            "group_id", "description...", master.address, user, passwd)
        self.assertStatus(topology, _executor.Job.ERROR)
        self.assertEqual(topology[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(topology[1][-1]["description"],
                         "Tried to execute action (_import_topology).")

        # Import topology.
        topology = self.proxy.group.import_topology(
            "group_id-2", "description...", master.address, user, passwd)
        self.assertStatus(topology, _executor.Job.SUCCESS)
        self.assertEqual(topology[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(topology[1][-1]["description"],
                         "Executed action (_import_topology).")
        expected_topology = {
            str(master.uuid): {"address": master.address, "slaves": [
            {str(slave_1.uuid): {"address": slave_1.address, "slaves": [
            {str(slave_2.uuid): {"address": slave_2.address,
            "slaves": []}}]}}]}}
        self.assertEqual(topology[2], expected_topology)

        # Look up a group.
        group = self.proxy.group.lookup_groups("group_id-3")
        self.assertStatus(group, _executor.Job.SUCCESS)
        self.assertEqual(group[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(group[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(group[2], {"group_id": "group_id-3", "description":
                                    "description..."})

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-3")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
             _server.MySQLServer.RUNNING],
            [str(slave_1.uuid), slave_1.address, False,
             _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

        # Look up a group.
        group = self.proxy.group.lookup_groups("group_id-4")
        self.assertStatus(group, _executor.Job.SUCCESS)
        self.assertEqual(group[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(group[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(group[2], {"group_id": "group_id-4", "description":
                                    "description..."})

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-4")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(slave_1.uuid), slave_1.address, True,
             _server.MySQLServer.RUNNING],
            [str(slave_2.uuid), slave_2.address, False,
             _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

    def test_switch_over_to(self):
        # Create topology: M1 ---> S2, M1 ---> S3
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        # Try to use a group that does not exist.
        status = self.proxy.group.switch_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Try to use a slave that does not exist.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.switch_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Try to use a slave without any master.
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)
        status = self.proxy.group.switch_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Try to use a slave with an invalid master.
        # The slave is not running and connected to the master.
        self.proxy.group.add("group_id", master.address, user, passwd)
        group = _server.Group.fetch("group_id")
        group.master = slave_1.uuid
        status = self.proxy.group.switch_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Everything is in place but the environment is not perfect.
        # The slave is not running and connected to the master.
        group.master = master.uuid
        _repl.stop_slave(slave_1, wait=True)
        _repl.reset_slave(slave_1, clean=True)
        status = self.proxy.group.switch_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Configure the slave but do not start it.
        _repl.switch_master(slave_1, master, user, passwd)
        status = self.proxy.group.switch_over(
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
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
             _server.MySQLServer.RUNNING],
            [str(slave_1.uuid), slave_1.address, False,
             _server.MySQLServer.RUNNING],
            [str(slave_2.uuid), slave_2.address, False,
             _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

        # Do the switch over.
        status = self.proxy.group.switch_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, False,
             _server.MySQLServer.RUNNING],
            [str(slave_1.uuid), slave_1.address, True,
             _server.MySQLServer.RUNNING],
            [str(slave_2.uuid), slave_2.address, False,
             _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)

    def test_switch_over(self):
        # Create topology: M1 ---> S2, M1 ---> S3, M1 ---> S4
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : []}, {2 : []}, {3 : []}]},
                                      user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)
        slave_3 = instances.get_instance(3)

        # Try to use a group that does not exist.
        status = self.proxy.group.switch_over("group_id")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_find_candidate_switch).")

        # Try to use a group without candidates.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.switch_over("group_id")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_find_candidate_switch).")

        # Try to use an invalid candidate (simulating that a server went down).
        invalid_server = _server.MySQLServer(
            _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
            "unknown_host:8080", user, passwd
            )
        _server.MySQLServer.add(invalid_server)
        group = _server.Group.fetch("group_id")
        group.add_server(invalid_server)
        status = self.proxy.group.switch_over("group_id")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_find_candidate_switch).")
        group.remove_server(invalid_server)
        _server.MySQLServer.remove(invalid_server)

        # Try to use a slave with an invalid master.
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)
        self.proxy.group.add("group_id", slave_3.address, user, passwd)
        group = _server.Group.fetch("group_id")
        group.master = slave_1.uuid
        status = self.proxy.group.switch_over("group_id")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_find_candidate_switch).")

        # Configure master, an invalid candidate and make a slave point to
        # a different master.
        self.proxy.group.add("group_id", master.address, user, passwd)
        group.master = master.uuid
        invalid_server = _server.MySQLServer(
            _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
            "unknown_host:8080", user, passwd
            )
        _server.MySQLServer.add(invalid_server)
        group = _server.Group.fetch("group_id")
        group.add_server(invalid_server)
        _repl.stop_slave(slave_3, wait=True)
        _repl.switch_master(slave_3, slave_2, user, passwd)

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
             _server.MySQLServer.RUNNING],
            [str(slave_1.uuid), slave_1.address, False,
             _server.MySQLServer.RUNNING],
            [str(slave_2.uuid), slave_2.address, False,
             _server.MySQLServer.RUNNING],
            [str(slave_3.uuid), slave_3.address, False,
             _server.MySQLServer.RUNNING],
            [str(invalid_server.uuid), invalid_server.address, False,
             _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(expected, retrieved)

        # Do the switch over.
        status = self.proxy.group.switch_over("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        retrieved.sort()
        self.assertNotEqual(expected, retrieved)

    def test_fail_over(self):
        # Create topology: M1 ---> S2, M1 ---> S3
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        # Try to use a group that does not exist.
        status = self.proxy.group.fail_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_fail).")

        # Try to use a slave that does not exist.
        self.proxy.group.create("group_id", "")
        status = self.proxy.group.fail_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_fail).")

        # Try to use a slave without any master.
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)
        status = self.proxy.group.fail_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_fail).")

        # Try to use a slave with an invalid master.
        # The slave is not running and connected to the master.
        self.proxy.group.add("group_id", master.address, user, passwd)
        group = _server.Group.fetch("group_id")
        group.master = slave_1.uuid
        status = self.proxy.group.fail_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_fail).")

        # Everything is in place but the environment is not perfect.
        # The group points to an invalid master.
        invalid_server = _server.MySQLServer(
            _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
            "unknown_host:8080", user, passwd
            )
        _server.MySQLServer.add(invalid_server)
        group = _server.Group.fetch("group_id")
        group.add_server(invalid_server)
        group.master = invalid_server.uuid
        status = self.proxy.group.fail_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_fail).")

        # Make the group point to a valid master.
        group.master = master.uuid

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.RUNNING],
             [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.RUNNING],
             [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.RUNNING],
             [str(invalid_server.uuid), invalid_server.address, False,
              _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(expected, retrieved)

        # Do the switch over.
        status = self.proxy.group.fail_over(
            "group_id", str(slave_1.uuid)
            )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, False,
              _server.MySQLServer.RUNNING],
             [str(slave_1.uuid), slave_1.address, True,
              _server.MySQLServer.RUNNING],
             [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.RUNNING],
             [str(invalid_server.uuid), invalid_server.address, False,
              _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(expected, retrieved)

    def test_promote_master(self):
        # Create topology: M1 ---> S2, M1 ---> S3
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        # Import topology.
        topology = self.proxy.group.import_topology(
            "group_id-0", "description...", master.address, user, passwd
            )
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

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-1")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.RUNNING],
             [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.RUNNING],
             [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.RUNNING]]
        retrieved.sort()
        expected.sort()
        self.assertEqual(expected, retrieved)

        # Do the fail over.
        fail = self.proxy.group.promote("group_id-1")
        self.assertStatus(fail, _executor.Job.SUCCESS)
        self.assertEqual(fail[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(fail[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id-1")
        self.assertStatus(servers, _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        retrieved.sort()
        self.assertNotEqual(expected, retrieved)

    def test_demote_master(self):
        # Create topology: M1 ---> S2, M1 ---> S3
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
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
        group.master = master.uuid

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, True,
              _server.MySQLServer.RUNNING],
             [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.RUNNING],
             [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.RUNNING]
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
                         "Executed action (_wait_candidates_demote).")

        # Look up servers.
        servers = self.proxy.group.lookup_servers("group_id")
        self.assertEqual(servers[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        retrieved = servers[2]
        expected = \
            [[str(master.uuid), master.address, False,
              _server.MySQLServer.RUNNING],
             [str(slave_1.uuid), slave_1.address, False,
              _server.MySQLServer.RUNNING],
             [str(slave_2.uuid), slave_2.address, False,
              _server.MySQLServer.RUNNING]
            ]
        retrieved.sort()
        expected.sort()
        self.assertEqual(retrieved, expected)
        self.assertFalse(_repl.is_slave_thread_running(slave_1))
        self.assertFalse(_repl.is_slave_thread_running(slave_2))

if __name__ == "__main__":
    unittest.main()

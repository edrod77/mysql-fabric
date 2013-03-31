"""Unit tests for checking some regular operations that use the
highavailability module.
"""
import unittest
import uuid as _uuid

import mysql.hub.executor as _executor
import mysql.hub.server as _server
import mysql.hub.replication as _repl
import mysql.hub.persistence as _persistence

import tests.utils

from mysql.hub.server import Group

class TestReplicationUse(unittest.TestCase):

    def setUp(self):
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()

    def tearDown(self):
        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def test_demote_promote(self):
        """Check the sequence demote and promote when some candidates have no
        information on GTIDs.
        """
        # Configure replication.
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        self.proxy.group.create("group_id", "")
        self.proxy.group.add("group_id", master.address, user, passwd)
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)
        self.proxy.group.promote("group_id", str(master.uuid))

        # Create some data.
        master.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        master.exec_stmt("USE test")
        master.exec_stmt("CREATE TABLE IF NOT EXISTS t_1(id INTEGER)")

        for server in [slave_2, slave_1, master]:
            # Demote the current master.
            status = self.proxy.group.demote("group_id")
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_wait_candidates_demote).")

            # Reset any information on GTIDs on a server.
            _repl.reset_slave(server, clean=True)
            _repl.reset_master(server)

            # Promote a new master.
            status = self.proxy.group.promote("group_id")
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

            # Create some data.
            server.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
            server.exec_stmt("USE test")
            server.exec_stmt("CREATE TABLE IF NOT EXISTS t_1(id INTEGER)")

    def test_promote_demote_remove(self):
        """Check what happens when one tries to promote a new master but the
        previous master was removed from the system.
        """
        # Configure replication.
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
        instances.destroy_instances()
        instances.configure_instances({0 : [{1 : []}, {2 : []}]}, user, passwd)
        master = instances.get_instance(0)
        slave_1 = instances.get_instance(1)
        slave_2 = instances.get_instance(2)

        self.proxy.group.create("group_id", "")
        self.proxy.group.add("group_id", master.address, user, passwd)
        self.proxy.group.add("group_id", slave_1.address, user, passwd)
        self.proxy.group.add("group_id", slave_2.address, user, passwd)

        # Promote a master.
        status = self.proxy.group.promote("group_id", str(master.uuid))
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Demote the current master.
        status = self.proxy.group.demote("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_wait_candidates_demote).")

        # Remove the previous master from the system.
        status = self.proxy.group.remove("group_id", str(master.uuid))
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_server).")

        # Promote any candidate to a master.
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

if __name__ == "__main__":
    unittest.main()

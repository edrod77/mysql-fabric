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

"""Unit tests for checking some regular operations that use the
highavailability module.
"""
import unittest
import uuid as _uuid

import mysql.fabric.executor as _executor
import mysql.fabric.server as _server
import mysql.fabric.replication as _repl
import mysql.fabric.persistence as _persistence
import mysql.fabric.errors as _errors

import tests.utils

from mysql.fabric.server import Group

class TestReplicationUse(unittest.TestCase):

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

    def test_demote_promote(self):
        """Check the sequence demote and promote when some candidates have no
        information on GTIDs.
        """
        # Configure replication.
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
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
                             "Executed action (_wait_slaves_demote).")

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
                         "Executed action (_wait_slaves_demote).")

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

    def test_check_unhealthy_slave(self):
        # Configure replication.
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
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

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)

        # Inject some events that makes a slave break.
        slave_1.set_session_binlog(False)
        slave_1.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        slave_1.exec_stmt("USE test")
        slave_1.exec_stmt("DROP TABLE IF EXISTS test")
        slave_1.exec_stmt("CREATE TABLE test (id INTEGER)")
        slave_1.set_session_binlog(True)

        slave_2.set_session_binlog(False)
        slave_2.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        slave_2.exec_stmt("USE test")
        slave_2.exec_stmt("DROP TABLE IF EXISTS test")
        slave_2.set_session_binlog(True)

        master.set_session_binlog(False)
        master.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        master.exec_stmt("USE test")
        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.set_session_binlog(True)
        master.exec_stmt("CREATE TABLE test (id INTEGER)")

        # Synchronize replicas.
        self.assertRaises(_errors.DatabaseError, _repl.sync_slave_with_master,
                          slave_1, master, timeout=0)
        _repl.sync_slave_with_master(slave_2, master, timeout=0)

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"],
            {"sql_running": False, "sql_error": "Error 'Table 'test' "
            "already exists' on query. Default database: 'test'. Query: "
            "'CREATE TABLE test (id INTEGER)'"}
            )
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)

        # Try to do a switch over to the faulty replica.
        status = self.proxy.group.promote("group_id", str(slave_1.uuid))
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Choose a new master.
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Synchronize replicas.
        self.assertRaises(_errors.DatabaseError, _repl.sync_slave_with_master,
                          slave_1, slave_2, timeout=0)
        _repl.sync_slave_with_master(master, slave_2, timeout=0)

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"],
            {"sql_running": False, "sql_error": "Error 'Table 'test' "
            "already exists' on query. Default database: 'test'. Query: "
            "'CREATE TABLE test (id INTEGER)'"}
            )
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], True)
        self.assertEqual(status[2][str(master.uuid)]["threads"], {})
        self.assertEqual(status[2][str(master.uuid)]["is_master"], False)

        # Choose a new master.
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Synchronize replicas.
        self.assertRaises(_errors.DatabaseError, _repl.sync_slave_with_master,
                          slave_1, master, timeout=0)
        _repl.sync_slave_with_master(slave_2, master, timeout=0)

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"],
            {"sql_running": False, "sql_error": "Error 'Table 'test' "
            "already exists' on query. Default database: 'test'. Query: "
            "'CREATE TABLE test (id INTEGER)'"}
            )
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)

    def test_check_no_healthy_slave(self):
        # Configure replication.
        user = "root"
        passwd = ""
        instances = tests.utils.MySQLInstances()
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

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)

        # Inject some events that make slaves break.
        slave_1.set_session_binlog(False)
        slave_1.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        slave_1.exec_stmt("USE test")
        slave_1.exec_stmt("DROP TABLE IF EXISTS test")
        slave_1.exec_stmt("CREATE TABLE test (id INTEGER)")
        slave_1.set_session_binlog(True)

        slave_2.set_session_binlog(False)
        slave_2.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        slave_2.exec_stmt("USE test")
        slave_2.exec_stmt("DROP TABLE IF EXISTS test")
        slave_2.exec_stmt("CREATE TABLE test (id INTEGER)")
        slave_2.set_session_binlog(True)

        master.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        master.exec_stmt("USE test")
        master.exec_stmt("SET sql_log_bin=0")
        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.exec_stmt("SET sql_log_bin=1")
        master.exec_stmt("CREATE TABLE test (id INTEGER)")

        # Synchronize replicas.
        self.assertRaises(_errors.DatabaseError, _repl.sync_slave_with_master,
                          slave_1, master, timeout=0)
        self.assertRaises(_errors.DatabaseError, _repl.sync_slave_with_master,
                          slave_2, master, timeout=0)

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"],
            {"sql_running": False, "sql_error": "Error 'Table 'test' "
            "already exists' on query. Default database: 'test'. Query: "
            "'CREATE TABLE test (id INTEGER)'"}
            )
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"],
            {"sql_running": False, "sql_error": "Error 'Table 'test' "
            "already exists' on query. Default database: 'test'. Query: "
            "'CREATE TABLE test (id INTEGER)'"}
            )
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)

        # Try to choose a new master through switch over.
        status = self.proxy.group.promote("group_id")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_find_candidate_switch).")

        # Try to reset the slave and restart slave.
        _repl.stop_slave(slave_1, wait=True)
        _repl.reset_slave(slave_1, clean=False)

        try:
            _repl.start_slave(slave_1, wait=True)
        except _errors.DatabaseError as error:
            self.assertEqual(
                str(error), "Error 'Table 'test' already exists' "
                "on query. Default database: 'test'. Query: 'CREATE "
                "TABLE test (id INTEGER)'"
                )

        # Synchronize replica.
        self.assertRaises(_errors.DatabaseError, _repl.sync_slave_with_master,
                          slave_1, master, timeout=0)

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertTrue(status[2][str(slave_1.uuid)]["threads"] ==
            {"sql_running": False, "sql_error": "Error 'Table 'test' "
            "already exists' on query. Default database: 'test'. Query: "
            "'CREATE TABLE test (id INTEGER)'"}
            )
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"],
            {"sql_running": False, "sql_error": "Error 'Table 'test' "
            "already exists' on query. Default database: 'test'. Query: "
            "'CREATE TABLE test (id INTEGER)'"}
            )
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)

        # Try to drop the table on the slave.
        _repl.stop_slave(slave_1, wait=True)
        _repl.reset_slave(slave_1, clean=False)
        slave_1.set_session_binlog(False)
        slave_1.exec_stmt("DROP TABLE IF EXISTS test")
        slave_1.set_session_binlog(True)
        _repl.start_slave(slave_1, wait=True)
        _repl.stop_slave(slave_2, wait=True)
        _repl.reset_slave(slave_2, clean=False)
        slave_2.set_session_binlog(False)
        slave_2.exec_stmt("DROP TABLE IF EXISTS test")
        slave_2.set_session_binlog(True)
        _repl.start_slave(slave_2, wait=True)

        # Synchronize replicas.
        _repl.sync_slave_with_master(slave_1, master, timeout=0)
        _repl.sync_slave_with_master(slave_2, master, timeout=0)

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)


if __name__ == "__main__":
    unittest.main()

"""Unit tests for checking some regular operations that use the
highavailability module.
"""
import unittest
import uuid as _uuid

import mysql.hub.executor as _executor
import mysql.hub.server as _server
import mysql.hub.replication as _repl
import mysql.hub.persistence as _persistence
import mysql.hub.errors as _errors

import tests.utils

from mysql.hub.server import Group

class TestReplicationUse(unittest.TestCase):

    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()
        tests.utils.cleanup_environment()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
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
        master_gtids = master.get_gtid_status()
        self.assertRaises(_errors.DatabaseError, _repl.wait_for_slave_gtid,
                          slave_1, master_gtids, timeout=0)
        _repl.wait_for_slave_gtid(slave_2, master_gtids, timeout=0)

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
        status = self.proxy.group.switch_over("group_id", str(slave_1.uuid))
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_check_candidate_switch).")

        # Choose a new master.
        status = self.proxy.group.switch_over("group_id")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        # Synchronize replicas.
        slave_gtids = slave_2.get_gtid_status()
        self.assertRaises(_errors.DatabaseError, _repl.wait_for_slave_gtid,
                          slave_1, slave_gtids, timeout=0)
        _repl.wait_for_slave_gtid(master, slave_gtids, timeout=0)

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
        master_gtids = master.get_gtid_status()
        self.assertRaises(_errors.DatabaseError, _repl.wait_for_slave_gtid,
                          slave_1, master_gtids, timeout=0)
        _repl.wait_for_slave_gtid(slave_2, master_gtids, timeout=0)

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
        master_gtids = master.get_gtid_status()
        self.assertRaises(_errors.DatabaseError, _repl.wait_for_slave_gtid, slave_1,
                          master_gtids, timeout=0)
        self.assertRaises(_errors.DatabaseError, _repl.wait_for_slave_gtid, slave_2,
                          master_gtids, timeout=0)

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
        status = self.proxy.group.switch_over("group_id")
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
        master_gtids = master.get_gtid_status()
        self.assertRaises(_errors.DatabaseError, _repl.wait_for_slave_gtid, slave_1,
                          master_gtids, timeout=0)

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
        master_gtids = master.get_gtid_status()
        _repl.wait_for_slave_gtid(slave_1, master_gtids, timeout=0)
        _repl.wait_for_slave_gtid(slave_2, master_gtids, timeout=0)

        # Check replication.
        status = self.proxy.group.check_group_availability("group_id")
        self.assertEqual(status[2][str(slave_1.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_1.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(slave_2.uuid)]["threads"], {})
        self.assertEqual(status[2][str(slave_2.uuid)]["is_master"], False)
        self.assertEqual(status[2][str(master.uuid)]["is_master"], True)

        # Clean up.
        master.exec_stmt("DROP DATABASE IF EXISTS test")
        master_gtids = master.get_gtid_status()
        _repl.wait_for_slave_gtid(slave_1, master_gtids, timeout=0)
        _repl.wait_for_slave_gtid(slave_2, master_gtids, timeout=0)

if __name__ == "__main__":
    unittest.main()

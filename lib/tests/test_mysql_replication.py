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

"""Unit tests for the configuration file handling.
"""
import re
import unittest
import uuid as _uuid

from collections import namedtuple

from mysql.fabric import (
    errors as _errors,
    persistence as persistence,
    )

from mysql.fabric.server import MySQLServer
from mysql.fabric.replication import *
# TODO: Remove the * and test the following functions:
# Unused import get_master_rpl_users from wildcard import
# Unused import check_slave_delay_health from wildcard import
# Unused import get_master_slaves from wildcard import

import tests.utils

# TODO: When the FakeMysql is pushed, change it and take care of the todos.
OPTIONS_MASTER = {
    "uuid" :  _uuid.UUID("80139491-08ed-11e2-b7bd-f0def124dcc5"),
    "address"  : tests.utils.MySQLInstances().get_address(0),
    "user" : "root"
}

OPTIONS_SLAVE = {
    "uuid" :  _uuid.UUID("811f03ff-08ed-11e2-b7bd-f0def124dcc5"),
    "address"  : tests.utils.MySQLInstances().get_address(1),
    "user" : "root"
}

class TestMySQLMaster(unittest.TestCase):
    """Unit test for the configuration file handling.
    """
    def setUp(self):
        """Configure the existing environment
        """
        uuid = MySQLServer.discover_uuid(**OPTIONS_MASTER)
        OPTIONS_MASTER["uuid"] = _uuid.UUID(uuid)
        # TODO: Change the initialization style.
        self.master = MySQLServer(**OPTIONS_MASTER)
        self.master.connect()
        reset_master(self.master)
        self.master.read_only = True
        self.master.read_only = False

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        self.master.disconnect()

    def test_master_binary_log(self):
        master = self.master
        # TODO: Test it also without binary log.
        # These tests requires to restart the master what will be done
        # with the FakeMySQL.

        # Get master status.
        check = re.compile('\w+-bin.000001')
        ret = get_master_status(master)
        self.assertNotEqual(check.match(ret[0][0]), None)

        # Reset Master.
        reset_master(master)
        ret = get_master_status(master)
        self.assertEqual(int(ret[0][1]), 151) # Format descriptor event.

    def test_master_health(self):
        master = self.master
        # TODO: Test it also without binary log.
        # TODO: Test it after removing rpl users.
        # These tests requires to restart the master what will be done
        # with the FakeMySQL.

        # Check health as a master before calling connect.
        master.disconnect()
        ret = check_master_issues(master)
        self.assertEqual(ret, {'is_running': False})

        # Check health as a master after calling connect.
        master.connect()
        ret = check_master_issues(master)
        self.assertEqual(ret, {})

class TestMySQLSlave(unittest.TestCase):
    """Unit test for the configuration file handling.
    """

    def setUp(self):
        """Configure the existing environment
        """
        uuid = MySQLServer.discover_uuid(**OPTIONS_MASTER)
        OPTIONS_MASTER["uuid"] = _uuid.UUID(uuid)
        self.master = MySQLServer(**OPTIONS_MASTER)
        self.master.connect()
        reset_master(self.master)

        uuid = MySQLServer.discover_uuid(**OPTIONS_SLAVE)
        OPTIONS_SLAVE["uuid"] = _uuid.UUID(uuid)
        self.slave = MySQLServer(**OPTIONS_SLAVE)
        self.slave.connect()
        stop_slave(self.slave, wait=True)
        reset_master(self.slave)
        reset_slave(self.slave, clean=True)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        stop_slave(self.slave, wait=True)
        self.slave.disconnect()
        self.master.disconnect()

    def test_switch_master(self):
        # TODO: Test it also without gtis so we can define binary
        # log positions.
        # These tests requires to restart the slave what will be done
        # with the FakeMySQL.

        # Set up replication.
        master = self.master
        slave = self.slave

        # Check that is slave is not connected to any master.
        self.assertFalse(is_slave_thread_running(slave, (IO_THREAD, )))
        self.assertNotEqual(slave_has_master(slave), str(master.uuid))

        # Switch to a master.
        switch_master(slave, master, "root", "")
        start_slave(slave, wait=True)
        self.assertTrue(is_slave_thread_running(slave, (IO_THREAD, )))
        # The IO_THREAD status and the UUID are not atomically updated.
        master_uuid = slave_has_master(slave)
        self.assertTrue(
            master_uuid == None or master_uuid == str(master.uuid)
            )

        # It is not possible to switch when replication is running.
        self.assertRaises(_errors.DatabaseError, switch_master, slave,
                          master, "root")

        # Reset and try to reconnect master and slave.
        stop_slave(slave, wait=True)
        reset_slave(slave, clean=True)
        switch_master(slave, master, "root", "")
        start_slave(slave, wait=True)
        self.assertTrue(is_slave_thread_running(slave, (IO_THREAD, )))
        self.assertEqual(slave_has_master(slave), str(master.uuid))

        # Change master's password, reset and try to reconnect master
        # and slave.
        stop_slave(slave, wait=True)
        master.set_session_binlog(False)
        master.exec_stmt(
            "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('foobar')"
            )
        master.set_session_binlog(True)
        switch_master(slave, master, "root", "foobar")
        start_slave(slave, wait=True)
        self.assertTrue(is_slave_thread_running(slave, (IO_THREAD, )))
        self.assertEqual(slave_has_master(slave), str(master.uuid))

        # Reset master's password, reset and try to reconnect master
        # and slave.
        stop_slave(slave, wait=True)
        master.set_session_binlog(False)
        master.exec_stmt(
            "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('')"
            )
        master.set_session_binlog(True)
        switch_master(slave, master, "root", "")
        start_slave(slave, wait=True)
        self.assertTrue(is_slave_thread_running(slave, (IO_THREAD, )))
        self.assertEqual(slave_has_master(slave), str(master.uuid))

    def test_slave_binary_log(self):
        # TODO: Test it also without binary log.
        # These tests requires to restart the slave what will be done
        # with the FakeMySQL.

        # Set up replication.
        master = self.master
        slave = self.slave
        switch_master(slave, master, "root")
        start_slave(slave, wait=True)

    def test_start_stop(self):
        # Set up replication.
        master = self.master
        slave = self.slave
        switch_master(slave, master, "root")

        # Start SQL Thread.
        start_slave(slave, wait=True, threads=(SQL_THREAD, ))
        status = get_slave_status(slave)
        self.assertEqual(status[0].Slave_IO_Running.upper(), "NO")
        self.assertEqual(status[0].Slave_SQL_Running.upper(), "YES")

        # Start IO Thread.
        start_slave(slave, wait=True, threads=(IO_THREAD, ))
        status = get_slave_status(slave)
        self.assertEqual(status[0].Slave_IO_Running.upper(), "YES")
        self.assertEqual(status[0].Slave_SQL_Running.upper(), "YES")

        # Stop IO Thread
        stop_slave(slave, wait=True, threads=(IO_THREAD, ))
        status = get_slave_status(slave)
        self.assertEqual(status[0].Slave_IO_Running.upper(), "NO")
        self.assertEqual(status[0].Slave_SQL_Running.upper(), "YES")

        # Stop IO Thread
        stop_slave(slave, wait=True, threads=(SQL_THREAD, ))
        status = get_slave_status(slave)
        self.assertEqual(status[0].Slave_IO_Running.upper(), "NO")
        self.assertEqual(status[0].Slave_SQL_Running.upper(), "NO")

    def test_wait_for_slave(self):
        # Set up replication.
        master = self.master
        slave = self.slave
        switch_master(slave, master, "root")

        # Wait for SQL Thread and IO Thread to stop. This times out.
        start_slave(slave, wait=True)
        self.assertRaises(_errors.TimeoutError, wait_for_slave_thread, slave,
                          timeout=1, wait_for_running=False)

        # Wait for SQL Thread and IO Thread to start. This times out.
        stop_slave(slave, wait=True)
        self.assertRaises(_errors.TimeoutError, wait_for_slave_thread, slave,
                          timeout=1, wait_for_running=True)

        master.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        master.exec_stmt("USE test")
        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.exec_stmt("CREATE TABLE test(id INTEGER)")
        binlog = get_master_status(master)
        binlog_file = binlog[0][0]
        binlog_pos = int(binlog[0][1])

        # Wait until the slave catches up. It returns False because
        # the threads are stopped.
        self.assertRaises(_errors.DatabaseError, wait_for_slave, slave,
                          binlog_file, binlog_pos, timeout=0)

        # Wait until the slave catches up. It returns True because
        # the threads are running.
        start_slave(slave, wait=True)
        wait_for_slave(slave, binlog_file, binlog_pos, timeout=0)

        # This times out because there are no new events.
        self.assertRaises(_errors.TimeoutError, wait_for_slave, slave,
                          binlog_file, 2 * binlog_pos, timeout=3)
        stop_slave(slave, wait=True)

        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.exec_stmt("CREATE TABLE test(id INTEGER)")

        # It throws an error because the threads are stopped.
        self.assertRaises(_errors.DatabaseError, sync_slave_with_master,
                          slave, master, timeout=0)

        # Wait until the slave catches up. It does not throw an error
        # because the threads are running.
        start_slave(slave, wait=True)
        sync_slave_with_master(slave, master, timeout=0)

        # This times out because there are no new events.
        gtid_executed = "%s:%s" % (str(master.uuid), "1-20")
        self.assertRaises(_errors.TimeoutError, wait_for_slave_gtid, slave,
                          gtid_executed, timeout=3)

    def test_check_rpl_health(self):
        # Set up replication.
        master = self.master
        slave = self.slave
        slave.disconnect()

        # Try to check the health when one cannot connect to the server.
        ret = check_slave_issues(slave)
        self.assertEqual(ret, {'is_running': False})

        # Try to check the health when change master has not been executed.
        slave.connect()
        ret = check_slave_issues(slave)
        self.assertEqual(ret, {'is_configured': False})

        # Try to check the health after executing change master.
        switch_master(slave, master, "root")
        ret = check_slave_issues(slave)
        self.assertEqual(ret, {'io_running': False, 'sql_running': False})

        # Try to check the health after starting one thread.
        start_slave(slave, wait=True, threads=(SQL_THREAD, ))
        ret = check_slave_issues(slave)
        self.assertEqual(ret, {'io_running': False})

        # Create data and synchronize to show there is no gtid behind.
        master.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        master.exec_stmt("USE test")
        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.exec_stmt("CREATE TABLE test(id INTEGER)")
        start_slave(slave, wait=True, threads=(IO_THREAD, ))
        sync_slave_with_master(slave, master, timeout=0)
        ret = check_slave_delay(slave, master)
        self.assertEqual(ret, {})

    def test_get_gtid_behind(self):
        # Set up replication.
        master = self.master
        slave = self.slave
        switch_master(slave, master, "root")

        # Check gtid that has no information on server_uuid.
        self.assertRaises(_errors.ProgrammingError, get_num_gtid, "1")

        sid_1 = "80139491-08ed-11e2-b7bd-f0def124dcc5"
        sid_2 = "99939491-08ed-11e2-b7bd-f0def124dcc5"

        # Check the pattern sid:trx_id.
        ret = get_num_gtid("%s:%s" % (sid_1, "5"))
        self.assertEqual(ret, 1)

        # Check the pattern sid:trx_id-trx_id.
        ret = get_num_gtid("%s:%s" % (sid_1, "5-10"))
        self.assertEqual(ret, 6)

        # Check the pattern sid:trx_id-trx_id, trx_id, trx_id-trx-id.
        ret = get_num_gtid("%s:%s,%s,%s" % (sid_1, "5-10", "20", "25-30"))
        self.assertEqual(ret, 13)

        # Check the pattern sid:trx_id-trx_id, sid:trx_id-trx-id.
        ret = get_num_gtid("%s:%s,%s:%s" % (sid_1, "5-10", sid_2, "5-6"))
        self.assertEqual(ret, 8)

        # Check the pattern sid:trx_id-trx_id, sid:trx_id-trx-id but filtering
        # server_uuids that are different from sid_2.
        ret = get_num_gtid("%s:%s,%s:%s" %
                           (sid_1, "5-10", sid_2, "5-6"), sid_2)
        self.assertEqual(ret, 2)

        # Check empty master_gtid_status and empty slave_gtid_status.
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        ret = get_slave_num_gtid_behind(slave, master_gtid_status)
        self.assertEqual(ret, 0)
        self.assertEqual(slave_gtid_status[0].GTID_EXECUTED, "")
        self.assertEqual(master_gtid_status[0].GTID_EXECUTED, "")

        # It is not possible to do any comparison if the master_gtid_status
        # is empty.
        slave.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        slave.exec_stmt("USE test")
        slave.exec_stmt("DROP TABLE IF EXISTS test")
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        self.assertRaises(_errors.InvalidGtidError, get_slave_num_gtid_behind,
                          slave, master_gtid_status)
        self.assertNotEqual(slave_gtid_status[0].GTID_EXECUTED, "")
        self.assertEqual(master_gtid_status[0].GTID_EXECUTED, "")

        # Check what happens if there are different sets of transactions.
        master.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        ret = get_slave_num_gtid_behind(slave, master_gtid_status)
        self.assertEqual(ret, 1)
        self.assertNotEqual(slave_gtid_status[0].GTID_EXECUTED, "")
        self.assertNotEqual(master_gtid_status[0].GTID_EXECUTED, "")

        # Check what happens if the slave_gtid_status is empty.
        reset_master(slave)
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        ret = get_slave_num_gtid_behind(slave, master_gtid_status)
        self.assertEqual(ret, 1)
        self.assertEqual(slave_gtid_status[0].GTID_EXECUTED, "")
        self.assertNotEqual(master_gtid_status[0].GTID_EXECUTED, "")


if __name__ == "__main__":
    unittest.main()

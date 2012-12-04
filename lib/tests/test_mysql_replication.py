"""Unit tests for the configuration file handling.
"""

import unittest
import uuid as _uuid

from collections import namedtuple

import mysql.hub.errors as _errors
import mysql.hub.utils as _utils
import mysql.hub.server_utils as _server_utils
import tests.utils as _test_utils

from mysql.hub.server import MySQLServer
from mysql.hub.persistence import MySQLPersister
from mysql.hub.replication import *

# TODO: When the FakeMysql is pushed, change it and take care of the todos.
OPTIONS_MASTER = {
    "uuid" :  _uuid.UUID("80139491-08ed-11e2-b7bd-f0def124dcc5"),
    "uri"  : "localhost:13000",
    "user" : "root"
}

OPTIONS_SLAVE = {
    "uuid" :  _uuid.UUID("811f03ff-08ed-11e2-b7bd-f0def124dcc5"),
    "uri"  : "localhost:13001",
    "user" : "root"
}

class TestMySQLMaster(unittest.TestCase):
    """Unit test for the configuration file handling.
    """

    __metaclass__ = _test_utils.SkipTests

    def setUp(self):
        self.persister = MySQLPersister("localhost:13000", "root", "")
        MySQLServer.create(self.persister)
        uuid = MySQLServer.discover_uuid(**OPTIONS_MASTER)
        OPTIONS_MASTER["uuid"] = _uuid.UUID(uuid)
        # TODO: Change the initialization style.
        self.master = MySQLServer(**OPTIONS_MASTER)
        self.master.connect()
        reset_master(self.master)
        self.master.read_only = True
        self.master.read_only = False

    def tearDown(self):
        self.master.disconnect()
        MySQLServer.drop(self.persister)

    def test_master_binary_log(self):
        master = self.master
        # TODO: Test it also without binary log.
        # These tests requires to restart the master what will be done
        # with the FakeMySQL.

        # Get master status.
        ret = get_master_status(master)
        self.assertEqual(ret[0][0], "master-bin.000001")

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
        ret = check_master_health(master)
        self.assertEqual(ret, [(False, ["Cannot connect to server"])])

        # Check health as a master after calling connect.
        master.connect()
        ret = check_master_health(master)
        self.assertEqual(ret, [(True, [])])

    def test_master_slaves(self):
        master = self.master
        # TODO: Test it with slaves.
        # These tests requires to restart the master what will be done
        # with the FakeMySQL.

        # Configure slave and check command.
        uuid = MySQLServer.discover_uuid(**OPTIONS_SLAVE)
        OPTIONS_SLAVE["uuid"] = _uuid.UUID(uuid)
        slave = MySQLServer(**OPTIONS_SLAVE)
        slave.connect()
        stop_slave(slave, wait=True)
        switch_master(slave, master, "root", "")
        start_slave(slave, wait=True)

        # Get Master slaves
        ret = get_master_slaves(master)
        if ret:
            uri = _server_utils.combine_host_port(ret[0].Host, ret[0].Port,
                _server_utils.MYSQL_DEFAULT_PORT)
            self.assertEqual(uri, "localhost:13001")


class TestMySQLSlave(unittest.TestCase):
    """Unit test for the configuration file handling.
    """

    __metaclass__ = _test_utils.SkipTests

    def setUp(self):
        self.persister = MySQLPersister("localhost:13000", "root", "")
        MySQLServer.create(self.persister)
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
        stop_slave(self.slave, wait=True)
        self.slave.disconnect()
        self.master.disconnect()
        MySQLServer.drop(self.persister)

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
        self.assertEqual(slave_has_master(slave), str(master.uuid))

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

        master.exec_stmt("USE test")
        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.exec_stmt("CREATE TABLE test(id INTEGER)")
        binlog = get_master_status(master)
        binlog_file = binlog[0][0]
        binlog_pos = int(binlog[0][1])

        # Wait until the slave catches up. It returns False because
        # the threads are stopped.
        self.assertFalse(wait_for_slave(slave, binlog_file, binlog_pos,
                                        timeout=0))

        # Wait until the slave catches up. It returns True because
        # the threads are running.
        start_slave(slave, wait=True)
        self.assertTrue(wait_for_slave(slave, binlog_file, binlog_pos,
                                       timeout=0))

        # This times out because there are no new events.
        self.assertRaises(_errors.TimeoutError, wait_for_slave, slave,
                          binlog_file, 2 * binlog_pos)
        stop_slave(slave, wait=True)

        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.exec_stmt("CREATE TABLE test(id INTEGER)")
        gtid_status = master.get_gtid_status()

        # It returns False because the threads are stopped.
        self.assertFalse(wait_for_slave_gtid(slave, gtid_status, timeout=0))

        # Wait until the slave catches up. It returns True because
        # the threads are running.
        start_slave(slave, wait=True)
        self.assertTrue(wait_for_slave_gtid(slave, gtid_status, timeout=0))

        # This times out because there are no new events.
        Row = namedtuple("Row", ["GTID_DONE", "GTID_LOST", "GTID_OWNED"])
        gtid_done = "%s:%s" % (str(master.uuid), "1-20")
        gtid_status = [Row(gtid_done, "", "")]
        self.assertRaises(_errors.TimeoutError, wait_for_slave_gtid, slave,
                          gtid_status)


    def test_check_rpl_health(self):
        # Set up replication.
        master = self.master
        slave = self.slave
        slave.disconnect()

        # Try to check the health when one cannot connect to the server.
        master_status = get_master_status(master)
        gtid_status = master.get_gtid_status()
        ret = check_slave_health(slave, master.uuid, master_status,
                                 gtid_status, 5, 5, 5)
        self.assertEqual(ret, [(False, ["Cannot connect to server",
                                        "Cannot connect to server"])])

        # Try to check the health when change master has not been executed.
        slave.connect()
        ret = check_slave_health(slave, master.uuid, master_status,
                                 gtid_status, 5, 5, 5)
        self.assertEqual(ret, [(False, ["Not connected or not running.",
                                       "Not connected or not running."])])

        # Try to check the health after executing change master.
        switch_master(slave, master, "root")
        ret = check_slave_health(slave, master.uuid, master_status,
                                 gtid_status, 5, 5, 5)
        self.assertEqual(ret, \
          [(False, ['IO thread is not running.', 'SQL thread is not running.'])])

        # Try to check the health after starting one thread.
        start_slave(slave, wait=True, threads=(SQL_THREAD, ))
        ret = check_slave_health(slave, master.uuid, master_status,
                                 gtid_status, 5, 5, 5)
        self.assertEqual(ret, [(False, ['IO thread is not running.'])])

        # Create data and synchronize to show there is not gtid behind.
        master.exec_stmt("USE test")
        master.exec_stmt("DROP TABLE IF EXISTS test")
        master.exec_stmt("CREATE TABLE test(id INTEGER)")
        start_slave(slave, wait=True, threads=(IO_THREAD, ))
        master_status = get_master_status(master)
        gtid_status = master.get_gtid_status()
        self.assertTrue(wait_for_slave_gtid(slave, gtid_status, timeout=0))
        ret = check_slave_health(slave, master.uuid, master_status,
                                 gtid_status, 5, 5, 5)
        self.assertEqual(ret, [(True, [])])

        # Notice that a negative delay makes the function return False
        # the slave reports zero as the minimum value.
        ret = check_slave_health(slave, master.uuid, master_status,
                                 gtid_status, -1, 5, 5)
        self.assertEqual(ret,
            [(False, ['Slave delay is 0 seconds behind master.'])])

        # Stop and create data to show there is gtid behind.
        stop_slave(slave, wait=True)
        for var in range(1, 12):
            master.exec_stmt("INSERT INTO test(id) VALUES(%d)" % (var))
        master_status = get_master_status(master)
        gtid_status = master.get_gtid_status()
        ret = check_slave_health(slave, master.uuid, master_status,
                                 gtid_status, 5, 5, 5)
        self.assertEqual(ret, \
          [(False, ['IO thread is not running.', 'SQL thread is not running.',
                    'Slave has 11 transactions behind master.'])])

    def test_get_gtid_behind(self):
        # Set up replication.
        master = self.master
        slave = self.slave
        switch_master(slave, master, "root")

        # Check gtid that has no information on server_uuid.
        self.assertRaises(_errors.ProgrammingError, get_num_gtid_behind, "1")

        sid_1 = "80139491-08ed-11e2-b7bd-f0def124dcc5"
        sid_2 = "99939491-08ed-11e2-b7bd-f0def124dcc5"

        # Check the pattern sid:trx_id.
        ret = get_num_gtid_behind("%s:%s" % (sid_1, "5"))
        self.assertEqual(ret, 1)

        # Check the pattern sid:trx_id-trx_id.
        ret = get_num_gtid_behind("%s:%s" % (sid_1, "5-10"))
        self.assertEqual(ret, 6)

        # Check the pattern sid:trx_id-trx_id, trx_id, trx_id-trx-id.
        ret = get_num_gtid_behind("%s:%s,%s,%s" % \
                                             (sid_1, "5-10", "20", "25-30"))
        self.assertEqual(ret, 13)

        # Check the pattern sid:trx_id-trx_id, sid:trx_id-trx-id.
        ret = get_num_gtid_behind("%s:%s,%s:%s" %
                                  (sid_1, "5-10", sid_2, "5-6"))
        self.assertEqual(ret, 8)

        # Check the pattern sid:trx_id-trx_id, sid:trx_id-trx-id but filtering
        # server_uuids that are different from sid_2.
        ret = get_num_gtid_behind("%s:%s,%s:%s" %
                                  (sid_1, "5-10", sid_2, "5-6"), sid_2)
        self.assertEqual(ret, 2)

        # Check empty master_gtid_status and empty slave_gtid_status.
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        ret = get_slave_num_gtid_behind(slave, master_gtid_status)
        self.assertEqual(ret, 0)
        self.assertEqual(slave_gtid_status[0].GTID_DONE, "")
        self.assertEqual(master_gtid_status[0].GTID_DONE, "")

        # It is not possible to do any comparison if the master_gtid_status
        # is empty.
        slave.exec_stmt("USE test")
        slave.exec_stmt("DROP TABLE IF EXISTS test");
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        self.assertRaises(_errors.ProgrammingError, get_slave_num_gtid_behind,
                          slave, master_gtid_status)
        self.assertNotEqual(slave_gtid_status[0].GTID_DONE, "")
        self.assertEqual(master_gtid_status[0].GTID_DONE, "")

        # Check what happens if there are different sets of transactions.
        master.exec_stmt("USE test")
        master.exec_stmt("DROP TABLE IF EXISTS test");
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        ret = get_slave_num_gtid_behind(slave, master_gtid_status)
        self.assertEqual(ret, 1)
        self.assertNotEqual(slave_gtid_status[0].GTID_DONE, "")
        self.assertNotEqual(master_gtid_status[0].GTID_DONE, "")

        # Check what happens if the slave_gtid_status is empty.
        reset_master(slave)
        master_gtid_status = master.get_gtid_status()
        slave_gtid_status = slave.get_gtid_status()
        ret = get_slave_num_gtid_behind(slave, master_gtid_status)
        self.assertEqual(ret, 1)
        self.assertEqual(slave_gtid_status[0].GTID_DONE, "")
        self.assertNotEqual(master_gtid_status[0].GTID_DONE, "")


if __name__ == "__main__":
    unittest.main()

"""Unit tests for testing MySQLServer.
"""

import unittest
import uuid as _uuid

import mysql.hub.errors as _errors
import mysql.hub.persistence as persistence
import mysql.hub.server_utils as _server_utils

from mysql.hub.server import MySQLServer, Group, ConnectionPool

import tests.utils

# TODO: When the FakeMysql is pushed, change it and take care of the todos.
OPTIONS = {
    "uuid" : None,
    "address"  : tests.utils.MySQLInstances().get_address(0),
    "user" : "root"
}

class TestMySQLServer(unittest.TestCase):
    """Unit test for testing MySQLServer.
    """
    def setUp(self):
        from __main__ import options
        persistence.init(host=options.host, port=options.port,
                          user=options.user, password=options.password)
        persistence.init_thread()

        uuid = MySQLServer.discover_uuid(**OPTIONS)
        OPTIONS["uuid"] = _uuid.UUID(uuid)
        self.server = MySQLServer(**OPTIONS)

    def tearDown(self):
        self.server.disconnect()
        persistence.deinit_thread()
        persistence.deinit()

    def test_wrong_uuid(self):
        # Check wrong uuid.
        OPTIONS["uuid"] = _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5")
        server = MySQLServer(**OPTIONS)
        self.assertRaises(_errors.MismatchUuidError, server.connect)

    def test_properties(self):
        server = self.server

        # Check property user.
        self.assertEqual(server.user, "root")
        server.user = "user"
        self.assertEqual(server.user, "user")
        server.user = "root"

        # Check property passwd.
        self.assertEqual(server.passwd, None)
        server.passwd = "passwd"
        self.assertEqual(server.passwd, "passwd")
        server.passwd = None

        # Create instance without connecting it with a server.
        self.assertEqual(server.read_only, None)
        self.assertEqual(server.server_id, None)
        self.assertEqual(server.gtid_enabled, None)
        self.assertEqual(server.binlog_enabled, None)
        self.assertEqual(server.version, None)
        self.assertEqual(server.default_charset, "latin1")

        # Bind instance to a server.
        server.connect()
        self.assertNotEqual(server.read_only, None)
        self.assertNotEqual(server.server_id, 0)
        self.assertEqual(server.gtid_enabled, True)
        self.assertEqual(server.binlog_enabled, True)
        self.assertEqual(server.default_charset, "latin1")

        # Check read_only property.
        server.read_only = True
        self.assertEqual(server.read_only, True)
        server.read_only = False
        self.assertEqual(server.read_only, False)

    def test_version(self):
        server = self.server
        server.connect()

        # Check version.
        self.assertFalse(server.check_version_compat((7, 0, 0)))
        self.assertFalse(server.check_version_compat((6, 0, 0)))
        #TODO: Check when version is composed only with numbers 5.5.7.
        #TODO: Create a generic function so we don't need to update
        #      it every time a new release is created.

    def test_gtid(self):
        server = self.server
        server.connect()

        # Executed gtids cannot be compared because we may have executed
        # some statements in other tests.
        for record in server.get_gtid_status():
            executed = record.GTID_EXECUTED.lower()
            self.assertTrue(executed.find(str(server.uuid)) != -1)
            self.assertEqual(record.GTID_PURGED, "")
            self.assertEqual(record.GTID_OWNED, "")
        #TODO: Test with gtids disabled.

    def test_storage(self):
        server = self.server
        server.connect()
        self.assertTrue(server.has_storage_engine("Innodb"))
        self.assertTrue(server.has_storage_engine(""))
        self.assertFalse(server.has_storage_engine("Unknown"))

    def test_connection_operations(self):
        server = self.server
        server.connect()

        server.set_session_binlog(False)
        self.assertFalse(server.session_binlog_enabled())
        server.set_session_binlog(True)
        self.assertTrue(server.session_binlog_enabled())

        server.set_foreign_key_checks(False)
        self.assertFalse(server.foreign_key_checks_enabled())
        server.set_foreign_key_checks(True)
        self.assertTrue(server.foreign_key_checks_enabled())

    def test_binlog(self):
        server = self.server
        server.connect()

        for record in server.get_binary_logs():
            self.assertTrue(record.Log_name in 
                ("master-bin.000001", "mysqld-bin.000001", "slave-bin.000001"))
        # TODO: Test with binlog disabled.

    def test_exec_stmt_options(self):
        server = self.server
        server.connect()

        # Populate testing tables.
        server.exec_stmt("USE test")
        server.exec_stmt("DROP TABLE IF EXISTS test_1")
        server.exec_stmt("CREATE TABLE test_1(id INTEGER)")
        server.exec_stmt("DROP TABLE IF EXISTS test_2")
        server.exec_stmt("CREATE TABLE test_2(id INTEGER)")
        for cont in range(1, 10):
            server.exec_stmt("INSERT INTO test_1 VALUES(%s)",
                             {"params" : (cont,)})

        # Test raw: True fetch : True
        ret = server.exec_stmt("SELECT COUNT(*) FROM test_1",
                               {"raw" : True, "fetch" : True})
        self.assertEqual(int(ret[0][0]), 9)

        # Test raw: False fetch : True
        ret = server.exec_stmt("SELECT COUNT(*) FROM test_1",
                               {"raw" : False, "fetch" : True})
        self.assertEqual(ret[0][0], 9)

        # Test raw: False fetch : False
        cursor = server.exec_stmt("SELECT COUNT(*) FROM test_1",
                                  {"raw" : False, "fetch" : False})
        ret = cursor.fetchone()
        self.assertEqual(ret[0], 9)

        # Test raw: True fetch : False
        cursor = server.exec_stmt("SELECT COUNT(*) FROM test_1",
                                  {"raw" : False, "fetch" : False})
        ret = cursor.fetchone()
        self.assertEqual(int(ret[0]), 9)

        # Nothing to be fetched.
        ret = server.exec_stmt("SELECT * FROM test_2")
        self.assertEqual(ret, [])

        # Unknown table.
        self.assertRaises(_errors.DatabaseError, server.exec_stmt,
                          "SELECT * FROM test_3")

        # Test option columns
        ret = server.exec_stmt("SELECT COUNT(*) COUNT FROM test_1",
                               {"columns" : True})
        self.assertEqual(int(ret[0][0]), 9)
        self.assertEqual(int(ret[0].COUNT), 9)

    def test_is_alive(self):
        # Check if server is alive.
        server = self.server
        self.assertFalse(server.is_alive())
        server.connect()
        self.assertTrue(server.is_alive())

    def test_utilities(self):
        # Test a function that gets host and port and returns
        # host:port
        address = _server_utils.combine_host_port(None, None, 3306)
        self.assertEqual(address, "unknown host:3306")

        address = _server_utils.combine_host_port("", None, 3306)
        self.assertEqual(address, "unknown host:3306")

        address = _server_utils.combine_host_port(None, "", 3306)
        self.assertEqual(address, "unknown host:3306")

        address = _server_utils.combine_host_port("host", "port", 3306)
        self.assertEqual(address, "host:port")

        address = _server_utils.combine_host_port("host", 1500, 3306)
        self.assertEqual(address, "host:1500")

        address = _server_utils.combine_host_port("127.0.0.1", 1500, 3306)
        self.assertEqual(address, "localhost:1500")

        # Test a function that gets host:port and returns (host, port)
        host_port = _server_utils.split_host_port("", 3306)
        self.assertEqual(host_port, ("", 3306))

        host_port = _server_utils.split_host_port(":", 3306)
        self.assertEqual(host_port, ("", ""))

        host_port = _server_utils.split_host_port("host:", 3306)
        self.assertEqual(host_port, ("host", ""))

        host_port = _server_utils.split_host_port(":port", 3306)
        self.assertEqual(host_port, ("", "port"))

        host_port = _server_utils.split_host_port("host:port", 3306)
        self.assertEqual(host_port, ("host", "port"))

    def test_server_id(self):
        # Configure
        uuid = MySQLServer.discover_uuid(**OPTIONS)
        OPTIONS["uuid"] = _uuid.UUID(uuid)
        server_1 = MySQLServer(**OPTIONS)
        server_2 = MySQLServer(**OPTIONS)

        # Check that two different objects represent the same server.
        self.assertEqual(server_1, server_2)

        # Check that a dictionary with server_1 and server_2 has in
        # fact only one entry.
        hash_info = {}
        hash_info[server_1] = server_1
        hash_info[server_2] = server_2
        self.assertEqual(len(hash_info), 1)

class TestConnectionPool(unittest.TestCase):
    def setUp(self):
        from __main__ import options
        persistence.init(host=options.host, port=options.port,
                         user=options.user, password=options.password)
        persistence.init_thread()

    def tearDown(self):
        persistence.deinit_thread()
        persistence.deinit()

    def test_connection_pool(self):
        # Configuration
        uuid = MySQLServer.discover_uuid(**OPTIONS)
        OPTIONS["uuid"] = uuid = _uuid.UUID(uuid)
        server_1 = MySQLServer(**OPTIONS)
        server_2 = MySQLServer(**OPTIONS)
        cnx_pool = ConnectionPool()

        # Purge connections and check the number of connections in
        # the pool.
        cnx_pool.purge_connections(uuid)
        self.assertEqual(cnx_pool.get_number_connections(uuid), 0)

        # Connect and check the number of connections in the pool.
        server_1.connect()
        server_2.connect()
        self.assertEqual(cnx_pool.get_number_connections(uuid), 0)

        # Delete one of the servers and check the number of
        # connections in the pool.
        del server_1
        self.assertEqual(cnx_pool.get_number_connections(uuid), 1)

        # Delete one of the servers and check the number of
        # connections in the pool.
        del server_2
        self.assertEqual(cnx_pool.get_number_connections(uuid), 2)

        # Purge connections and check the number of connections in
        # the pool. However, call purge_connections twice.
        cnx_pool.purge_connections(uuid)
        self.assertEqual(cnx_pool.get_number_connections(uuid), 0)
        cnx_pool.purge_connections(uuid)
        self.assertEqual(cnx_pool.get_number_connections(uuid), 0)


class TestGroup(unittest.TestCase):
    def setUp(self):
        from __main__ import options
        persistence.init(host=options.host, port=options.port,
                         user=options.user, password=options.password)
        persistence.init_thread()

    def tearDown(self):
        persistence.deinit_thread()
        persistence.deinit()

    def test_properties(self):
        set_of_groups = set()
        group_1 = Group.add("mysql.com", "First description.")
        group_2 = Group.add("oracle.com", "First description.")
        self.assertEqual(group_1.group_id, "mysql.com")
        group_1.description = "New description."
        self.assertEqual(group_1.description, "New description.")
        self.assertEqual(group_1, group_1)
        self.assertFalse(group_1 == group_2)

        set_of_groups.add(group_1)
        set_of_groups.add(group_2)
        self.assertEqual(len(set_of_groups), 2)
        set_of_groups.add(group_1)
        self.assertEqual(len(set_of_groups), 2)
        group_1.remove()
        group_2.remove()


    def test_managment(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer(**options_1)
        MySQLServer.add(options_1["uuid"], options_1["address"], None, None)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer(**options_2)
        MySQLServer.add(options_2["uuid"], options_2["address"], None, None)
        group_1 = Group.add("oracle.com", "First description.")

        # Add servers to a group
        group_1.add_server(server_1)
        group_1.add_server(server_2)
        self.assertRaises(_errors.DatabaseError, group_1.add_server, server_1)
        self.assertEqual(len(group_1.servers()), 2)

        # Remove servers to a group
        group_1.remove_server(server_1)
        group_1.remove_server(server_2)
        group_1.remove_server(server_1)
        self.assertEqual(len(group_1.servers()), 0)

if __name__ == "__main__":
    unittest.main()

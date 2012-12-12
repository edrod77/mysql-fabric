"""Unit tests for testing MySQLServer.
"""

import unittest
import uuid as _uuid

import mysql.hub.errors as _errors
import mysql.hub.persistence as persistence

from mysql.hub.server import MySQLServer
from mysql.hub.persistence import MySQLPersister

# TODO: When the FakeMysql is pushed, change it and take care of the todos.
OPTIONS = {
    "uuid" : None,
    "uri"  : "localhost:13000",
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

    def test_wrong_connection(self):
        server = self.server

        # Trying to get a new connection.
        self.assertRaises(_errors.DatabaseError, server.connection)

        # Check what happens when an attempt to connect fails.
        server.passwd = "wrong"
        self.assertRaises(_errors.DatabaseError, server.connect)
        server.passwd = ""

        self.assertRaises(_errors.DatabaseError, server.exec_stmt,
                          "SELECT VERSION()")
        server.connect()
        server.exec_stmt("SELECT VERSION()")
        server.disconnect()
        self.assertRaises(_errors.DatabaseError, server.exec_stmt,
                          "SELECT VERSION()")

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
        self.assertEqual(server.read_only, False)
        self.assertEqual(server.server_id, 1)
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
        self.assertFalse(server.check_version_compat((7,0,0)))
        self.assertFalse(server.check_version_compat((6,0,0)))
        #TODO: Check when version is composed only with numbers 5.5.7.
        #TODO: Create a generic function so we don't need to update
        #      it every time a new release is created.

    def test_gtid(self):
        server = self.server
        server.connect()

        # Executed gtids cannot be compared because we may have executed
        # some statements in other tests.
        for record in server.get_gtid_status():
           self.assertTrue(record.GTID_EXECUTED.find(str(server.uuid)) != -1)
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
           self.assertEqual(record.Log_name, "master-bin.000001")
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
        for cont in range(1,10):
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

    def test_connect_options(self):
        server = self.server
        server.connect()

        return                #TODO: Figure of if this test is needed.

        # Trying to create a new connection overriding the host.
        params= {"host" : "unknown"}
        self.assertRaises(_errors.ConfigurationError, server.connect, **params)
        self.assertEqual(server.uri, "localhost:13000")

        # Create a new connection but notice that default database
        # is not set.
        server.connect()
        self.assertRaises(_errors.DatabaseError, server.exec_stmt,
                          "DROP TABLE IF EXISTS test")
        server.disconnect()
        params= {"database" : "test"}
        server.connect(**params)
        server.exec_stmt("DROP TABLE IF EXISTS test")
        server.disconnect()

if __name__ == "__main__":
    unittest.main()

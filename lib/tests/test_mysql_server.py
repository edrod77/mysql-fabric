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

"""Unit tests for testing MySQLServer.
"""
import re
import unittest
import uuid as _uuid
import tests.utils
import mysql.fabric.errors as _errors
import mysql.fabric.server_utils as _server_utils

from mysql.fabric.server import (
    MySQLServer,
    Group,
    ConnectionPool,
)

OPTIONS = {
    "uuid" : None,
    "address"  : tests.utils.MySQLInstances().get_address(0),
    "user" : tests.utils.MySQLInstances().user,
    "passwd" : tests.utils.MySQLInstances().passwd,
}

class TestMySQLServer(unittest.TestCase):
    """Unit test for testing MySQLServer.
    """
    def setUp(self):
        """Configure the existing environment
        """
        uuid = MySQLServer.discover_uuid(OPTIONS["address"])
        OPTIONS["uuid"] = _uuid.UUID(uuid)
        self.server = MySQLServer(**OPTIONS)
        MySQLServer.add(self.server)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        self.server.disconnect()
        MySQLServer.remove(self.server)

    def test_wrong_uuid(self):
        """Test what happens when a server has a wrong uuid.
        """
        # Check wrong uuid.
        OPTIONS["uuid"] = _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5")
        server = MySQLServer(**OPTIONS)
        self.assertRaises(_errors.UuidError, server.connect)
        server.disconnect()
        ConnectionPool().purge_connections(OPTIONS["uuid"])

    def test_properties(self):
        """Test setting MySQLServer's properties.
        """
        server = self.server

        # Check property user.
        self.assertEqual(server.user, tests.utils.MySQLInstances().user)
        server.user = "user"
        self.assertEqual(server.user, "user")
        server.user = tests.utils.MySQLInstances().user

        # Check property passwd.
        self.assertEqual(server.passwd, tests.utils.MySQLInstances().passwd)
        server.passwd = "passwd"
        self.assertEqual(server.passwd, "passwd")
        server.passwd = tests.utils.MySQLInstances().passwd

        # Check property status.
        self.assertEqual(server.status, MySQLServer.SECONDARY)
        server.status = MySQLServer.FAULTY
        self.assertEqual(server.status, MySQLServer.FAULTY)
        fetched_server = MySQLServer.fetch(server.uuid)
        self.assertEqual(server.status, fetched_server.status)
        server.status = MySQLServer.SECONDARY
        fetched_server = MySQLServer.fetch(server.uuid)
        self.assertEqual(server.status, fetched_server.status)

        # Check property mode.
        self.assertEqual(server.mode, MySQLServer.READ_ONLY)
        server.mode = MySQLServer.OFFLINE
        self.assertEqual(server.mode, MySQLServer.OFFLINE)
        fetched_server = MySQLServer.fetch(server.uuid)
        self.assertEqual(server.mode, fetched_server.mode)
        server.mode = MySQLServer.READ_ONLY
        fetched_server = MySQLServer.fetch(server.uuid)
        self.assertEqual(server.mode, fetched_server.mode)

        # Check property weight.
        self.assertEqual(server.weight, MySQLServer.DEFAULT_WEIGHT)
        server.weight = 0.1
        self.assertEqual(server.weight, 0.1)
        fetched_server = MySQLServer.fetch(server.uuid)
        self.assertEqual(server.weight, fetched_server.weight)
        server.weight = MySQLServer.DEFAULT_WEIGHT
        fetched_server = MySQLServer.fetch(server.uuid)
        self.assertEqual(server.weight, fetched_server.weight)

        # Create instance without connecting it with a server.
        self.assertEqual(server.read_only, None)
        self.assertEqual(server.server_id, None)
        self.assertEqual(server.gtid_enabled, None)
        self.assertEqual(server.binlog_enabled, None)
        self.assertEqual(server.version, None)

        # Bind instance to a server.
        server.connect()
        self.assertNotEqual(server.read_only, None)
        self.assertNotEqual(server.server_id, 0)
        self.assertEqual(server.gtid_enabled, True)
        self.assertEqual(server.binlog_enabled, True)

        # Check read_only property.
        server.read_only = True
        self.assertEqual(server.read_only, True)
        server.read_only = False
        self.assertEqual(server.read_only, False)

    def test_version(self):
        """Check MySQLServer's version.
        """
        server = self.server
        server.connect()

        # Check version.
        self.assertFalse(server.check_version_compat((7, 0, 0)))
        self.assertFalse(server.check_version_compat((6, 0, 0)))
        # Note this function needs to be updated to update every
        # time a new release is created.

    def test_gtid(self):
        """Check MySQLServer's GTIDs.
        """
        server = self.server
        server.connect()

        for record in server.get_gtid_status():
            self.assertEqual(record.GTID_EXECUTED, "")
            self.assertEqual(record.GTID_PURGED, "")
            self.assertEqual(record.GTID_OWNED, "")
        # Note this is only being tested with GTIDs.


    def test_storage(self):
        """Check MySQLServer's storage.
        """
        server = self.server
        server.connect()
        self.assertTrue(server.has_storage_engine("Innodb"))
        self.assertTrue(server.has_storage_engine(""))
        self.assertFalse(server.has_storage_engine("Unknown"))

    def test_session_properties(self):
        """Test some session's properties.
        """
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
        """Test binary logging is supported.
        """
        server = self.server
        server.connect()

        check = re.compile('\w+-bin.000001')
        for record in server.get_binary_logs():
            self.assertNotEqual(check.match(record.Log_name), None)
        # Note this is only being tested with the binary log.

    def test_exec_stmt_options(self):
        """Test statement's execution.
        """
        server = self.server
        server.connect()

        # Populate testing tables.
        server.exec_stmt("CREATE DATABASE IF NOT EXISTS test")
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

    def test_is_connected(self):
        """Check whether MySQLServer is alive or not.
        """
        # Check if server is alive.
        server = self.server
        self.assertTrue(server.is_alive())
        self.assertFalse(server.is_connected())
        server.connect()
        self.assertTrue(server.is_connected())

    def test_utilities(self):
        """Check MySQLServer's utilities module.
        """
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
        """Test MySQLServer's uuid.
        """
        # Configure
        uuid = MySQLServer.discover_uuid(OPTIONS["address"])
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

    def test_persister_id(self):
        """Test Persister's uuid.
        """
        # Get persister'a address.
        instances = tests.utils.MySQLInstances()
        address = instances.state_store_address
        user = instances.root_user
        passwd = instances.root_passwd

        # Try to manage the MySQLPersister.
        uuid = MySQLServer.discover_uuid(
            address=address, user=user, passwd=passwd
        )
        server = MySQLServer(_uuid.UUID(uuid), address, user, passwd)
        self.assertRaises(_errors.ServerError, MySQLServer.add, server)

    def test_privileges(self):
        """Test whether user's have the appropriate privileges.
        """
        # Some privileges
        MINIMUM_PRIVILEGES = [
            "REPLICATION SLAVE", "REPLICATION CLIENT", "SUPER",
            "SHOW DATABASES", "RELOAD"
        ]

        # Connect to server as root and create temporary user.
        uuid = MySQLServer.discover_uuid(OPTIONS["address"])
        server = MySQLServer(
            _uuid.UUID(uuid), OPTIONS["address"],
            tests.utils.MySQLInstances().root_user,
            tests.utils.MySQLInstances().root_passwd
        )
        ConnectionPool().purge_connections(_uuid.UUID(uuid))
        server.connect()
        server.set_session_binlog(False)
        server.exec_stmt(
            "CREATE USER 'jeffrey'@'%%' IDENTIFIED BY 'mypass'"
        )

        # Check if jeffrey (temporary user) has the appropriate privileges.
        # There is not privilege associate to jeffrey.
        new_server = MySQLServer(
            _uuid.UUID(uuid), OPTIONS["address"], "jeffrey", "mypass"
        )
        new_server.connect()
        self.assertFalse(
            new_server.has_privileges(MINIMUM_PRIVILEGES)
        )

        # Check if jeffrey (temporary user) has the appropriate privileges.
        # Grant required privileges except RELOAD
        # There is no RELOAD on a global level.
        privileges=", ".join([priv for priv in MINIMUM_PRIVILEGES
             if priv != "RELOAD"]
        )
        server.exec_stmt(
            "GRANT {privileges} ON *.* TO 'jeffrey'@'%%'".format(
            privileges=privileges)
        )
        server.exec_stmt("FLUSH PRIVILEGES")
        self.assertFalse(
            new_server.has_privileges(MINIMUM_PRIVILEGES)
        )

        # Check if jeffrey (temporary user) has the appropriate privileges.
        # The RELOAD on a global level was granted.
        server.exec_stmt("GRANT RELOAD ON *.* TO 'jeffrey'@'%%'")
        server.exec_stmt("FLUSH PRIVILEGES")
        self.assertTrue(
            new_server.has_privileges(MINIMUM_PRIVILEGES)
        )

        # Check if jeffrey (temporary user) has the appropriate privileges.
        # Revoke privilegs from temporary user.
        # There is no ALL on a global level.
        server.exec_stmt("REVOKE ALL PRIVILEGES, GRANT OPTION FROM "
                         "'jeffrey'@'%%'"
        )
        server.exec_stmt("GRANT ALL ON fabric.* TO 'jeffrey'@'%%'")
        server.exec_stmt("FLUSH PRIVILEGES")
        self.assertFalse(
            new_server.has_privileges(MINIMUM_PRIVILEGES)
        )

        # Check if jeffrey (temporary user) has the appropriate privileges.
        # The ALL on a global level was granted.
        server.exec_stmt("GRANT ALL ON *.* TO 'jeffrey'@'%%'")
        server.exec_stmt("FLUSH PRIVILEGES")
        self.assertTrue(
            new_server.has_privileges(MINIMUM_PRIVILEGES)
        )

        # Drop temporary user.
        server.exec_stmt("DROP USER 'jeffrey'@'%%'")
        server.set_session_binlog(True)
        server.disconnect()
        new_server.disconnect()
        ConnectionPool().purge_connections(_uuid.UUID(uuid))

class TestConnectionPool(unittest.TestCase):
    """Unit test for testing Connection Pool.
    """
    def setUp(self):
        """Configure the existing environment
        """
        pass

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_connection_pool(self):
        """Test connection pool.
        """
        # Configuration
        uuid = MySQLServer.discover_uuid(OPTIONS["address"])
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
    """Unit test for testing Group.
    """
    def setUp(self):
        """Configure the existing environment
        """
        pass

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_properties(self):
        """Test group's properties.
        """
        group_1 = Group("mysql.com")
        Group.add(group_1)
        fetched_group_1 = Group.fetch(group_1.group_id)
        self.assertEqual(group_1.group_id, "mysql.com")
        self.assertEqual(fetched_group_1.group_id, "mysql.com")
        self.assertEqual(fetched_group_1.master_defined, None)

        group_2 = Group("oracle.com", "First description.")
        Group.add(group_2)
        fetched_group_2 = Group.fetch(group_2.group_id)
        self.assertEqual(group_2.group_id, "oracle.com")
        self.assertEqual(fetched_group_2.group_id, "oracle.com")

        group_1.description = "New description."
        fetched_group_1 = Group.fetch(group_1.group_id)
        self.assertEqual(group_1.description, "New description.")
        self.assertEqual(fetched_group_1.description, "New description.")

        group_1.description = None
        fetched_group_1 = Group.fetch(group_1.group_id)
        self.assertEqual(group_1.description, None)
        self.assertEqual(fetched_group_1.description, None)

        group_1.status = Group.INACTIVE
        fetched_group_1 = Group.fetch(group_1.group_id)
        self.assertEqual(group_1.status, Group.INACTIVE)
        self.assertEqual(fetched_group_1.status, Group.INACTIVE)

        self.assertEqual(group_1, fetched_group_1)
        self.assertEqual(group_2, fetched_group_2)
        self.assertNotEqual(group_1, group_2)

        set_of_groups = set()
        set_of_groups.add(group_1)
        set_of_groups.add(group_2)
        set_of_groups.add(fetched_group_1)
        set_of_groups.add(fetched_group_2)
        self.assertEqual(len(set_of_groups), 2)

    def test_managment(self):
        """Test adding server to a group.
        """
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer(**options_1)
        MySQLServer.add(server_1)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer(**options_2)
        MySQLServer.add(server_2)
        group_1 = Group("oracle.com", "First description.")
        Group.add(group_1)

        # Add servers to a group
        group_1.add_server(server_1)
        group_1.add_server(server_2)
        self.assertRaises(AssertionError, group_1.add_server, server_1)
        self.assertEqual(len(group_1.servers()), 2)

        # Remove servers to a group
        group_1.remove_server(server_1)
        group_1.remove_server(server_2)
        self.assertRaises(AssertionError, group_1.remove_server, server_1)
        self.assertEqual(len(group_1.servers()), 0)

if __name__ == "__main__":
    unittest.main()

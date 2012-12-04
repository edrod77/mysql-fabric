"""Unit tests for the module "server.py".
"""

import unittest
import uuid as _uuid

import mysql.hub.errors as _errors
import mysql.hub.server_utils as _server_utils
import tests.utils as _test_utils

from mysql.hub.server import Group, Server, MySQLServer
from mysql.hub.persistence import MySQLPersister


class ConcreteServer(Server):
    def __init__(self, uuid, uri=None):
        super(ConcreteServer, self).__init__(uuid, uri)

    def _do_connection(*args, **kwargs):
        return "Connection ", args, kwargs


class TestServer(unittest.TestCase):

    __metaclass__ = _test_utils.SkipTests

    def test_properties(self):
        set_of_servers = set()
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = Server(**options_1)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = Server(**options_2)

        self.assertEqual(server_1, server_1)
        self.assertNotEqual(server_1, server_2)

        set_of_servers.add(server_1)
        set_of_servers.add(server_2)
        self.assertEqual(len(set_of_servers), 2)
        set_of_servers.add(server_1)
        self.assertEqual(len(set_of_servers), 2)

    def test_connection(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = Server(**options_1)
        server_2 = ConcreteServer(**options_1)

        # Server does not implement do_connection.
        self.assertRaises(NotImplementedError, server_1.connection)

        # Create and release a connection.
        connection_1 = server_2.connection()
        server_2.release_connection(connection_1)
        self.assertEqual(server_2.available_connections, 1)

        # Reuse a previously released connection.
        connection_2 = server_2.connection()
        self.assertEqual(server_2.available_connections, 0)
        self.assertEqual(id(connection_1), id(connection_2))
        server_2.release_connection(connection_2)

        # Try to purge connections but our test returns a string
        # instead of a real connection.
        self.assertRaises(AttributeError, server_2.purge_connections)

    def test_utilities(self):
        # Test a function that gets host and port and returns
        # host:port
        uri = _server_utils.combine_host_port(None, None, 3306)
        self.assertEqual(uri, "unknown host:3306")

        uri = _server_utils.combine_host_port("", None, 3306)
        self.assertEqual(uri, "unknown host:3306")

        uri = _server_utils.combine_host_port(None, "", 3306)
        self.assertEqual(uri, "unknown host:3306")

        uri = _server_utils.combine_host_port("host", "port", 3306)
        self.assertEqual(uri, "host:port")

        uri = _server_utils.combine_host_port("host", 1500, 3306)
        self.assertEqual(uri, "host:1500")

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


class TestGroup(unittest.TestCase):

    __metaclass__ = _test_utils.SkipTests

    def setUp(self):
        self.persister = MySQLPersister("localhost:13000", "root", "")
        Group.create(self.persister)

    def tearDown(self):
        Group.drop(self.persister)

    def test_properties(self):
        set_of_groups = set()
        group_1 = Group("mysql.com", "First description.")
        group_2 = Group("oracle.com", "First description.")
        self.assertEqual(group_1.group_id, "mysql.com")
        group_1.set_description(self.persister, "New description.")
        self.assertEqual(group_1.get_description(), "New description.")
        self.assertEqual(group_1, group_1)
        self.assertFalse(group_1 == group_2)

        set_of_groups.add(group_1)
        set_of_groups.add(group_2)
        self.assertEqual(len(set_of_groups), 2)
        set_of_groups.add(group_1)
        self.assertEqual(len(set_of_groups), 2)

    def test_managment(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = Server(**options_1)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = Server(**options_2)
        group_1 = Group("oracle.com", "First description.")

        # Add servers to a group
        group_1.add_server(self.persister, server_1)
        group_1.add_server(self.persister, server_2)
        self.assertRaises(_errors.DatabaseError, group_1.add_server,
                          self.persister, server_1)
        self.assertEqual(len(group_1.servers(self.persister)), 2)

        # Remove servers to a group
        group_1.remove_server(self.persister, server_1)
        group_1.remove_server(self.persister, server_2)
        group_1.remove_server(self.persister, server_1)
        self.assertEqual(len(group_1.servers(self.persister)), 0)


if __name__ == "__main__":
    unittest.main()

import unittest
import sqlite3
import uuid as _uuid

import mysql.hub.errors as _errors
import tests.utils as _utils

from mysql.hub.server import *

class SQLiteServer(Server):
    def __init__(self, uuid, uri):
        super(SQLiteServer, self).__init__(uuid, uri)
        self._do_connection()

    def _do_connection(self):
        self.__cnx = sqlite3.connect("/tmp/fabric.db")
        self.__cnx.execute("ATTACH DATABASE '/tmp/fabric.db' AS fabric")

    def commit(self):
       self.__cnx.commit()

    def rollback(self):
        self.__cnx.rollback()

    def exec_query(self, query_str, options=None):
        """Execute a query for the client and return a result set or a
        cursor.

        This is the singular method to execute queries. It should be the only
        method used as it contains critical error code to catch the issue
        with mysql.connector throwing an error on an empty result set.

        Note: will handle exception and print error if query fails

        Note: if fetchall is False, the method returns the cursor instance

        :param query_str: The query to execute
        :param options: Options to control behavior:

        - params - Parameters for query.
        - columns - Add column headings as first row (default is False).
        - fetch - Execute the fetch as part of the operation and use a
                  buffered cursor (default is True)
        - raw - If True, use a buffered raw cursor (default is True)

        It returns a result set or a cursor.
        """
        if self.__cnx is None:
            raise _errors.DatabaseError("Connection is invalid.")

        options = options if options is not None else {}
        params = options.get('params', ())
        columns = options.get('columns', False)
        fetch = options.get('fetch', True)
        raw = options.get('raw', True)

        results = ()
        cur = self.__cnx.cursor()

        try:
            cur.execute(query_str, params)
        except Exception as error:
            print error

        if fetch or columns:
            try:
                results = cur.fetchall()
            except Exception as error:
                pass
            if columns:
                col_headings = cur.column_names
                col_names = []
                for col in col_headings:
                    col_names.append(col)
                results = col_names, results
            cur.close()
            self.__cnx.commit()
            return results
        else:
            return cur

class TestGroup(unittest.TestCase):
    def setUp(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        self.__persistence_server = SQLiteServer(**options_1)
        Group.create(self.__persistence_server)
        MySQLServer.create(self.__persistence_server)

    def tearDown(self):
        Group.drop(self.__persistence_server)
        MySQLServer.drop(self.__persistence_server)

    def test_group_constructor(self):
        group_1 = Group.add(self.__persistence_server, "mysql.com",
                        "First description.")
        group_2 = Group.fetch(self.__persistence_server, "mysql.com")
        self.assertEqual(group_1, group_2)

    def test_add_server(self):
        group_1 = Group.add(self.__persistence_server, "mysql.com",
                "First description.")
        options_1 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer.add(**options_2)
        group_1.add_server(server_1)
        group_1.add_server(server_2)

        servers = group_1.servers

        self.assertTrue(group_1.check_server_membership(server_1.uuid))
        self.assertTrue(group_1.check_server_membership(server_2.uuid))

    def test_remove_server(self):
        group_1 = Group.add(self.__persistence_server, "mysql.com",
                "First description.")
        options_1 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer.add(**options_2)
        group_1.add_server(server_1)
        group_1.add_server(server_2)
        servers = group_1.servers

        self.assertTrue(group_1.check_server_membership(server_1.uuid))
        self.assertTrue(group_1.check_server_membership(server_2.uuid))

        group_1.remove_server(server_1)
        group_1.remove_server(server_2)

        self.assertFalse(group_1.check_server_membership(server_1.uuid))
        self.assertFalse(group_1.check_server_membership(server_2.uuid))

    def test_update_description(self):
         group_1 = Group(self.__persistence_server, "mysql.com",
                        "First description.")
         group_1.description = "Second Description."
         self.assertEqual(group_1.description, "Second Description.")

    def test_remove_group(self):
         group_1 = Group.add(self.__persistence_server, "mysql.com",
                        "First description.")
         group_1.remove()
         self.assertEqual(Group.fetch
                          (self.__persistence_server, "mysql.com"), None)

    def test_MySQLServer_create(self):
        options_1 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer.add(**options_2)
        MySQLServer.fetch(self.__persistence_server,
                                  options_1["uuid"])
        MySQLServer.fetch(self.__persistence_server,
                                  options_2["uuid"])

#    def test_MySQLServer_read_only(self):
#        options_1 = {
#            "persistence_server" : self.__persistence_server,
#            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
#            "uri"  : "server_1.mysql.com:3060",
#        }
#        server_1 = MySQLServer.add(**options_1)
#        options_2 = {
#            "persistence_server" : self.__persistence_server,
#            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
#            "uri"  : "server_2.mysql.com:3060",
#        }
#        server_2 = MySQLServer.add(**options_2)
#        MySQLServer.fetch(self.__persistence_server,
#                                  options_1["uuid"])
#        MySQLServer.fetch(self.__persistence_server,
#                                  options_2["uuid"])
#        server_1.read_only = True
#        server_2.read_only = True
#
#        self.assertTrue(server_1.read_only)
#        self.assertTrue(server_2.read_only)
#
#        server_1.read_only = False
#        server_2.read_only = False
#
#        self.assertFalse(server_1.read_only)
#        self.assertFalse(server_2.read_only)

    def test_MySQLServer_User(self):
        options_1 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer.add(**options_1)

        options_2 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer.add(**options_2)

        server_1.user = "u1"
        server_2.user = "u2"

        server_1_ = MySQLServer.fetch(self.__persistence_server,
                                              options_1["uuid"])
        server_2_ = MySQLServer.fetch(self.__persistence_server,
                                              options_2["uuid"])

        self.assertEqual(server_1.user, "u1")
        self.assertEqual(server_2.user, "u2")

        self.assertEqual(server_1, server_1_)
        self.assertEqual(server_2, server_2_)


    def test_MySQLServer_Password(self):
        options_1 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer.add(**options_2)

        server_1.passwd = "p1"
        server_2.passwd = "p2"

        server_1_ = MySQLServer.fetch(self.__persistence_server,
                                              options_1["uuid"])
        server_2_ = MySQLServer.fetch(self.__persistence_server,
                                              options_2["uuid"])

        self.assertEqual(server_1.passwd, "p1")
        self.assertEqual(server_2.passwd, "p2")

        self.assertEqual(server_1, server_1_)
        self.assertEqual(server_2, server_2_)

    def test_MySQLServer_Remove(self):
        options_1 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "persistence_server" : self.__persistence_server,
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        server_2 = MySQLServer.add(**options_2)

        MySQLServer.fetch(self.__persistence_server,
                                  options_1["uuid"])
        MySQLServer.fetch(self.__persistence_server,
                                  options_2["uuid"])
        server_1.remove()
        server_2.remove()

        server_1_ = MySQLServer.fetch(self.__persistence_server,
                                              options_1["uuid"])
        server_2_ = MySQLServer.fetch(self.__persistence_server,
                                              options_2["uuid"])

        self.assertEqual(server_1_, None)
        self.assertEqual(server_2_, None)
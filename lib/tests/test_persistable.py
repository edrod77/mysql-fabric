import unittest
import sqlite3
import uuid as _uuid

import mysql.hub.errors as _errors
import tests.utils as _test_utils

from mysql.hub.server import *

class TestGroup(unittest.TestCase):
    def setUp(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        self.__persistence_server = _test_utils.PersistenceServer(**options_1)
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

     # TODO: We need a Fake or Real MySQL Server to run this.
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

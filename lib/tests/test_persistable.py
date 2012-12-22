import unittest
import uuid as _uuid

import mysql.hub.persistence as _persistence
import mysql.hub.errors as _errors

from mysql.hub.server import MySQLServer, Group

class TestGroup(unittest.TestCase):

    def setUp(self):
        from __main__ import options
        _persistence.init(host=options.host, port=options.port,
                          user=options.user, password=options.password,
                          database=options.database)
        _persistence.init_thread()

    def tearDown(self):
        _persistence.deinit_thread()
        _persistence.deinit()

    def test_group_constructor(self):
        group_1 = Group.add("mysql.com", "First description.")
        group_2 = Group.fetch("mysql.com") 
        self.assertEqual(group_1, group_2)
        self.assertRaises(_errors.DatabaseError,
                          Group.add, "mysql.com", "Second description.")
        group_1.remove()
        group_2.remove()

    def test_add_server(self):
        group_1 = Group.add("mysql.com", "First description.")
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_2 = MySQLServer.add(**options_2)
        group_1.add_server(server_1)
        group_1.add_server(server_2)

        self.assertTrue(group_1.contains_server(server_1.uuid))
        self.assertTrue(group_1.contains_server(server_2.uuid))

    def test_remove_server(self):
        group_1 = Group.add("mysql.com", "First description.")
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_2 = MySQLServer.add(**options_2)
        group_1.add_server(server_1)
        group_1.add_server(server_2)

        self.assertTrue(group_1.contains_server(server_1.uuid))
        self.assertTrue(group_1.contains_server(server_2.uuid))

        group_1.remove_server(server_1)
        group_1.remove_server(server_2)

        self.assertFalse(group_1.contains_server(server_1.uuid))
        self.assertFalse(group_1.contains_server(server_2.uuid))

    def test_update_description(self):
        group_1 = Group("mysql.com", "First description.")
        group_1.description = "Second Description."
        self.assertEqual(group_1.description, "Second Description.")

    def test_remove_group(self):
        group_1 = Group.add("mysql.com", "First description.")
        group_1.remove()
        self.assertEqual(Group.fetch("mysql.com"), None)

    def test_MySQLServer_create(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_1 = MySQLServer.add(**options_1)

        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_2 = MySQLServer.add(**options_2)

        fetched_server_1 = MySQLServer.fetch(options_1["uuid"])
        fetched_server_2 = MySQLServer.fetch(options_2["uuid"])

        self.assertEqual(server_1, fetched_server_1)
        self.assertEqual(server_2, fetched_server_2)

    def test_MySQLServer_User(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_1 = MySQLServer.add(**options_1)

        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_2 = MySQLServer.add(**options_2)

        server_1.user = "u1"
        server_2.user = "u2"

        server_1_ = MySQLServer.fetch(options_1["uuid"])
        server_2_ = MySQLServer.fetch(options_2["uuid"])

        self.assertEqual(server_1.user, "u1")
        self.assertEqual(server_2.user, "u2")

        self.assertEqual(server_1, server_1_)
        self.assertEqual(server_2, server_2_)


    def test_MySQLServer_Password(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_2 = MySQLServer.add(**options_2)

        server_1.passwd = "p1"
        server_2.passwd = "p2"

        server_1_ = MySQLServer.fetch(options_1["uuid"])
        server_2_ = MySQLServer.fetch(options_2["uuid"])

        self.assertEqual(server_1.passwd, "p1")
        self.assertEqual(server_2.passwd, "p2")

        self.assertEqual(server_1, server_1_)
        self.assertEqual(server_2, server_2_)

    def test_MySQLServer_Remove(self):
        options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_1 = MySQLServer.add(**options_1)
        options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
            "user" : "user",
            "passwd" : "passwd"
        }
        server_2 = MySQLServer.add(**options_2)

        MySQLServer.fetch(options_1["uuid"])
        MySQLServer.fetch(options_2["uuid"])
        server_1.remove()
        server_2.remove()

        server_1_ = MySQLServer.fetch(options_1["uuid"])
        server_2_ = MySQLServer.fetch(options_2["uuid"])

        self.assertEqual(server_1_, None)
        self.assertEqual(server_2_, None)

if __name__ == "__main__":
    unittest.main()

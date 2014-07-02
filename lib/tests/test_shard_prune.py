#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
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

import unittest
import uuid as _uuid
import tests.utils

from tests.utils import MySQLInstances
from mysql.fabric import executor as _executor
from mysql.fabric.server import (
    Group,
    MySQLServer,
)

class TestShardingPrune(tests.utils.TestCase):

    def setUp(self):
        """Creates the following topology for testing,

        GROUPID1 - localhost:13001, localhost:13002 - Global Group
        GROUPID2 - localhost:13003, localhost:13004 - shard 1
        GROUPID3 - localhost:13005, localhost:13006 - shard 2
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd
        }

        uuid_server1 = MySQLServer.discover_uuid(self.__options_1["address"])
        self.__options_1["uuid"] = _uuid.UUID(uuid_server1)
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__server_1)

        self.__group_1 = Group("GROUPID1", "First description.")
        Group.add(self.__group_1)
        self.__group_1.add_server(self.__server_1)
        tests.utils.configure_decoupled_master(self.__group_1, self.__server_1)

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(1),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd
        }

        uuid_server2 = MySQLServer.discover_uuid(self.__options_2["address"])
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)
        self.__server_2.connect()
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_2.exec_stmt("CREATE DATABASE db1")
        self.__server_2.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 601):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))


        self.__group_2 = Group("GROUPID2", "Second description.")
        Group.add(self.__group_2)
        self.__group_2.add_server(self.__server_2)
        tests.utils.configure_decoupled_master(self.__group_2, self.__server_2)

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd
        }

        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)
        self.__server_3.connect()
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_3.exec_stmt("CREATE DATABASE db1")
        self.__server_3.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 601):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add( self.__group_3)
        self.__group_3.add_server(self.__server_3)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{bb45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(3),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd
        }

        uuid_server4 = MySQLServer.discover_uuid(self.__options_4["address"])
        self.__options_4["uuid"] = _uuid.UUID(uuid_server4)
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)
        self.__server_4.connect()
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_4.exec_stmt("CREATE DATABASE db1")
        self.__server_4.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 601):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_4 = Group("GROUPID4", "Fourth description.")
        Group.add( self.__group_4)
        self.__group_4.add_server(self.__server_4)
        tests.utils.configure_decoupled_master(self.__group_4, self.__server_4)

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(4),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd
        }

        uuid_server5 = MySQLServer.discover_uuid(self.__options_5["address"])
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)
        self.__server_5.connect()
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_5.exec_stmt("CREATE DATABASE db1")
        self.__server_5.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 601):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_5 = Group("GROUPID5", "Fifth description.")
        Group.add( self.__group_5)
        self.__group_5.add_server(self.__server_5)
        tests.utils.configure_decoupled_master(self.__group_5, self.__server_5)

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{cc45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(5),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd
        }

        uuid_server6 = MySQLServer.discover_uuid(self.__options_6["address"])
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)
        self.__server_6.connect()
        self.__server_6.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_6.exec_stmt("CREATE DATABASE db1")
        self.__server_6.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 601):
            self.__server_6.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_6 = Group("GROUPID6", "Sixth description.")
        Group.add( self.__group_6)
        self.__group_6.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_6, self.__server_6)

        status = self.proxy.sharding.create_definition("RANGE", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(
            1,
            "GROUPID2/1,GROUPID3/101,GROUPID4/201,"
            "GROUPID5/301,GROUPID6/401",
            "ENABLED"
        )
        self.check_xmlrpc_command_result(status)

    def test_prune_shard(self):
        status = self.proxy.sharding.prune_shard("db1.t1")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_servers("db1.t1", 1,  "LOCAL")
        row = self.check_xmlrpc_simple(status, {})
        shard_server = MySQLServer.fetch(row['server_uuid'])
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 1)

        status = self.proxy.sharding.lookup_servers("db1.t1", 101,  "LOCAL")
        row = self.check_xmlrpc_simple(status, {})
        shard_server = MySQLServer.fetch(row['server_uuid'])
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 200)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 101)

        status = self.proxy.sharding.lookup_servers("db1.t1", 202,  "LOCAL")
        row = self.check_xmlrpc_simple(status, {})
        shard_server = MySQLServer.fetch(row['server_uuid'])
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 300)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 201)

        status = self.proxy.sharding.lookup_servers("db1.t1", 303,  "LOCAL")
        row = self.check_xmlrpc_simple(status, {})
        shard_server = MySQLServer.fetch(row['server_uuid'])
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 400)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 301)

        status = self.proxy.sharding.lookup_servers("db1.t1", 404,  "LOCAL")
        row = self.check_xmlrpc_simple(status, {})
        shard_server = MySQLServer.fetch(row['server_uuid'])
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 200)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 600)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 401)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

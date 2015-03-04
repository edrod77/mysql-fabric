#
# Copyright (c) 2013,2015, Oracle and/or its affiliates. All rights reserved.
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

from tests.utils import (
    MySQLInstances,
    fetch_test_server,
)
from mysql.fabric import executor as _executor
from mysql.fabric.server import (
    Group,
    MySQLServer,
)

class TestShardingServices(tests.utils.TestCase):
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
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
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server2 = MySQLServer.discover_uuid(self.__options_2["address"])
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)

        self.__group_2 = Group("GROUPID2", "Second description.")
        Group.add(self.__group_2)
        self.__group_2.add_server(self.__server_2)
        tests.utils.configure_decoupled_master(self.__group_2, self.__server_2)

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)

        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add( self.__group_3)
        self.__group_3.add_server(self.__server_3)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{bb45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(3),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server4 = MySQLServer.discover_uuid(self.__options_4["address"])
        self.__options_4["uuid"] = _uuid.UUID(uuid_server4)
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)

        self.__group_4 = Group("GROUPID4", "Fourth description.")
        Group.add( self.__group_4)
        self.__group_4.add_server(self.__server_4)
        tests.utils.configure_decoupled_master(self.__group_4, self.__server_4)

        self.__server_4.connect()
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_4.exec_stmt("CREATE DATABASE db2")
        self.__server_4.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID2 INT, name VARCHAR(30))")
        for i in range(1, 201):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))


        self.__options_5 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(4),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server5 = MySQLServer.discover_uuid(self.__options_5["address"])
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)

        self.__group_5 = Group("GROUPID5", "Fifth description.")
        Group.add( self.__group_5)
        self.__group_5.add_server(self.__server_5)
        tests.utils.configure_decoupled_master(self.__group_5, self.__server_5)

        self.__server_5.connect()
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_5.exec_stmt("CREATE DATABASE db2")
        self.__server_5.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID2 INT, name VARCHAR(30))")
        for i in range(1, 201):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{cc45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(5),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server6 = MySQLServer.discover_uuid(self.__options_6["address"])
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)

        self.__group_6 = Group("GROUPID6", "Sixth description.")
        Group.add( self.__group_6)
        self.__group_6.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_6, self.__server_6)

        status = self.proxy.sharding.create_definition("HASH", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.create_definition("RANGE", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=2)

        status = self.proxy.sharding.create_definition("HASH", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=3)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID1")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_table(2, "db2.t2", "userID2")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_table(3, "db3.t3", "userID3")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(1, "GROUPID2,GROUPID3",
            "ENABLED")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(2, "GROUPID4/0,GROUPID5/101",
            "ENABLED")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(3, "GROUPID6", "ENABLED")
        self.check_xmlrpc_command_result(status)

    def test_list_shard_defns(self):
        expected_shard_mapping_list1 = tests.utils.make_mapping_result([
            [1, "HASH", "GROUPID1"],
            [2, "RANGE", "GROUPID1"],
            [3, "HASH", "GROUPID1"]
        ])

        status = self.proxy.sharding.list_definitions()
        self.check_xmlrpc_result(status, expected_shard_mapping_list1)

    def test_list_shard_mappings(self):
        expected_hash = tests.utils.make_shard_mapping_list_result([
            [1, 'HASH', "db1.t1", "GROUPID1", "userID1"],
            [3, 'HASH', "db3.t3", "GROUPID1", "userID3" ],
        ])
        status = self.proxy.sharding.list_tables("HASH")
        self.check_xmlrpc_result(status, expected_hash)

        expected_range = tests.utils.make_shard_mapping_list_result([
            [2, "RANGE", "db2.t2", "GROUPID1", "userID2"],
        ])
        status = self.proxy.sharding.list_tables("RANGE")
        self.check_xmlrpc_result(status, expected_range)

    def test_shard_prune(self):
        status = self.proxy.sharding.prune_shard("db2.t2")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_servers("db2.t2", 1,  "LOCAL")
        info = self.check_xmlrpc_simple(status, {})
        shard_uuid = info['server_uuid']
        shard_server = fetch_test_server(shard_uuid)
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db2.t2",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID2) FROM db2.t2",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID2) FROM db2.t2",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 1)

        status = self.proxy.sharding.lookup_servers("db2.t2", 101,  "LOCAL")
        info = self.check_xmlrpc_simple(status, {})
        shard_uuid = info['server_uuid']
        shard_server = fetch_test_server(shard_uuid)
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db2.t2",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID2) FROM db2.t2",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 200)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID2) FROM db2.t2",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 101)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

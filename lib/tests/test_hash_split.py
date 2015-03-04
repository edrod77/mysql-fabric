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
import tests.utils

from tests.utils import (
    MySQLInstances,
    ShardingUtils,
    fetch_test_server,
)
from mysql.fabric import executor as _executor
from mysql.fabric import errors as _errors
from mysql.fabric.server import MySQLServer
from mysql.fabric.sharding import HashShardingSpecification

class TestShardSplit(tests.utils.TestCase):
    def setUp(self):
        """Creates the following topology for testing,

        GROUPID1 - localhost:13001, localhost:13002 - Global Group
        GROUPID2 - localhost:13003, localhost:13004 - shard 1
        GROUPID3 - localhost:13005, localhost:13006 - shard 2
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        status = self.proxy.group.create("GROUPID1", "First description.")
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.add(
            "GROUPID1", MySQLInstances().get_address(0)
        )
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.add(
            "GROUPID1", MySQLInstances().get_address(1)
        )
        self.check_xmlrpc_command_result(status)

        status = self.proxy.group.create("GROUPID2", "Second description.")
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.add(
            "GROUPID2", MySQLInstances().get_address(2)
        )
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.add(
            "GROUPID2", MySQLInstances().get_address(3)
        )
        self.check_xmlrpc_command_result(status)

        status = self.proxy.group.create("GROUPID3", "Third description.")
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.add(
            "GROUPID3", MySQLInstances().get_address(4)
        )
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.add(
            "GROUPID3", MySQLInstances().get_address(5)
        )
        self.check_xmlrpc_command_result(status)

        status = self.proxy.group.promote("GROUPID1")
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.promote("GROUPID2")
        self.check_xmlrpc_command_result(status)
        status = self.proxy.group.promote("GROUPID3")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.create_definition("HASH", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(1, "GROUPID2", "ENABLED")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        for info in self.check_xmlrpc_iter(status):
            shard_uuid = info['server_uuid']
            shard_server = fetch_test_server(shard_uuid)
            shard_server.connect()
        shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")
        shard_server.exec_stmt("CREATE DATABASE db1")
        shard_server.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")

        for i in range(1,  100):
            shard_server.exec_stmt("INSERT INTO db1.t1 "
                                      "VALUES(%s, 'TEST %s')" % (i, i))

    def test_MD5_HEX(self):
        """Ensure the STRICT_ALL_TABLES mode catches large inserts into columns.
        """
        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        for info in self.check_xmlrpc_iter(status):
            shard_uuid = info['server_uuid']
            shard_server = fetch_test_server(shard_uuid)
            shard_server.connect()
        shard_server.exec_stmt("CREATE DATABASE SAMPDB")
        shard_server.exec_stmt("USE SAMPDB")
        shard_server.exec_stmt("SET SESSION sql_mode = STRICT_ALL_TABLES")
        shard_server.exec_stmt("CREATE TABLE sample(empno int)")
        shard_server.exec_stmt("CREATE TABLE sample1(s VARBINARY(16))")
        shard_server.exec_stmt("INSERT INTO sample VALUES(1)")
        shard_server.exec_stmt("INSERT INTO sample VALUES(2)")
        shard_server.exec_stmt("SELECT HEX(MD5(MAX(empno))) "
                                "INTO @a FROM sample")
        shard_server.exec_stmt("SELECT MD5(MAX(empno)) "
                                "INTO @b FROM sample")

        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
            "INSERT INTO sample1 VALUES (UNHEX(@a))"
        )

        shard_server.exec_stmt("INSERT INTO sample1 VALUES (UNHEX(@b))")

        shard_server.exec_stmt("DROP DATABASE SAMPDB")

    def test_shard_split(self):
        split_cnt_1 = 0
        split_cnt_2 = 0
        shard_server_1 = None
        shard_server_2 = None
        expected_address_list_1 = \
            [MySQLInstances().get_address(2), MySQLInstances().get_address(3)]
        expected_address_list_2 = \
            [MySQLInstances().get_address(4), MySQLInstances().get_address(5)]
        status = self.proxy.sharding.split_shard("1", "GROUPID3")
        self.check_xmlrpc_command_result(status)

        for i in range(1,  100):
            status = self.proxy.sharding.lookup_servers("db1.t1", i, "LOCAL")
            obtained_uuid_list = [
                info['server_uuid']
                for info in self.check_xmlrpc_iter(status)
            ]
            obtained_address_list = [
                info['address']
                for info in self.check_xmlrpc_iter(status)
            ]
            try:
                self.assertEqual(
                    set(expected_address_list_1), set(obtained_address_list)
                )
                split_cnt_1 = split_cnt_1 + 1
                if shard_server_1 is None:
                    shard_server_1 = fetch_test_server(obtained_uuid_list[0])
            except AssertionError:
                self.assertEqual(
                    set(expected_address_list_2), set(obtained_address_list)
                )
                split_cnt_2 = split_cnt_2 + 1
                if shard_server_2 is None:
                    shard_server_2 = fetch_test_server(obtained_uuid_list[0])

        #Ensure that both the splits have been utilized.
        self.assertTrue(split_cnt_1 > 0)
        self.assertTrue(split_cnt_2 > 0)

        shard_server_1.connect()
        shard_server_2.connect()

        row_cnt_shard_1 = shard_server_1.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )

        row_cnt_shard_2 = shard_server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )

        #Ensure that the split has happened, the number of values in
        #each shard should be less than the original.
        self.assertTrue(int(row_cnt_shard_1[0][0]) < 100)
        self.assertTrue(int(row_cnt_shard_2[0][0]) < 100)

        #Ensure tha two new shard_ids have been generated.
        hash_sharding_specifications = HashShardingSpecification.list(1)
        self.assertTrue(ShardingUtils.compare_hash_specifications(
            hash_sharding_specifications[1],
            HashShardingSpecification.fetch(2)))
        self.assertTrue(ShardingUtils.compare_hash_specifications(
            hash_sharding_specifications[0],
            HashShardingSpecification.fetch(3)))

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

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
import tests.utils

from tests.utils import (
    MySQLInstances,
    ShardingUtils,
)
from mysql.fabric import executor as _executor
from mysql.fabric.server import MySQLServer
from mysql.fabric.sharding import HashShardingSpecification

class TestShardSplit(tests.utils.TestCase):

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

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
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
        shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")
        shard_server.exec_stmt("CREATE DATABASE db1")
        shard_server.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")

        for i in range(1,  100):
            shard_server.exec_stmt("INSERT INTO db1.t1 "
                                      "VALUES(%s, 'TEST %s')" % (i, i))

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
                "%s:%s" % (info['host'], info['port']) 
                for info in self.check_xmlrpc_iter(status)
            ]
            try:
                self.assertEqual(
                    set(expected_address_list_1), set(obtained_address_list)
                )
                split_cnt_1 = split_cnt_1 + 1
                if shard_server_1 is None:
                    shard_server_1 = MySQLServer.fetch(obtained_uuid_list[0])
            except AssertionError:
                self.assertEqual(
                    set(expected_address_list_2), set(obtained_address_list)
                )
                split_cnt_2 = split_cnt_2 + 1
                if shard_server_2 is None:
                    shard_server_2 = MySQLServer.fetch(obtained_uuid_list[0])

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
        self.proxy.sharding.enable_shard("2")

        status = self.proxy.sharding.lookup_servers("1", 500,  "GLOBAL")
        for info in self.check_xmlrpc_iter(status):
            shard_uuid = info['server_uuid']
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        for info in self.check_xmlrpc_iter(status):
            shard_uuid = info['server_uuid']
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")
            shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")

        status = self.proxy.sharding.lookup_servers("db1.t1", 800,  "LOCAL")
        for info in self.check_xmlrpc_iter(status):
            shard_uuid = info['server_uuid']
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")
            shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")

        status = self.proxy.sharding.disable_shard("2")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.disable_shard("3")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.remove_shard("2")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.remove_shard("3")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.remove_table("db1.t1")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.remove_definition("1")
        self.check_xmlrpc_command_result(status)

        self.proxy.group.demote("GROUPID1")
        self.proxy.group.demote("GROUPID2")
        self.proxy.group.demote("GROUPID3")
        for group_id in ("GROUPID1", "GROUPID2", "GROUPID3"):
            status = self.proxy.group.lookup_servers(group_id)
            for info in self.check_xmlrpc_iter(status):
                shard_uuid = info['server_uuid']
                packet = self.proxy.group.remove(
                    group_id, shard_uuid
                )
                self.check_xmlrpc_command_result(packet)

            status = self.proxy.group.destroy(group_id)
            self.check_xmlrpc_command_result(status)

        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

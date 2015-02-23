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
    fetch_test_server,
)
from mysql.fabric import (
    executor as _executor,
    errors as _errors,
    group_replication as _group_replication,
    replication as _replication,
)
from mysql.fabric.server import MySQLServer

import mysql.fabric.protocols.xmlrpc as _xmlrpc

class TestShardingPrune(tests.utils.TestCase):
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
        status =  self.proxy.group.add(
            "GROUPID2", MySQLInstances().get_address(3),
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

        status = self.proxy.sharding.create_definition("RANGE", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(1, "GROUPID2/0",
                                               "ENABLED")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        for info in self.check_xmlrpc_iter(status):
            shard_uuid = info['server_uuid']
            self.shard_server = fetch_test_server(shard_uuid)
            self.shard_server.connect()
        self.shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.shard_server.exec_stmt("CREATE DATABASE db1")
        self.shard_server.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(101, 'TEST 1')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(102, 'TEST 2')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(103, 'TEST 3')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1001, 'TEST 4')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1002, 'TEST 5')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1003, 'TEST 6')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1004, 'TEST 7')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1005, 'TEST 8')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1006, 'TEST 9')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1007, 'TEST 10')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1008, 'TEST 11')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1009, 'TEST 12')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1010, 'TEST 13')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1011, 'TEST 14')")
        self.shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(1012, 'TEST 15')")

    def test_sync_readonly_servers(self):
        status = self.proxy.group.lookup_servers("GROUPID3")
        for info in self.check_xmlrpc_iter(status):
            if info['status'] == MySQLServer.SECONDARY:
                slave_uuid = info['server_uuid']
                slave_server = fetch_test_server(slave_uuid)
                slave_server.connect()
        _group_replication.setup_group_replication("GROUPID2", "GROUPID3")
        _replication.synchronize_with_read_only(
            slave_server, self.shard_server, 3, 5
        )
        _group_replication.stop_group_slave("GROUPID2", "GROUPID3", True)
        try:
            rows = self.shard_server.exec_stmt(
                                "SELECT NAME FROM db1.t1",
                                {"fetch" : True})
        except _errors.DatabaseError:
            raise Exception("Enable Shard failed to enable shard.")
        self.assertEqual(len(rows), 15)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

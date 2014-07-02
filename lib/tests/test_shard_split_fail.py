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

from mysql.fabric import executor as _executor
from mysql.fabric.server import MySQLServer
from tests.utils import MySQLInstances

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

        status =  self.proxy.group.add(
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

        status = self.proxy.sharding.create_definition("RANGE", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(1, "GROUPID2/0", "ENABLED")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        for row in self.check_xmlrpc_iter(status):
            if row['status'] == MySQLServer.PRIMARY:
                shard_server = MySQLServer.fetch(row['server_uuid'])
                shard_server.connect()
        shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")
        shard_server.exec_stmt("CREATE DATABASE db1")
        shard_server.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(101, 'TEST 1')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(102, 'TEST 2')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(103, 'TEST 3')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(701, 'TEST 4')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(702, 'TEST 5')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(703, 'TEST 6')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(704, 'TEST 7')")

    def test_shard_split_invalid_split_range_failed(self):
        status = self.proxy.sharding.split_shard("1", "GROUPID3", "ABCEDFGH")
        self.check_xmlrpc_command_result(status, has_error=True)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

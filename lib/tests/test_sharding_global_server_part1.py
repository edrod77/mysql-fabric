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

from time import sleep
from tests.utils import (
    MySQLInstances,
    fetch_test_server,
)
from mysql.fabric import (
    executor as _executor,
    errors as _errors,
)
from mysql.fabric.server import MySQLServer

class TestShardingGlobalServerPart1(tests.utils.TestCase):
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
        
        status = self.proxy.sharding.add_table(1, "db1.t1", "userID1")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(
            1, "GROUPID2/0,GROUPID3/1001", "ENABLED")
        self.check_xmlrpc_command_result(status)

    def test_shard_server_add_disabled(self):
        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        for row in self.check_xmlrpc_iter(status):
            if row['status'] == MySQLServer.PRIMARY:
                shard_server_1 = fetch_test_server(row['server_uuid'])
                shard_server_1.connect()

        self.proxy.sharding.disable_shard("1")
        self.proxy.sharding.remove_shard("1")

        sleep(3)

        status = self.proxy.sharding.lookup_servers("1", 500,  "GLOBAL")
        for row in self.check_xmlrpc_iter(status):
            if row['status'] == MySQLServer.PRIMARY:
                global_master = fetch_test_server(row['server_uuid'])
                global_master.connect()

        global_master.exec_stmt("DROP DATABASE IF EXISTS global_db")
        global_master.exec_stmt("CREATE DATABASE global_db")
        global_master.exec_stmt("CREATE TABLE global_db.global_table"
                                  "(userID INT, name VARCHAR(30))")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(101, 'TEST 1')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(202, 'TEST 2')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(303, 'TEST 3')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(404, 'TEST 4')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(505, 'TEST 5')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(606, 'TEST 6')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(707, 'TEST 7')")

        sleep(5)

        status = self.proxy.sharding.add_shard(1, "GROUPID2/0", "DISABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

        sleep(5)

        try:
            shard_server_1.exec_stmt(
                "SELECT NAME FROM global_db.global_table",
                {"fetch" : True}
            )
            raise Exception("Adding a disabled shard did not stop replication")
        except _errors.DatabaseError:
            #The table should not have been created.
            pass

    def test_shard_server_added_later(self):
        self.proxy.sharding.disable_shard("1")
        self.proxy.sharding.remove_shard("1")

        sleep(3)

        status = self.proxy.sharding.lookup_servers("1", 500,  "GLOBAL")
        for row in self.check_xmlrpc_iter(status):
            if row['status'] == MySQLServer.PRIMARY:
                global_master = fetch_test_server(row['server_uuid'])
                global_master.connect()

        global_master.exec_stmt("DROP DATABASE IF EXISTS global_db")
        global_master.exec_stmt("CREATE DATABASE global_db")
        global_master.exec_stmt("CREATE TABLE global_db.global_table"
                                  "(userID INT, name VARCHAR(30))")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(101, 'TEST 1')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(202, 'TEST 2')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(303, 'TEST 3')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(404, 'TEST 4')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(505, 'TEST 5')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(606, 'TEST 6')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(707, 'TEST 7')")

        sleep(5)

        status = self.proxy.sharding.add_shard(1, "GROUPID2/0", "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

        sleep(5)

        status = self.proxy.sharding.lookup_servers("db1.t1", 1500,  "LOCAL")

        for row in self.check_xmlrpc_iter(status):
            if row['status'] == MySQLServer.PRIMARY:
                shard_server = fetch_test_server(row['server_uuid'])
                shard_server.connect()
                try:
                    rows = shard_server.exec_stmt(
                        "SELECT NAME FROM global_db.global_table",
                        {"fetch" : True}
                    )
                except _errors.DatabaseError:
                    raise Exception("Enable Shard failed to enable shard.")
                self.assertEqual(len(rows), 7)
                self.assertEqual(rows[0][0], 'TEST 1')
                self.assertEqual(rows[1][0], 'TEST 2')
                self.assertEqual(rows[2][0], 'TEST 3')
                self.assertEqual(rows[3][0], 'TEST 4')
                self.assertEqual(rows[4][0], 'TEST 5')
                self.assertEqual(rows[5][0], 'TEST 6')
                self.assertEqual(rows[6][0], 'TEST 7')

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

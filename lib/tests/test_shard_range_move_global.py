#
# Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
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

"""Tests RANGE shard moves and also tests the impact of the move on the
global server configuration.
"""
import unittest
import uuid as _uuid
import time
import tests.utils

from mysql.fabric import (
    executor as _executor,
    errors as _errors,
)
from mysql.fabric.server import (
    Group,
    MySQLServer,
)
from tests.utils import MySQLInstances

class TestShardingPrune(unittest.TestCase):
    '''Contains unit tests for testing the shard move operation and for verifying
    that the global server configuration remains constant after the shard move
    configuration.
    '''

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        """Creates the topology for testing.
        """
        tests.utils.cleanup_environment()
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : "root"
        }

        uuid_server1 = MySQLServer.discover_uuid(**self.__options_1)
        self.__options_1["uuid"] = _uuid.UUID(uuid_server1)
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__server_1)
        self.__server_1.connect()

        self.__group_1 = Group("GROUPID1", "First description.")
        Group.add(self.__group_1)
        self.__group_1.add_server(self.__server_1)
        tests.utils.configure_decoupled_master(self.__group_1, self.__server_1)

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(1),
            "user" : "root"
        }

        uuid_server2 = MySQLServer.discover_uuid(**self.__options_2)
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)
        self.__server_2.connect()
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_2.exec_stmt("CREATE DATABASE db1")
        self.__server_2.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_2.exec_stmt("CREATE DATABASE db2")
        self.__server_2.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))

        self.__group_2 = Group("GROUPID2", "Second description.")
        Group.add(self.__group_2)
        self.__group_2.add_server(self.__server_2)
        tests.utils.configure_decoupled_master(self.__group_2, self.__server_2)

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : "root"
        }

        uuid_server3 = MySQLServer.discover_uuid(**self.__options_3)
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)
        self.__server_3.connect()
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_3.exec_stmt("CREATE DATABASE db1")
        self.__server_3.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_3.exec_stmt("CREATE DATABASE db2")
        self.__server_3.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))

        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add( self.__group_3)
        self.__group_3.add_server(self.__server_3)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{bb45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(3),
            "user" : "root"
        }

        uuid_server4 = MySQLServer.discover_uuid(**self.__options_4)
        self.__options_4["uuid"] = _uuid.UUID(uuid_server4)
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)
        self.__server_4.connect()
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_4.exec_stmt("CREATE DATABASE db1")
        self.__server_4.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_4.exec_stmt("CREATE DATABASE db2")
        self.__server_4.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))

        self.__group_4 = Group("GROUPID4", "Fourth description.")
        Group.add( self.__group_4)
        self.__group_4.add_server(self.__server_4)
        tests.utils.configure_decoupled_master(self.__group_4, self.__server_4)

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(4),
            "user" : "root"
        }

        uuid_server5 = MySQLServer.discover_uuid(**self.__options_5)
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)
        self.__server_5.connect()
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_5.exec_stmt("CREATE DATABASE db1")
        self.__server_5.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_5.exec_stmt("CREATE DATABASE db2")
        self.__server_5.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))

        self.__group_5 = Group("GROUPID5", "Fifth description.")
        Group.add( self.__group_5)
        self.__group_5.add_server(self.__server_5)
        tests.utils.configure_decoupled_master(self.__group_5, self.__server_5)

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{cc45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(5),
            "user" : "root"
        }

        uuid_server6 = MySQLServer.discover_uuid(**self.__options_6)
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)
        self.__server_6.connect()

        self.__group_6 = Group("GROUPID6", "Sixth description.")
        Group.add( self.__group_6)
        self.__group_6.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_6, self.__server_6)

        status = self.proxy.sharding.define("RANGE", "GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 1)

        status = self.proxy.sharding.add_mapping(1, "db1.t1", "userID")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")
        status = self.proxy.sharding.add_mapping(1, "db2.t2", "userID")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard(1, "GROUPID2",
                                               "ENABLED", 1)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(1, "GROUPID3",
                                               "ENABLED", 101)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(1, "GROUPID4",
                                               "ENABLED", 1001)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(1, "GROUPID5",
                                               "ENABLED", 10001)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.prune_shard("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")

    def test_move_shard_1(self):
        '''Test the move of shard 1 and the global server configuration
        after that. The test moves shard 1 from GROUPID2 to GROUPID6.
        After the move is done, it verifies the count on GROUPID6 to check
        that the group has all the tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples. GROUPID2 should not have received the values
        since it has had the shard moved away from it.
        '''
        row_cnt_shard_before_move_db1_t1 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db2_t2 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db1_t1 =\
            int(row_cnt_shard_before_move_db1_t1[0][0])
        row_cnt_shard_before_move_db2_t2 =\
            int(row_cnt_shard_before_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_before_move_db1_t1, 70)
        self.assertEqual(row_cnt_shard_before_move_db2_t2, 70)
        status = self.proxy.sharding.move("1", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_setup_resharding_switch).")
        row_cnt_shard_after_move_db1_t1 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db2_t2 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db1_t1 =\
            int(row_cnt_shard_after_move_db1_t1[0][0])
        row_cnt_shard_after_move_db2_t2 =\
            int(row_cnt_shard_after_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_after_move_db1_t1,
                         row_cnt_shard_before_move_db1_t1)
        self.assertEqual(row_cnt_shard_after_move_db2_t2,
                         row_cnt_shard_before_move_db2_t2)

        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
        try:
            #Verify that the data is not there in the first shard's old
            #group.
            global_table_count = self.__server_2.exec_stmt(
                        "SELECT COUNT(*) FROM global.global_table",
                        {"fetch" : True}
                    )
            raise Exception("Server should not be connected to global server")
        except _errors.DatabaseError:
            pass
        #Verify that the data is there in the second shard.
        global_table_count = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the third shard.
        global_table_count = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the first shard's new
        #group.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_move_shard_2(self):
        '''Test the move of shard 2 and the global server configuration
        after that.  The test moves shard 2 from GROUPID3 to GROUPID6.
        After the move is done, it verifies the count on GROUPID6 to check
        that the group has all the tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples. GROUPID3 should not have received the values
        since it has had the shard moved away from it.
        '''
        row_cnt_shard_before_move_db1_t1 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db2_t2 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db1_t1 =\
            int(row_cnt_shard_before_move_db1_t1[0][0])
        row_cnt_shard_before_move_db2_t2 =\
            int(row_cnt_shard_before_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_before_move_db1_t1, 200)
        self.assertEqual(row_cnt_shard_before_move_db2_t2, 200)
        status = self.proxy.sharding.move("2", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_setup_resharding_switch).")
        row_cnt_shard_after_move_db1_t1 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db2_t2 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db1_t1 =\
            int(row_cnt_shard_after_move_db1_t1[0][0])
        row_cnt_shard_after_move_db2_t2 =\
            int(row_cnt_shard_after_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_after_move_db1_t1,
                         row_cnt_shard_before_move_db1_t1)
        self.assertEqual(row_cnt_shard_after_move_db2_t2,
                         row_cnt_shard_before_move_db2_t2)
        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
        try:
            #Verify that the data is not there in the second shard's
            #old group.
            global_table_count = self.__server_3.exec_stmt(
                        "SELECT COUNT(*) FROM global.global_table",
                        {"fetch" : True}
                    )
            raise Exception("Server should not be connected to global server")
        except _errors.DatabaseError:
            pass
        #Verify that the data is there in the first shard.
        global_table_count = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the third shard.
        global_table_count = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the second shard's new
        #group.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_move_shard_3(self):
        '''Test the move of shard 3 and the global server configuration
        after that. The test moves shard 3 from GROUPID4 to GROUPID6.
        After the move is done, it verifies the count on GROUPID6 to check
        that the group has all the tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples. GROUPID4 should not have received the values
        since it has had the shard moved away from it.
        '''
        row_cnt_shard_before_move_db1_t1 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db2_t2 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db1_t1 =\
            int(row_cnt_shard_before_move_db1_t1[0][0])
        row_cnt_shard_before_move_db2_t2 =\
            int(row_cnt_shard_before_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_before_move_db1_t1, 200)
        self.assertEqual(row_cnt_shard_before_move_db2_t2, 200)
        status = self.proxy.sharding.move("3", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_setup_resharding_switch).")
        row_cnt_shard_after_move_db1_t1 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db2_t2 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db1_t1 =\
            int(row_cnt_shard_after_move_db1_t1[0][0])
        row_cnt_shard_after_move_db2_t2 =\
            int(row_cnt_shard_after_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_after_move_db1_t1,
                         row_cnt_shard_before_move_db1_t1)
        self.assertEqual(row_cnt_shard_after_move_db2_t2,
                         row_cnt_shard_before_move_db2_t2)
        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
        try:
            #Verify that the data is not there in the third shard's
            #original group.
            global_table_count = self.__server_4.exec_stmt(
                        "SELECT COUNT(*) FROM global.global_table",
                        {"fetch" : True}
                    )
            raise Exception("Server should not be connected to global server")
        except _errors.DatabaseError:
            pass
        #Verify that the data is there in the first shard.
        global_table_count = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the second shard.
        global_table_count = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the third shard
        #new group.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_move_shard_4(self):
        '''Test the move of shard 4 and the global server configuration
        after that. The test moves shard 4 from GROUPID5 to GROUPID6.
        After the move is done, it verifies the count on GROUPID6 to check
        that the group has all the tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples. GROUPID5 should not have received the values
        since it has had the shard moved away from it.
        '''
        row_cnt_shard_before_move_db1_t1 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db2_t2 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_before_move_db1_t1 =\
            int(row_cnt_shard_before_move_db1_t1[0][0])
        row_cnt_shard_before_move_db2_t2 =\
            int(row_cnt_shard_before_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_before_move_db1_t1, 400)
        self.assertEqual(row_cnt_shard_before_move_db2_t2, 400)
        status = self.proxy.sharding.move("4", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_setup_resharding_switch).")
        row_cnt_shard_after_move_db1_t1 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db2_t2 = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_move_db1_t1 =\
            int(row_cnt_shard_after_move_db1_t1[0][0])
        row_cnt_shard_after_move_db2_t2 =\
            int(row_cnt_shard_after_move_db2_t2[0][0])

        self.assertEqual(row_cnt_shard_after_move_db1_t1,
                         row_cnt_shard_before_move_db1_t1)
        self.assertEqual(row_cnt_shard_after_move_db2_t2,
                         row_cnt_shard_before_move_db2_t2)
        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
        try:
            #Verify that the data is not there in the fourth shard's
            #original group.
            global_table_count = self.__server_5.exec_stmt(
                        "SELECT COUNT(*) FROM global.global_table",
                        {"fetch" : True}
                    )
            raise Exception("Server should not be connected to global server")
        except _errors.DatabaseError:
            pass
        #Verify that the data is there in the first shard.
        global_table_count = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the second shard.
        global_table_count = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the third shard.
        global_table_count = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fourth shard's new
        #group.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)


    def tearDown(self):
        status = self.proxy.sharding.disable_shard("1")
        status = self.proxy.sharding.disable_shard("2")
        status = self.proxy.sharding.disable_shard("3")
        status = self.proxy.sharding.disable_shard("4")
        status = self.proxy.sharding.disable_shard("5")
        status = self.proxy.sharding.disable_shard("6")

        status = self.proxy.sharding.remove_shard("1")
        status = self.proxy.sharding.remove_shard("2")
        status = self.proxy.sharding.remove_shard("3")
        status = self.proxy.sharding.remove_shard("4")
        status = self.proxy.sharding.remove_shard("5")
        status = self.proxy.sharding.remove_shard("6")

        status = self.proxy.sharding.remove_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping).")

        self.proxy.group.demote("GROUPID1")
        self.proxy.group.demote("GROUPID2")
        self.proxy.group.demote("GROUPID3")
        self.proxy.group.demote("GROUPID4")
        self.proxy.group.demote("GROUPID5")
        self.proxy.group.demote("GROUPID6")

        for group_id in ("GROUPID1", "GROUPID2", "GROUPID3",
            "GROUPID4", "GROUPID5", "GROUPID6"):
            status = self.proxy.group.lookup_servers(group_id)
            self.assertEqual(status["success"], True)
            self.assertEqual(status["message"], False)
            obtained_server_list = status["return"]
            status = self.proxy.group.remove(group_id, obtained_server_list[0][0])
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_remove_server).")
            status = self.proxy.group.destroy(group_id)
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_destroy_group).")

        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

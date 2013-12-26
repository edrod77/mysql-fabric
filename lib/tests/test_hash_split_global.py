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

"""Tests HASH shard splits and also the impact of the splits
on the global server.
"""
import unittest
import uuid as _uuid
import time
import tests.utils

from mysql.fabric import executor as _executor
from mysql.fabric.server import (
    Group,
    MySQLServer,
)
from tests.utils import MySQLInstances

class TestHashSplitGlobal(unittest.TestCase):
    """Contains unit tests for testing the shard split operation and for
    verifying that the global server configuration remains constant after
    the shard split configuration.
    """

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        """Configure the existing environment
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
        for i in range(1, 1001):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_2.exec_stmt("CREATE DATABASE db2")
        self.__server_2.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_2.exec_stmt("CREATE DATABASE db3")
        self.__server_2.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID INT, Department INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_2.exec_stmt("INSERT INTO db3.t3 "
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
        for i in range(1, 1001):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_3.exec_stmt("CREATE DATABASE db2")
        self.__server_3.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_3.exec_stmt("CREATE DATABASE db3")
        self.__server_3.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID INT, Department INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_3.exec_stmt("INSERT INTO db3.t3 "
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
        for i in range(1, 1001):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_4.exec_stmt("CREATE DATABASE db2")
        self.__server_4.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_4.exec_stmt("CREATE DATABASE db3")
        self.__server_4.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID INT, Department INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_4.exec_stmt("INSERT INTO db3.t3 "
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
        for i in range(1, 1001):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_5.exec_stmt("CREATE DATABASE db2")
        self.__server_5.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_5.exec_stmt("CREATE DATABASE db3")
        self.__server_5.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID INT, Department INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 1001):
            self.__server_5.exec_stmt("INSERT INTO db3.t3 "
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

        self.__group_6 = Group("GROUPID6", "Sixth description.")
        Group.add( self.__group_6)
        self.__group_6.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_6, self.__server_6)

        status = self.proxy.sharding.define("HASH", "GROUPID1")
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
        status = self.proxy.sharding.add_mapping(1, "db3.t3", "userID")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard(1, "GROUPID2", "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard(1, "GROUPID3", "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard(1, "GROUPID4", "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard(1, "GROUPID5", "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.prune_shard("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")
        status = self.proxy.sharding.prune_shard("db2.t2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")
        status = self.proxy.sharding.prune_shard("db3.t3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")

    def test_split_shard_1(self):
        '''Test the split of shard 1 and the global server configuration
        after that. The test splits shard 1 between GROUPID2 and GROUPID6.
        After the split is done, it verifies the count on GROUPID6 to check
        that the group has tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples.
        '''
        row_cnt_shard_db1_t1 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_db2_t2 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_db3_t3 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_db1_t1 = int(row_cnt_shard_db1_t1[0][0])
        row_cnt_shard_db2_t2 = int(row_cnt_shard_db2_t2[0][0])
        row_cnt_shard_db3_t3 = int(row_cnt_shard_db3_t3[0][0])

        status = self.proxy.sharding.split("1", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables_after_split).")

        self.__server_6.connect()

        row_cnt_shard_after_split_1_db1_t1 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3 = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_a = \
            int(row_cnt_shard_after_split_1_db1_t1[0][0])
        row_cnt_shard_after_split_1_db2_t2_a = \
            int(row_cnt_shard_after_split_1_db2_t2[0][0])
        row_cnt_shard_after_split_1_db3_t3_a = \
            int(row_cnt_shard_after_split_1_db3_t3[0][0])

        row_cnt_shard_after_split_1_db1_t1_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_b = \
            int(row_cnt_shard_after_split_1_db1_t1_b[0][0])
        row_cnt_shard_after_split_1_db2_t2_b = \
            int(row_cnt_shard_after_split_1_db2_t2_b[0][0])
        row_cnt_shard_after_split_1_db3_t3_b = \
            int(row_cnt_shard_after_split_1_db3_t3_b[0][0])

        self.assertTrue(
            row_cnt_shard_db1_t1 ==
            (row_cnt_shard_after_split_1_db1_t1_a +
            row_cnt_shard_after_split_1_db1_t1_b)
        )
        self.assertTrue(
            row_cnt_shard_db2_t2 ==
            (row_cnt_shard_after_split_1_db2_t2_a +
            row_cnt_shard_after_split_1_db2_t2_b)
        )
        self.assertTrue(
            row_cnt_shard_db3_t3 ==
            (row_cnt_shard_after_split_1_db3_t3_a +
            row_cnt_shard_after_split_1_db3_t3_b)
        )

        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
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
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fifth shard.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_split_shard_2(self):
        '''Test the split of shard 2 and the global server configuration
        after that. The test splits shard 2 between GROUPID3 and GROUPID6.
        After the split is done, it verifies the count on GROUPID6 to check
        that the group has tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples.
        '''
        row_cnt_shard_db1_t1 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_db2_t2 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_db3_t3 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_db1_t1 = int(row_cnt_shard_db1_t1[0][0])
        row_cnt_shard_db2_t2 = int(row_cnt_shard_db2_t2[0][0])
        row_cnt_shard_db3_t3 = int(row_cnt_shard_db3_t3[0][0])

        status = self.proxy.sharding.split("2", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables_after_split).")

        self.__server_6.connect()

        row_cnt_shard_after_split_1_db1_t1 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_a = \
            int(row_cnt_shard_after_split_1_db1_t1[0][0])
        row_cnt_shard_after_split_1_db2_t2_a = \
            int(row_cnt_shard_after_split_1_db2_t2[0][0])
        row_cnt_shard_after_split_1_db3_t3_a = \
            int(row_cnt_shard_after_split_1_db3_t3[0][0])

        row_cnt_shard_after_split_1_db1_t1_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_b = \
            int(row_cnt_shard_after_split_1_db1_t1_b[0][0])
        row_cnt_shard_after_split_1_db2_t2_b = \
            int(row_cnt_shard_after_split_1_db2_t2_b[0][0])
        row_cnt_shard_after_split_1_db3_t3_b = \
            int(row_cnt_shard_after_split_1_db3_t3_b[0][0])

        self.assertTrue(
            row_cnt_shard_db1_t1 ==
            (row_cnt_shard_after_split_1_db1_t1_a +
            row_cnt_shard_after_split_1_db1_t1_b)
        )
        self.assertTrue(
            row_cnt_shard_db2_t2 ==
            (row_cnt_shard_after_split_1_db2_t2_a +
            row_cnt_shard_after_split_1_db2_t2_b)
        )
        self.assertTrue(
            row_cnt_shard_db3_t3 ==
            (row_cnt_shard_after_split_1_db3_t3_a +
            row_cnt_shard_after_split_1_db3_t3_b)
        )

        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
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
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fifth shard.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_split_shard_3(self):
        '''Test the split of shard 3 and the global server configuration
        after that. The test splits shard 3 between GROUPID4 and GROUPID6.
        After the split is done, it verifies the count on GROUPID6 to check
        that the group has tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples.
        '''
        row_cnt_shard_db1_t1 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_db2_t2 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_db3_t3 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_db1_t1 = int(row_cnt_shard_db1_t1[0][0])
        row_cnt_shard_db2_t2 = int(row_cnt_shard_db2_t2[0][0])
        row_cnt_shard_db3_t3 = int(row_cnt_shard_db3_t3[0][0])

        status = self.proxy.sharding.split("3", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables_after_split).")

        self.__server_6.connect()

        row_cnt_shard_after_split_1_db1_t1 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3 = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_a = \
            int(row_cnt_shard_after_split_1_db1_t1[0][0])
        row_cnt_shard_after_split_1_db2_t2_a = \
            int(row_cnt_shard_after_split_1_db2_t2[0][0])
        row_cnt_shard_after_split_1_db3_t3_a = \
            int(row_cnt_shard_after_split_1_db3_t3[0][0])

        row_cnt_shard_after_split_1_db1_t1_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_b = \
            int(row_cnt_shard_after_split_1_db1_t1_b[0][0])
        row_cnt_shard_after_split_1_db2_t2_b = \
            int(row_cnt_shard_after_split_1_db2_t2_b[0][0])
        row_cnt_shard_after_split_1_db3_t3_b = \
            int(row_cnt_shard_after_split_1_db3_t3_b[0][0])

        self.assertTrue(
            row_cnt_shard_db1_t1 ==
            (row_cnt_shard_after_split_1_db1_t1_a +
            row_cnt_shard_after_split_1_db1_t1_b)
        )
        self.assertTrue(
            row_cnt_shard_db2_t2 ==
            (row_cnt_shard_after_split_1_db2_t2_a +
            row_cnt_shard_after_split_1_db2_t2_b)
        )
        self.assertTrue(
            row_cnt_shard_db3_t3 ==
            (row_cnt_shard_after_split_1_db3_t3_a +
            row_cnt_shard_after_split_1_db3_t3_b)
        )

        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
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
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fifth shard.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_split_shard_4(self):
        '''Test the split of shard 4 and the global server configuration
        after that. The test splits shard 4 between GROUPID5 and GROUPID6.
        After the split is done, it verifies the count on GROUPID6 to check
        that the group has tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples.
        '''
        row_cnt_shard_db1_t1 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_db2_t2 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_db3_t3 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_db1_t1 = int(row_cnt_shard_db1_t1[0][0])
        row_cnt_shard_db2_t2 = int(row_cnt_shard_db2_t2[0][0])
        row_cnt_shard_db3_t3 = int(row_cnt_shard_db3_t3[0][0])

        status = self.proxy.sharding.split("4", "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables_after_split).")

        self.__server_6.connect()

        row_cnt_shard_after_split_1_db1_t1 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_a = \
            int(row_cnt_shard_after_split_1_db1_t1[0][0])
        row_cnt_shard_after_split_1_db2_t2_a = \
            int(row_cnt_shard_after_split_1_db2_t2[0][0])
        row_cnt_shard_after_split_1_db3_t3_a = \
            int(row_cnt_shard_after_split_1_db3_t3[0][0])

        row_cnt_shard_after_split_1_db1_t1_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db2_t2_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_1_db3_t3_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )

        row_cnt_shard_after_split_1_db1_t1_b = \
            int(row_cnt_shard_after_split_1_db1_t1_b[0][0])
        row_cnt_shard_after_split_1_db2_t2_b = \
            int(row_cnt_shard_after_split_1_db2_t2_b[0][0])
        row_cnt_shard_after_split_1_db3_t3_b = \
            int(row_cnt_shard_after_split_1_db3_t3_b[0][0])

        self.assertTrue(
            row_cnt_shard_db1_t1 ==
            (row_cnt_shard_after_split_1_db1_t1_a +
            row_cnt_shard_after_split_1_db1_t1_b)
        )
        self.assertTrue(
            row_cnt_shard_db2_t2 ==
            (row_cnt_shard_after_split_1_db2_t2_a +
            row_cnt_shard_after_split_1_db2_t2_b)
        )
        self.assertTrue(
            row_cnt_shard_db3_t3 ==
            (row_cnt_shard_after_split_1_db3_t3_a +
            row_cnt_shard_after_split_1_db3_t3_b)
        )
        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        time.sleep(5)
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
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fifth shard.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_hash_dump(self):
        """Test the dump of the HASH sharding information for the table.
        """
        shardinginformation_1 = [0, 0, 0,
            [['db1', 't1', 'userID',
            'E2996A7B8509367020B55A4ACD2AE46A',
            '1', 'HASH', 'GROUPID2', 'GROUPID1'],
            ['db1', 't1', 'userID',
            'E88F547DF45C99F27646C76316FB21DF',
            '2', 'HASH', 'GROUPID3', 'GROUPID1'],
            ['db1', 't1', 'userID',
            '7A2E76448FF04233F3851A492BEF1090',
            '3', 'HASH', 'GROUPID4', 'GROUPID1'],
            ['db1', 't1', 'userID',
            '97427AA63E300F56536710F5D73A35FA',
            '4', 'HASH', 'GROUPID5', 'GROUPID1']]]
        self.assertEqual(
            self.proxy.store.dump_sharding_information(0, "db1.t1"),
            shardinginformation_1
        )

    def tearDown(self):
        """Clean up the existing environment
        """
        self.proxy.sharding.disable_shard(1)
        self.proxy.sharding.remove_shard(1)

        self.proxy.sharding.disable_shard(2)
        self.proxy.sharding.remove_shard(2)

        self.proxy.sharding.disable_shard(3)
        self.proxy.sharding.remove_shard(3)

        self.proxy.sharding.disable_shard(4)
        self.proxy.sharding.remove_shard(4)

        self.proxy.sharding.disable_shard(5)
        self.proxy.sharding.remove_shard(5)

        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

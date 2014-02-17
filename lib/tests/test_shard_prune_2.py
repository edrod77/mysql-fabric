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

import unittest
import uuid as _uuid
import tests.utils

from mysql.fabric import executor as _executor
from mysql.fabric.server import (
    Group,
    MySQLServer,
)
from tests.utils import MySQLInstances

class TestShardingPrune(unittest.TestCase):

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

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd
        }

        uuid_server1 = MySQLServer.discover_uuid(**self.__options_1)
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

        uuid_server2 = MySQLServer.discover_uuid(**self.__options_2)
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)
        self.__server_2.connect()
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_2.exec_stmt("CREATE DATABASE db1")
        self.__server_2.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
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

        uuid_server3 = MySQLServer.discover_uuid(**self.__options_3)
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)
        self.__server_3.connect()
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_3.exec_stmt("CREATE DATABASE db1")
        self.__server_3.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
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

        uuid_server4 = MySQLServer.discover_uuid(**self.__options_4)
        self.__options_4["uuid"] = _uuid.UUID(uuid_server4)
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)
        self.__server_4.connect()
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_4.exec_stmt("CREATE DATABASE db1")
        self.__server_4.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
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

        uuid_server5 = MySQLServer.discover_uuid(**self.__options_5)
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)
        self.__server_5.connect()
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_5.exec_stmt("CREATE DATABASE db1")
        self.__server_5.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
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

        uuid_server6 = MySQLServer.discover_uuid(**self.__options_6)
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)
        self.__server_6.connect()
        self.__server_6.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_6.exec_stmt("CREATE DATABASE db1")
        self.__server_6.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_6.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_6.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_6.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_6.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_6.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

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
        status = self.proxy.sharding.add_shard(1, "GROUPID6",
                                               "ENABLED", 100001)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

    def test_prune_shard(self):
        status = self.proxy.sharding.prune_shard("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")

        status = self.proxy.sharding.lookup_servers("db1.t1", 1,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        shard_uuid = obtained_server_list[0][0]
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 70)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 70)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 1)

        status = self.proxy.sharding.lookup_servers("db1.t1", 101,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        shard_uuid = obtained_server_list[0][0]
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 200)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 300)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 101)

        status = self.proxy.sharding.lookup_servers("db1.t1", 1202,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        shard_uuid = obtained_server_list[0][0]
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 200)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 1200)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 1001)

        status = self.proxy.sharding.lookup_servers("db1.t1", 11000,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        shard_uuid = obtained_server_list[0][0]
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 200)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 10200)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 10001)

        status = self.proxy.sharding.lookup_servers("db1.t1", 100100,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        shard_uuid = obtained_server_list[0][0]
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        rows = shard_server.exec_stmt(
                                    "SELECT COUNT(*) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 200)
        rows = shard_server.exec_stmt(
                                    "SELECT MAX(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100200)
        rows = shard_server.exec_stmt(
                                    "SELECT MIN(userID) FROM db1.t1",
                                    {"fetch" : True})
        self.assertTrue(int(rows[0][0]) == 100001)

    def tearDown(self):
        self.__server_2.exec_stmt("DROP TABLE db1.t1")
        self.__server_2.exec_stmt("DROP DATABASE db1")
        self.__server_3.exec_stmt("DROP TABLE db1.t1")
        self.__server_3.exec_stmt("DROP DATABASE db1")
        self.__server_4.exec_stmt("DROP TABLE db1.t1")
        self.__server_4.exec_stmt("DROP DATABASE db1")
        self.__server_5.exec_stmt("DROP TABLE db1.t1")
        self.__server_5.exec_stmt("DROP DATABASE db1")
        self.__server_6.exec_stmt("DROP TABLE db1.t1")
        self.__server_6.exec_stmt("DROP DATABASE db1")

        status = self.proxy.sharding.disable_shard("1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.disable_shard("2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.disable_shard("3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.disable_shard("4")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.disable_shard("5")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.remove_shard("1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")

        status = self.proxy.sharding.remove_shard("2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")

        status = self.proxy.sharding.remove_shard("3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")

        status = self.proxy.sharding.remove_shard("4")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")

        status = self.proxy.sharding.remove_shard("5")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")

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
            self.assertEqual(status[0], True)
            self.assertEqual(status[1], "")
            obtained_server_list = status[2]
            status = \
                self.proxy.group.remove(group_id, obtained_server_list[0][0])
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

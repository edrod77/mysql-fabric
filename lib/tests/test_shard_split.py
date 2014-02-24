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
import tests.utils

from time import sleep
from mysql.fabric import executor as _executor
from mysql.fabric.server import MySQLServer
from tests.utils import (
    MySQLInstances,
    ShardingUtils,
)
from mysql.fabric.sharding import RangeShardingSpecification
from mysql.fabric.errors import DatabaseError

class TestShardSplit(unittest.TestCase):

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
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")
        status = self.proxy.group.add(
            "GROUPID1", MySQLInstances().get_address(0)
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        status = self.proxy.group.add(
            "GROUPID1", MySQLInstances().get_address(1)
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        status = self.proxy.group.create("GROUPID2", "Second description.")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")
        status = self.proxy.group.add(
            "GROUPID2", MySQLInstances().get_address(2)
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        status =  self.proxy.group.add(
            "GROUPID2", MySQLInstances().get_address(3)
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        status = self.proxy.group.create("GROUPID3", "Third description.")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")
        status = self.proxy.group.add(
            "GROUPID3", MySQLInstances().get_address(4)
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        status = self.proxy.group.add(
            "GROUPID3", MySQLInstances().get_address(5)
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        status = self.proxy.group.promote("GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")
        status = self.proxy.group.promote("GROUPID2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")
        status = self.proxy.group.promote("GROUPID3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        status = self.proxy.sharding.create_definition("RANGE", "GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard(1, "GROUPID2",
                                               "ENABLED", 0)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            if obtained_server_list[idx][2]:
                shard_uuid = obtained_server_list[idx][0]
                shard_server = MySQLServer.fetch(shard_uuid)
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

    def test_shard_split(self):
        status = self.proxy.sharding.split_shard("1", "GROUPID3", "600")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables_after_split).")
        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            server_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(server_uuid)
            shard_server.connect()
            rows = shard_server.exec_stmt(
                                    "SELECT NAME FROM db1.t1",
                                    {"fetch" : True})
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0][0], 'TEST 1')
            self.assertEqual(rows[1][0], 'TEST 2')
            self.assertEqual(rows[2][0], 'TEST 3')

        status = self.proxy.sharding.lookup_servers("db1.t1", 800,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            server_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(server_uuid)
            shard_server.connect()
            rows = shard_server.exec_stmt(
                                    "SELECT NAME FROM db1.t1",
                                    {"fetch" : True})
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[0][0], 'TEST 4')
            self.assertEqual(rows[1][0], 'TEST 5')
            self.assertEqual(rows[2][0], 'TEST 6')
            self.assertEqual(rows[3][0], 'TEST 7')

        status = self.proxy.sharding.lookup_servers("1", 500,  "GLOBAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            if obtained_server_list[idx][2]:
                global_master_uuid = obtained_server_list[idx][0]
                break

        global_master = MySQLServer.fetch(global_master_uuid)
        global_master.connect()

        global_master.exec_stmt("DROP DATABASE IF EXISTS global_db")
        global_master.exec_stmt("CREATE DATABASE global_db")
        global_master.exec_stmt("CREATE TABLE global_db.global_table"
                                  "(userID INT, name VARCHAR(30))")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(101, 'TEST 1')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(202, 'TEST 2')")

        status = self.proxy.group.promote("GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        sleep(5)

        status = self.proxy.sharding.lookup_servers("1", 500,  "GLOBAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            if obtained_server_list[idx][2]:
                global_master_uuid = obtained_server_list[idx][0]
                break

        global_master = MySQLServer.fetch(global_master_uuid)
        global_master.connect()

        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(303, 'TEST 3')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(404, 'TEST 4')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(505, 'TEST 5')")
        global_master.exec_stmt("INSERT INTO global_db.global_table "
                                  "VALUES(606, 'TEST 6')")

        sleep(5)

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            if obtained_server_list[idx][2]:
                shard_uuid = obtained_server_list[idx][0]
                shard_server = MySQLServer.fetch(shard_uuid)
                shard_server.connect()
                rows = shard_server.exec_stmt(
                    "SELECT NAME FROM global_db.global_table", {"fetch" : True}
                )
                self.assertEqual(len(rows), 6)
                self.assertEqual(rows[0][0], 'TEST 1')
                self.assertEqual(rows[1][0], 'TEST 2')
                self.assertEqual(rows[2][0], 'TEST 3')
                self.assertEqual(rows[3][0], 'TEST 4')
                self.assertEqual(rows[4][0], 'TEST 5')
                self.assertEqual(rows[5][0], 'TEST 6')

        #Ensure tha two new shard_ids have been generated.
        range_sharding_specifications = RangeShardingSpecification.list(1)
        self.assertTrue(ShardingUtils.compare_range_specifications(
            range_sharding_specifications[0],
            RangeShardingSpecification.fetch(2)))
        self.assertTrue(ShardingUtils.compare_range_specifications(
            range_sharding_specifications[1],
            RangeShardingSpecification.fetch(3)))

    def test_update_only(self):
        """Test the shard split but without provisioning.
        """
        # Get group information before the shard_move operation
        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        local_list_before = status[2]
        status = self.proxy.sharding.lookup_servers("1", 500,  "GLOBAL")
        global_list_before = status[2]

        # Do the shard split and compare group information.
        status = self.proxy.sharding.split_shard("1", "GROUPID3", "600", True)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_setup_resharding_switch).")
        status = self.proxy.sharding.lookup_servers("db1.t1", 601,  "LOCAL")
        local_list_after = status[2]
        self.assertNotEqual(local_list_before, local_list_after)
        status = self.proxy.sharding.lookup_servers("1", 601,  "GLOBAL")
        global_list_after = status[2]
        self.assertEqual(global_list_before, global_list_after)

        # The group has changed but no data was transfered.
        shard_server = MySQLServer.fetch(local_list_after[0][0])
        shard_server.connect()
        self.assertRaises(
            DatabaseError, shard_server.exec_stmt,
            "SELECT NAME FROM db1.t1", {"fetch" : True}
        )

    def tearDown(self):
        self.proxy.sharding.enable_shard("2")

        status = self.proxy.sharding.lookup_servers("1", 500,  "GLOBAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            shard_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            shard_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")
            shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")

        status = self.proxy.sharding.lookup_servers("db1.t1", 800,  "LOCAL")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            shard_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")
            shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")

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

        status = self.proxy.sharding.remove_table("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping).")

        status = self.proxy.sharding.remove_definition("1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping_defn).")

        self.proxy.group.demote("GROUPID1")
        self.proxy.group.demote("GROUPID2")
        self.proxy.group.demote("GROUPID3")
        for group_id in ("GROUPID1", "GROUPID2", "GROUPID3"):
            status = self.proxy.group.lookup_servers(group_id)
            self.assertEqual(status[0], True)
            self.assertEqual(status[1], "")
            obtained_server_list = status[2]
            status = \
                self.proxy.group.remove(
                    group_id, obtained_server_list[0]["server_uuid"]
                )
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_remove_server).")

            status = \
                self.proxy.group.remove(
                    group_id, obtained_server_list[1]["server_uuid"]
                )
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

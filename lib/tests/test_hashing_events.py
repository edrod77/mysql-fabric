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

class TestShardingServices(unittest.TestCase):

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        """Configure the existing environment
        """
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

        status = self.proxy.sharding.add_mapping(1, "db1.t1", "userID1")
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

        status = self.proxy.sharding.add_shard(1, "GROUPID6", "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

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

    def test_add_shard_invalid_group_exception(self):
        #Use an invalid group ID (WRONG_GROUP)
        status = self.proxy.sharding.add_shard(4, "WRONG_GROUP", "ENABLED")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")

    def test_add_shard_invalid_state_exception(self):
        #WRONG_STATE is an invalid description of the shard state.
        status = self.proxy.sharding.add_shard(4, "GROUP4", "WRONG_STATE")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")

    def test_add_shard_invalid_shard_mapping(self):
        status = self.proxy.sharding.add_shard(25000, "GROUPID4", "ENABLED")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")

    def test_remove_shard_mapping_shards_exist_exception(self):
        status = self.proxy.sharding.remove_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard_mapping).")

    def test_remove_sharding_specification(self):
        status = self.proxy.sharding.disable_shard(1)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")
        status = self.proxy.sharding.remove_shard(1)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")
        status = self.proxy.sharding.disable_shard(2)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")
        status = self.proxy.sharding.remove_shard(2)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")
        status = self.proxy.sharding.disable_shard(3)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")
        status = self.proxy.sharding.remove_shard(3)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")
        status = self.proxy.sharding.disable_shard(4)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")
        status = self.proxy.sharding.remove_shard(4)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")
        status = self.proxy.sharding.disable_shard(5)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")
        status = self.proxy.sharding.remove_shard(5)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2], True)

    def test_remove_sharding_specification_exception(self):
        status = self.proxy.sharding.remove_shard(1)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard).")

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_mapping("db1.t1")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        self.assertEqual(status[2], {"shard_mapping_id":1,
                                     "table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"HASH",
                                     "global_group":"GROUPID1"})

    def test_list(self):
        status = self.proxy.sharding.list_mappings("HASH")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        self.assertEqual(status[2], [{"shard_mapping_id":1,
                                     "table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"HASH",
                                     "global_group":"GROUPID1"}])

    def test_list_exception(self):
        status = self.proxy.sharding.list_mappings("Wrong")
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2], True)


    def test_lookup(self):
        shard_1_hit_count = 0
        shard_2_hit_count = 0
        shard_3_hit_count = 0
        shard_4_hit_count = 0
        shard_5_hit_count = 0

        for i in range(1,  200):
            status = self.proxy.sharding.lookup_servers("db1.t1", i, "LOCAL")
            self.assertEqual(status[0], True)
            self.assertEqual(status[1], "")
            obtained_server_list = status[2]

            if obtained_server_list[0][1] == self.__server_2.address and \
                str(obtained_server_list[0][0]) == str(self.__server_2.uuid):
                shard_1_hit_count = shard_1_hit_count + 1
            elif obtained_server_list[0][1] == self.__server_3.address and \
                str(obtained_server_list[0][0]) == str(self.__server_3.uuid):
                shard_2_hit_count = shard_2_hit_count + 1
            elif obtained_server_list[0][1] == self.__server_4.address and \
                str(obtained_server_list[0][0]) == str(self.__server_4.uuid):
                shard_3_hit_count = shard_3_hit_count + 1
            elif obtained_server_list[0][1] == self.__server_5.address and \
                str(obtained_server_list[0][0]) == str(self.__server_5.uuid):
                shard_4_hit_count = shard_4_hit_count + 1
            elif obtained_server_list[0][1] == self.__server_6.address and \
                str(obtained_server_list[0][0]) == str(self.__server_6.uuid):
                shard_5_hit_count = shard_5_hit_count + 1

        self.assertTrue(shard_1_hit_count > 0)
        self.assertTrue(shard_2_hit_count > 0)
        self.assertTrue(shard_3_hit_count > 0)
        self.assertTrue(shard_4_hit_count > 0)
        self.assertTrue(shard_5_hit_count > 0)

    def test_lookup_disabled_exception(self):
        #Since inhashing it is difficult to guess which shard the value will
        #fall into, disable both the shards.
        self.proxy.sharding.disable_shard(1)
        self.proxy.sharding.disable_shard(2)
        self.proxy.sharding.disable_shard(3)
        self.proxy.sharding.disable_shard(4)
        self.proxy.sharding.disable_shard(5)
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2], True)


    def test_lookup_wrong_table_exception(self):
        status = self.proxy.sharding.lookup_servers("Wrong", 500, "LOCAL")
        self.assertEqual(status[0], False)
        self.assertNotEqual(status[1], "")
        self.assertEqual(status[2], True)

    def test_list_shard_mappings(self):
        expected_shard_mapping_list1 =   [1, "HASH", "GROUPID1"]
        status = self.proxy.sharding.list_definitions()
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_shard_mapping_list = status[2]
        self.assertEqual(
            set(expected_shard_mapping_list1),
            set(obtained_shard_mapping_list[0])
        )

    def test_enable_shard_exception(self):
        status = self.proxy.sharding.enable_shard(25000)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_enable_shard).")

    def test_disable_shard_exception(self):
        status = self.proxy.sharding.disable_shard(25000)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_disable_shard).")

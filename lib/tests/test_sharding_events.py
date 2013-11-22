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

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(1),
            "user" : "root"
        }

        uuid_server2 = MySQLServer.discover_uuid(**self.__options_2)
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)

        self.__group_1 = Group("GROUPID1", "First description.")
        Group.add(self.__group_1)
        self.__group_1.add_server(self.__server_1)
        self.__group_1.add_server(self.__server_2)
        tests.utils.configure_decoupled_master(self.__group_1, self.__server_1)

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : "root"
        }

        uuid_server3 = MySQLServer.discover_uuid(**self.__options_3)
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{bb45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(3),
            "user" : "root"
        }

        uuid_server4 = MySQLServer.discover_uuid(**self.__options_4)
        self.__options_4["uuid"] = _uuid.UUID(uuid_server4)
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)

        self.__group_2 = Group("GROUPID2", "Second description.")
        Group.add(self.__group_2)
        self.__group_2.add_server(self.__server_3)
        self.__group_2.add_server(self.__server_4)
        tests.utils.configure_decoupled_master(self.__group_2, self.__server_3)

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(4),
            "user" : "root"
        }

        uuid_server5 = MySQLServer.discover_uuid(**self.__options_5)
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{cc45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(5),
            "user" : "root"
        }

        uuid_server6 = MySQLServer.discover_uuid(**self.__options_6)
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)

        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add( self.__group_3)
        self.__group_3.add_server(self.__server_5)
        self.__group_3.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_5)

        status = self.proxy.sharding.define("RANGE", "GROUPID1")
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

        status = self.proxy.sharding.add_shard(1, "GROUPID2", "ENABLED", 0)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        #Ensure that adding a invalid lower bound for RANGE sharding fails.
        status = self.proxy.sharding.add_shard(1, "GROUPID3",
                                               "ENABLED", "ABCDEFGH")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(
            status[1][-1]["description"],
            "Tried to execute action (_add_shard)."
        )

        #Ensure that adding an invalid shard_mapping_id fails.
        status = self.proxy.sharding.add_shard(20000, "GROUPID2", "ENABLED", 0)
        self.assertStatus(status,  _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                        "Tried to execute action (_add_shard).")

        #Ensure that adding a string, but valid lower_bound passes.
        status = self.proxy.sharding.add_shard(1, "GROUPID3", "ENABLED",
                        "1001")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        self.__group_4 = Group("GROUPID4", "FOURTH DUMMY DESCRIPTION")
        Group.add(self.__group_4)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_fail_duplicate_add_shard(self):
        """Tests that addition of an existing lower_bound to a
        shard mapping fails. Also test adding lower_bounds with
        a 0 pre-pended the value being inserted.
        """
        status = self.proxy.sharding.add_shard(1, "GROUPID2", "ENABLED",  0)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(
            status[1][-1]["description"],
            "Tried to execute action (_add_shard)."
        )

        #Since the lower_bound datatype is a VARBINARY, ensure that
        #pre-pending a 00 to the lower bound does not result in adding
        #the same values in the data store. This basically shows that
        #comparisons of integers are not impacted with a 00 pre-pended
        #to the values.
        status = self.proxy.sharding.add_shard(1, 001001, "GROUPID3",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(
            status[1][-1]["description"],
            "Tried to execute action (_add_shard)."
        )

        #Since the lower_bound datatype is a VARBINARY, ensure that
        #pre-pending a 0000 to the lower bound does not result in adding
        #the same values in the data store. This basically shows that
        #comparisons of integers are not impacted with a 00 pre-pended
        #to the values.
        status = self.proxy.sharding.add_shard(1, 00001001, "GROUPID3",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(
            status[1][-1]["description"],
            "Tried to execute action (_add_shard)."
        )

    def test_define_shard_mapping_wrong_sharding_type(self):
        #Use an invalid sharding type (WRONG)
        status = self.proxy.sharding.define("WRONG", "GROUPID4")
        self.assertStatus(status,  _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_define_shard_mapping).")

    def test_add_shard_invalid_group_exception(self):
        #Use an invalid group ID (WRONG_GROUP)
        status = self.proxy.sharding.add_shard(4, "WRONG_GROUP",
                                               "ENABLED", 8001)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")

    def test_add_shard_invalid_state_exception(self):
        #WRONG_STATE is an invalid description of the shard state.
        status = self.proxy.sharding.add_shard(4, "GROUP4",
                                               "WRONG_STATE", 8001)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")

    def test_add_shard_invalid_range_exception(self):
        #Notice LB > UB in the case below.
        status = self.proxy.sharding.add_shard(4, "GROUP4",
                                               "ENABLED", 9000)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")


    def test_add_shard_invalid_shard_mapping(self):
        status = self.proxy.sharding.add_shard(25000, "GROUPID4",
                                               "ENABLED", 8001)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")
        status = self.proxy.sharding.add_shard(25000, "GROUPID4",
                                               "ENABLED", 8001)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_add_shard).")


    def test_remove_shard_mapping(self):
        #Remove the shards before removing the mapping
        self.proxy.sharding.disable_shard("1")
        self.proxy.sharding.remove_shard("1")
        self.proxy.sharding.disable_shard("2")
        self.proxy.sharding.remove_shard("2")
        status = self.proxy.sharding.remove_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping).")

        status = self.proxy.sharding.lookup_mapping("db1.t1")
        self.assertEqual(status["success"],True)
        self.assertEqual(status["message"], False)
        self.assertEqual(status["return"], {"shard_mapping_id":"",
                                     "table_name":"",
                                     "column_name":"",
                                     "type_name":"",
                                     "global_group":""})

    def test_remove_shard_mapping_shards_exist_exception(self):
        status = self.proxy.sharding.remove_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard_mapping).")

    def test_remove_shard_mapping_exception(self):
        status = self.proxy.sharding.remove_mapping("Wrong")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard_mapping).")

    def test_remove_sharding_specification(self):
        expected_server_list = [
                        [str(self.__server_5.uuid),
                         MySQLInstances().get_address(4),
                         True],
                        [str(self.__server_6.uuid),
                         MySQLInstances().get_address(5),
                         False]
                        ]
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
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.assertEqual(status["success"], False)
        self.assertEqual(status["message"], "Error:Invalid Key 500")
        self.assertEqual(status["return"], False)

    def test_remove_sharding_specification_exception(self):
        status = self.proxy.sharding.remove_shard(1)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard).")

    def test_remove_sharding_specification_wrong_key_exception(self):
        status = self.proxy.sharding.remove_shard(55500)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard).")

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_mapping("db1.t1")
        expected_output = {"shard_mapping_id":1,
                                     "table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID1"}
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        self.assertEqual(status["return"], expected_output)

    def test_lookup_shard_mapping_empty(self):
        status = self.proxy.sharding.lookup_mapping("Wrong")
        expected_output = {"shard_mapping_id":"",
                                     "table_name":"",
                                     "column_name":"",
                                     "type_name":"",
                                     "global_group":""}
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        self.assertEqual(status["return"], expected_output)

    def test_list(self):
        status = self.proxy.sharding.list_mappings("RANGE")
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        self.assertEqual(status["return"], [{"shard_mapping_id":1,
                                     "table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID1"}])

    def test_list_exception(self):
        status = self.proxy.sharding.list_mappings("Wrong")
        self.assertEqual(status["success"], False)
        self.assertEqual(status["message"], "Error:Invalid Sharding Type Wrong")
        self.assertEqual(status["return"], False)


    def test_lookup(self):
        expected_server_list = [
                        [str(self.__server_3.uuid),
                         MySQLInstances().get_address(2),
                         True],
                        [str(self.__server_4.uuid),
                         MySQLInstances().get_address(3),
                         False]
                        ]
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        obtained_server_list = status["return"]
        expected_uuid_list = [expected_server_list[0][0],
                              expected_server_list[1][0]]
        obtained_uuid_list = [obtained_server_list[0][0],
                              obtained_server_list[1][0]]

        expected_address_list = [expected_server_list[0][1],
                                expected_server_list[1][1]]
        obtained_address_list = [obtained_server_list[0][1],
                                obtained_server_list[1][1]]

        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

    def test_lookup_first_key(self):
        expected_server_list = [
                        [str(self.__server_3.uuid),
                         MySQLInstances().get_address(2),
                         True],
                        [str(self.__server_4.uuid),
                         MySQLInstances().get_address(3),
                         False]
                        ]
        status = self.proxy.sharding.lookup_servers("db1.t1", 1, "LOCAL")
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        obtained_server_list = status["return"]
        expected_uuid_list = [expected_server_list[0][0],
                              expected_server_list[1][0]]
        obtained_uuid_list = [obtained_server_list[0][0],
                              obtained_server_list[1][0]]

        expected_address_list = [expected_server_list[0][1],
                                expected_server_list[1][1]]
        obtained_address_list = [obtained_server_list[0][1],
                                obtained_server_list[1][1]]

        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

    def test_lookup_last_key(self):
        expected_server_list = [
                        [str(self.__server_3.uuid),
                         MySQLInstances().get_address(2),
                         True],
                        [str(self.__server_4.uuid),
                         MySQLInstances().get_address(3),
                         False]
                        ]
        status = self.proxy.sharding.lookup_servers("db1.t1", 1000, "LOCAL")
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        obtained_server_list = status["return"]
        expected_uuid_list = [expected_server_list[0][0],
                              expected_server_list[1][0]]
        obtained_uuid_list = [obtained_server_list[0][0],
                              obtained_server_list[1][0]]

        expected_address_list = [expected_server_list[0][1],
                                expected_server_list[1][1]]
        obtained_address_list = [obtained_server_list[0][1],
                                obtained_server_list[1][1]]

        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

    def test_lookup_first_in_next_key(self):
        expected_server_list = [
                        [str(self.__server_5.uuid),
                         MySQLInstances().get_address(4),
                         True],
                        [str(self.__server_6.uuid),
                         MySQLInstances().get_address(5),
                         False]
                        ]
        status = self.proxy.sharding.lookup_servers("db1.t1", 1001, "LOCAL")
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        obtained_server_list = status["return"]
        expected_uuid_list = [expected_server_list[0][0],
                              expected_server_list[1][0]]
        obtained_uuid_list = [obtained_server_list[0][0],
                              obtained_server_list[1][0]]

        expected_address_list = [expected_server_list[0][1],
                                expected_server_list[1][1]]
        obtained_address_list = [obtained_server_list[0][1],
                                obtained_server_list[1][1]]

        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

    def test_lookup_disabled_exception(self):
        self.proxy.sharding.disable_shard(1)
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.assertEqual(status["success"], False)
        self.assertEqual(status["message"], "Error:Shard not enabled")
        self.assertEqual(status["return"], False)


    def test_lookup_wrong_table_exception(self):
        status = self.proxy.sharding.lookup_servers("Wrong", 500, "LOCAL")
        self.assertEqual(status["success"], False)
        self.assertEqual(status["message"], "Error:Table name Wrong not found")
        self.assertEqual(status["return"], False)

    def test_lookup_wrong_key_exception(self):
        expected_server_list = [
                        [str(self.__server_5.uuid),
                         MySQLInstances().get_address(4),
                         True],
                        [str(self.__server_6.uuid),
                         MySQLInstances().get_address(5),
                         False]
                        ]
        status = self.proxy.sharding.lookup_servers("db1.t1", 55500, "LOCAL")
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        obtained_server_list = status["return"]
        expected_uuid_list = [expected_server_list[0][0],
                              expected_server_list[1][0]]
        obtained_uuid_list = [obtained_server_list[0][0],
                              obtained_server_list[1][0]]

        expected_address_list = [expected_server_list[0][1],
                                expected_server_list[1][1]]
        obtained_address_list = [obtained_server_list[0][1],
                                obtained_server_list[1][1]]

        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

    def test_list_shard_mappings(self):
        expected_shard_mapping_list1 =   [1, "RANGE", "GROUPID1"]
        status = self.proxy.sharding.list_definitions()
        self.assertEqual(status["success"], True)
        self.assertEqual(status["message"], False)
        obtained_shard_mapping_list = status["return"]
        self.assertEqual(set(expected_shard_mapping_list1),  set(obtained_shard_mapping_list[0]))

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

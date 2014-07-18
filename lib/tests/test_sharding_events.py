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
import uuid as _uuid
import tests.utils

from mysql.fabric import executor as _executor
from mysql.fabric.server import (
    Group,
    MySQLServer,
)
from tests.utils import MySQLInstances

class TestShardingServices(tests.utils.TestCase):
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server1 = MySQLServer.discover_uuid(self.__options_1["address"])
        self.__options_1["uuid"] = _uuid.UUID(uuid_server1)
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__server_1)

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(1),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server2 = MySQLServer.discover_uuid(self.__options_2["address"])
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
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{bb45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(3),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server4 = MySQLServer.discover_uuid(self.__options_4["address"])
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
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server5 = MySQLServer.discover_uuid(self.__options_5["address"])
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{cc45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(5),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server6 = MySQLServer.discover_uuid(self.__options_6["address"])
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)

        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add( self.__group_3)
        self.__group_3.add_server(self.__server_5)
        self.__group_3.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_5)

        status = self.proxy.sharding.create_definition("RANGE", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID1")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(1, "GROUPID2/0,GROUPID3/1001",
            "ENABLED")
        self.check_xmlrpc_command_result(status)

        self.__group_4 = Group("GROUPID4", "FOURTH DUMMY DESCRIPTION")
        Group.add(self.__group_4)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_fail_duplicate_add_shard_mapping_id(self):
        """Verify that a duplicate shard mapping ID inserted
        results in an error.
        """
        status = self.proxy.sharding.add_table(1, "db1.t1", "userID1")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_fail_non_existent_shard_mapping_id(self):
        """Verify that adding a table to a shard mapping ID that does not
        exist does not pass.
        """
        status = self.proxy.sharding.add_table(3, "db2.t2", "userID2")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_fail_duplicate_add_shard(self):
        """Tests that addition of an existing lower_bound to a
        shard mapping fails. Also test adding lower_bounds with
        a 0 pre-pended the value being inserted.
        """
        status = self.proxy.sharding.add_shard(1, "GROUPID2/0", "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

        #Since the lower_bound datatype is a VARBINARY, ensure that
        #pre-pending a 00 to the lower bound does not result in adding
        #the same values in the data store. This basically shows that
        #comparisons of integers are not impacted with a 00 pre-pended
        #to the values.
        status = self.proxy.sharding.add_shard(1, "GROUPID3/001001", "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

        #Since the lower_bound datatype is a VARBINARY, ensure that
        #pre-pending a 0000 to the lower bound does not result in adding
        #the same values in the data store. This basically shows that
        #comparisons of integers are not impacted with a 00 pre-pended
        #to the values.
        status = self.proxy.sharding.add_shard(1, "GROUPID3/00001001",
                "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_define_shard_mapping_wrong_sharding_type(self):
        #Use an invalid sharding type (WRONG)
        status = self.proxy.sharding.create_definition("WRONG", "GROUPID4")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_add_shard_invalid_group_exception(self):
        #Use an invalid group ID (WRONG_GROUP)
        status = self.proxy.sharding.add_shard(4, "WRONG_GROUP/8001",
                                               "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_add_shard_invalid_state_exception(self):
        #WRONG_STATE is an invalid description of the shard state.
        status = self.proxy.sharding.add_shard(4, "GROUP4/8001", "WRONG_STATE")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_add_shard_invalid_range_exception(self):
        #Notice LB > UB in the case below.
        status = self.proxy.sharding.add_shard(4, "GROUP4/9000", "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_add_shard_invalid_shard_mapping(self):
        status = self.proxy.sharding.add_shard(25000, "GROUPID4/8001",
                                               "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

        status = self.proxy.sharding.add_shard(25000, "GROUPID4/8001",
                                               "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)


    def test_remove_shard_mapping(self):
        #Remove the shards before removing the mapping
        self.proxy.sharding.disable_shard("1")
        self.proxy.sharding.remove_shard("1")
        self.proxy.sharding.disable_shard("2")
        self.proxy.sharding.remove_shard("2")
        status = self.proxy.sharding.remove_table("db1.t1")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_table("db1.t1")
        info = self.check_xmlrpc_simple(status, {})
        self.assertEqual(info, {
            "mapping_id" : "",
            "table_name" : "",
            "column_name" : "",
            "type_name" : "",
            "global_group":""
        })

    def test_remove_shard_mapping_shards_exist_exception(self):
        status = self.proxy.sharding.remove_table("db1.t1")
        self.check_xmlrpc_command_result(status)

    def test_remove_shard_mapping_exception(self):
        status = self.proxy.sharding.remove_table("Wrong")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_remove_sharding_specification(self):
        status = self.proxy.sharding.disable_shard(1)
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.remove_shard(1)
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_remove_sharding_specification_exception(self):
        status = self.proxy.sharding.remove_shard(1)
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_remove_sharding_specification_wrong_key_exception(self):
        status = self.proxy.sharding.remove_shard(55500)
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_table("db1.t1")
        expected_output = {
            "mapping_id" : 1,
            "table_name" : "db1.t1",
            "column_name" : "userID1",
            "type_name" : "RANGE",
            "global_group" : "GROUPID1"
        }
        self.check_xmlrpc_simple(status, expected_output)

    def test_lookup_shard_mapping_empty(self):
        status = self.proxy.sharding.lookup_table("Wrong")
        expected_output = {
            "mapping_id" : "",
            "table_name" : "",
            "column_name" : "",
            "type_name": "",
            "global_group": ""
        }
        self.check_xmlrpc_simple(status, expected_output)

    def test_list(self):
        status = self.proxy.sharding.list_tables("RANGE")
        expected_output = {
            "mapping_id" : 1,
            "table_name" : "db1.t1",
            "column_name" : "userID1",
            "type_name" : "RANGE",
            "global_group":"GROUPID1"
        }
        self.check_xmlrpc_simple(status, expected_output)

    def test_list_exception(self):
        status = self.proxy.sharding.list_tables("Wrong")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_lookup(self):
        expected_uuid_list = [
            str(self.__server_3.uuid),
            str(self.__server_4.uuid)
        ]

        expected_address_list = [
            MySQLInstances().get_address(2),
            MySQLInstances().get_address(3)
        ]

        # Lookup any key
        obtained_uuid_list = []
        obtained_address_list = []
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        for row in self.check_xmlrpc_iter(status):
            obtained_uuid_list.append(row['server_uuid'])
            obtained_address_list.append(row['address'])
        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

        # Lookup first key
        obtained_uuid_list = []
        obtained_address_list = []
        status = self.proxy.sharding.lookup_servers("db1.t1", 1, "LOCAL")
        for row in self.check_xmlrpc_iter(status):
            obtained_uuid_list.append(row['server_uuid'])
            obtained_address_list.append(row['address'])
        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))


        # Lookup last key
        obtained_uuid_list = []
        obtained_address_list = []
        status = self.proxy.sharding.lookup_servers("db1.t1", 1000, "LOCAL")
        for row in self.check_xmlrpc_iter(status):
            obtained_uuid_list.append(row['server_uuid'])
            obtained_address_list.append(row['address'])
        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

        # Lookup first in next key.
        expected_uuid_list = [
            str(self.__server_5.uuid),
            str(self.__server_6.uuid)
        ]

        expected_address_list = [
            MySQLInstances().get_address(4),
            MySQLInstances().get_address(5)
        ]

        obtained_uuid_list = []
        obtained_address_list = []
        status = self.proxy.sharding.lookup_servers("db1.t1", 1001, "LOCAL")
        for row in self.check_xmlrpc_iter(status):
            obtained_uuid_list.append(row['server_uuid'])
            obtained_address_list.append(row['address'])
        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

        # Lookup wrong table
        status = self.proxy.sharding.lookup_servers("Wrong", 500, "LOCAL")
        self.check_xmlrpc_command_result(status, has_error=True)

        # Lookup wrong key
        status = self.proxy.sharding.lookup_servers("db1.t1", 55500, "LOCAL")
        for row in self.check_xmlrpc_iter(status):
            obtained_uuid_list.append(row['server_uuid'])
            obtained_address_list.append(row['address'])
        self.assertEqual(set(expected_uuid_list), set(obtained_uuid_list))
        self.assertEqual(set(expected_address_list), set(obtained_address_list))

    def test_lookup_disabled_exception(self):
        self.proxy.sharding.disable_shard(1)
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_list_shard_mappings(self):
        expected_shard_mapping = {
            'mapping_id' : 1,
            'type_name' : 'RANGE',
            'global_group_id' : 'GROUPID1'
        }
        status = self.proxy.sharding.list_definitions()
        info = self.check_xmlrpc_simple(status, {})
        self.assertEqual(info, expected_shard_mapping)

    def test_enable_shard_exception(self):
        status = self.proxy.sharding.enable_shard(25000)
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_disable_shard_exception(self):
        status = self.proxy.sharding.disable_shard(25000)
        self.check_xmlrpc_command_result(status, has_error=True)

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
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server1 = MySQLServer.discover_uuid(self.__options_1["address"])
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
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server2 = MySQLServer.discover_uuid(self.__options_2["address"])
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
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
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
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server4 = MySQLServer.discover_uuid(self.__options_4["address"])
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
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server5 = MySQLServer.discover_uuid(self.__options_5["address"])
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
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server6 = MySQLServer.discover_uuid(self.__options_6["address"])
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)

        self.__group_6 = Group("GROUPID6", "Sixth description.")
        Group.add( self.__group_6)
        self.__group_6.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_6, self.__server_6)

        status = self.proxy.sharding.create_definition("HASH", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID1")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(1, "GROUPID2,GROUPID3,"
                "GROUPID4,GROUPID5,GROUPID6", "ENABLED")
        self.check_xmlrpc_command_result(status)

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

    def test_add_shard_cannot_add_when_shards_exist_exception(self):
        status = self.proxy.sharding.add_shard(1, "NON_EXISTENT_GROUP",
            "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_add_shard_invalid_group_exception(self):
        #Remove all the existing shards in the system. Since add shard
        #shard will not be allowed when shards exist in the system
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

        #Use an invalid group ID (WRONG_GROUP)
        status = self.proxy.sharding.add_shard(4, "WRONG_GROUP", "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_add_shard_invalid_state_exception(self):
        #Remove all the existing shards in the system. Since add shard
        #shard will not be allowed when shards exist in the system
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

        #WRONG_STATE is an invalid description of the shard state.
        status = self.proxy.sharding.add_shard(4, "GROUP4", "WRONG_STATE")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_add_shard_invalid_shard_mapping(self):
        status = self.proxy.sharding.add_shard(25000, "GROUPID4", "ENABLED")
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_remove_shard_mapping_shards_exist(self):
        #It is not an error to remove tables from a shard mapping.
        status = self.proxy.sharding.remove_table("db1.t1")
        self.check_xmlrpc_command_result(status)

    def test_remove_sharding_specification(self):
        status = self.proxy.sharding.disable_shard(1)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.remove_shard(1)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.disable_shard(2)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.remove_shard(2)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.disable_shard(3)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.remove_shard(3)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.disable_shard(4)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.remove_shard(4)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.disable_shard(5)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.sharding.remove_shard(5)
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.check_xmlrpc_simple(status, {}, has_error=True)

    def test_remove_sharding_specification_exception(self):
        status = self.proxy.sharding.remove_shard(1)
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_table("db1.t1")
        self.check_xmlrpc_simple(status, {
            "shard_mapping_id": 1,
            "table_name": "db1.t1",
            "column_name": "userID1",
            "type_name": "HASH",
            "global_group": "GROUPID1"
        })

    def test_list(self):
        status = self.proxy.sharding.list_tables("HASH")
        self.check_xmlrpc_simple(status, {
            "shard_mapping_id": 1,
            "table_name": "db1.t1",
            "column_name": "userID1",
            "type_name": "HASH",
            "global_group": "GROUPID1"
        })

    def test_list_exception(self):
        status = self.proxy.sharding.list_tables("Wrong")
        self.check_xmlrpc_simple(status, {}, has_error=True)

    def test_lookup(self):
        shard_hit_count = {}

        for i in range(1,  200):
            status = self.proxy.sharding.lookup_servers("db1.t1", i, "LOCAL")
            for info in self.check_xmlrpc_iter(status):
                uuid = info['server_uuid']
                if uuid not in shard_hit_count:
                    shard_hit_count[uuid] = 0
                shard_hit_count[uuid] += 1

        self.assertEqual(len(shard_hit_count), 5)
        for key, count in shard_hit_count.items():
            self.assertTrue(count > 0)

    def test_lookup_disabled_exception(self):
        #Since inhashing it is difficult to guess which shard the value will
        #fall into, disable both the shards.
        self.proxy.sharding.disable_shard(1)
        self.proxy.sharding.disable_shard(2)
        self.proxy.sharding.disable_shard(3)
        self.proxy.sharding.disable_shard(4)
        self.proxy.sharding.disable_shard(5)
        status = self.proxy.sharding.lookup_servers("db1.t1", 500, "LOCAL")
        self.check_xmlrpc_simple(status, {}, has_error=True)


    def test_lookup_wrong_table_exception(self):
        status = self.proxy.sharding.lookup_servers("Wrong", 500, "LOCAL")
        self.check_xmlrpc_simple(status, {}, has_error=True)

    def test_list_shard_mappings(self):
        status = self.proxy.sharding.list_definitions()
        self.check_xmlrpc_simple(status, {
            'mapping_id': 1, 
            'type_name': "HASH",
            'global_group_id': "GROUPID1",
        }, rowcount=1)

    def test_enable_shard_exception(self):
        status = self.proxy.sharding.enable_shard(25000)
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_disable_shard_exception(self):
        status = self.proxy.sharding.disable_shard(25000)
        self.check_xmlrpc_command_result(status, has_error=True)

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

from mysql.fabric.sharding import (
    ShardMapping,
    RangeShardingSpecification,
    Shards,
)
from mysql.fabric.server import (
    Group,
    MySQLServer,
)
from mysql.fabric import server_utils
from tests.utils import MySQLInstances

class TestSharding(unittest.TestCase):
    """Test dump interface associated to sharding.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        self.__options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_1.mysql.com:3060",
        }
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__server_1)
        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_2.mysql.com:3060",
        }
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)
        self.__group_1 = Group("GROUPID1", "First description.")
        Group.add(self.__group_1)
        self.__group_1.add_server(self.__server_1)
        self.__group_1.add_server(self.__server_2)

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }
        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add(self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{dd75a12a-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_4.mysql.com:3060",
        }
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)
        self.__group_2 = Group("GROUPID2", "Second description.")
        Group.add(self.__group_2)
        self.__group_2.add_server(self.__server_3)
        self.__group_2.add_server(self.__server_4)
        tests.utils.configure_decoupled_master(self.__group_2, self.__server_3)

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{ee75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }
        uuid_server5 = MySQLServer.discover_uuid(self.__options_5["address"])
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{ff75a12a-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_6.mysql.com:3060",
        }
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)
        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add(self.__group_3)
        self.__group_3.add_server(self.__server_5)
        self.__group_3.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_5)

        self.__options_1_host,  self.__options_1_port = \
            server_utils.split_host_port(self.__options_1["address"], 13001)
        self.__options_2_host,  self.__options_2_port = \
            server_utils.split_host_port(self.__options_2["address"], 13001)
        self.__options_3_host,  self.__options_3_port = \
            server_utils.split_host_port(self.__options_3["address"], 13001)
        self.__options_4_host,  self.__options_4_port = \
            server_utils.split_host_port(self.__options_4["address"], 13001)
        self.__options_5_host,  self.__options_5_port = \
            server_utils.split_host_port(self.__options_5["address"], 13001)
        self.__options_6_host,  self.__options_6_port = \
            server_utils.split_host_port(self.__options_6["address"], 13001)

        group_4 = Group("GROUPID4", "4TH description.")
        Group.add(group_4)
        group_5 = Group("GROUPID5", "5TH description.")
        Group.add(group_5)
        group_6 = Group("GROUPID6", "6TH description.")
        Group.add(group_6)
        group_7 = Group("GROUPID7", "7TH description.")
        Group.add(group_7)
        group_8 = Group("GROUPID8", "8TH description.")
        Group.add(group_8)
        group_9 = Group("GROUPID9", "9TH description.")
        Group.add(group_9)
        group_10 = Group("GROUPID10", "10TH description.")
        Group.add(group_10)
        group_11 = Group("GROUPID11", "11TH description.")
        Group.add(group_11)
        group_12 = Group("GROUPID12", "12TH description.")
        Group.add(group_12)
        group_13 = Group("GROUPID13", "13TH description.")
        Group.add(group_13)
        group_14 = Group("GROUPID14", "14TH description.")
        Group.add(group_14)

        self.__shard_mapping_list = ShardMapping.list_shard_mapping_defn()
        self.assertEquals( self.__shard_mapping_list,  [])
        self.__shard_mapping_id_1 = ShardMapping.define("RANGE", "GROUPID10")
        self.__shard_mapping_id_2 = ShardMapping.define("RANGE", "GROUPID11")
        self.__shard_mapping_id_3 = ShardMapping.define("RANGE", "GROUPID12")
        #Test with sharding type values in lower case
        self.__shard_mapping_id_4 = ShardMapping.define("range", "GROUPID13")
        self.__shard_mapping_id_5 = ShardMapping.define("range", "GROUPID14")

        self.__shard_mapping_1 = \
            ShardMapping.add(self.__shard_mapping_id_1, "db1.t1", "userID1")
        self.__shard_mapping_2 = \
            ShardMapping.add(self.__shard_mapping_id_2, "db2.t2", "userID2")
        self.__shard_mapping_3 = \
            ShardMapping.add(self.__shard_mapping_id_3, "db3.t3", "userID3")
        self.__shard_mapping_4 = \
            ShardMapping.add(self.__shard_mapping_id_4, "db4.t4", "userID4")

        self.__shard_mapping_5 = \
            ShardMapping.add(self.__shard_mapping_id_5, "prune_db.prune_table",
                             "userID")

        self.__shard_id_1 = Shards.add("GROUPID1", "ENABLED")
        self.__shard_id_2 = Shards.add("GROUPID10", "ENABLED")
        self.__shard_id_3 = Shards.add("GROUPID11", "DISABLED")
        self.__shard_id_4 = Shards.add("GROUPID4", "ENABLED")
        self.__shard_id_5 = Shards.add("GROUPID5", "ENABLED")
        self.__shard_id_6 = Shards.add("GROUPID6", "ENABLED")
        self.__shard_id_7 = Shards.add("GROUPID7", "ENABLED")
        self.__shard_id_8 = Shards.add("GROUPID8", "ENABLED")
        self.__shard_id_9 = Shards.add("GROUPID9", "ENABLED")
        self.__shard_id_10 = Shards.add("GROUPID2", "ENABLED")
        self.__shard_id_11 = Shards.add("GROUPID3", "ENABLED")

        self.__range_sharding_specification_1 = RangeShardingSpecification.add(
            self.__shard_mapping_1.shard_mapping_id,
            0,
            self.__shard_id_1.shard_id
        )
        self.__range_sharding_specification_2 = RangeShardingSpecification.add(
            self.__shard_mapping_1.shard_mapping_id,
            1001,
            self.__shard_id_2.shard_id
        )
        self.__range_sharding_specification_3 = RangeShardingSpecification.add(
            self.__shard_mapping_1.shard_mapping_id,
            2001,
            self.__shard_id_3.shard_id
        )

        self.__range_sharding_specification_4 = RangeShardingSpecification.add(
            self.__shard_mapping_2.shard_mapping_id,
            3001,
            self.__shard_id_4.shard_id
        )
        self.__range_sharding_specification_5 = RangeShardingSpecification.add(
            self.__shard_mapping_2.shard_mapping_id,
            4001,
            self.__shard_id_5.shard_id
        )

        self.__range_sharding_specification_6 = RangeShardingSpecification.add(
            self.__shard_mapping_3.shard_mapping_id,
            6001,
            self.__shard_id_6.shard_id
        )
        self.__range_sharding_specification_7 = RangeShardingSpecification.add(
            self.__shard_mapping_3.shard_mapping_id,
            7001,
            self.__shard_id_7.shard_id
        )

        self.__range_sharding_specification_8 = RangeShardingSpecification.add(
            self.__shard_mapping_4.shard_mapping_id,
            8001,
            self.__shard_id_8.shard_id
        )
        self.__range_sharding_specification_9 = RangeShardingSpecification.add(
            self.__shard_mapping_4.shard_mapping_id,
           10001,
            self.__shard_id_9.shard_id
        )

        self.__range_sharding_specification_10 = RangeShardingSpecification.add(
            self.__shard_mapping_5.shard_mapping_id,
            100,
            self.__shard_id_10.shard_id)
        self.__range_sharding_specification_11 = RangeShardingSpecification.add(
            self.__shard_mapping_5.shard_mapping_id,
            201,
            self.__shard_id_11.shard_id)

        READ_ONLY = MySQLServer.get_mode_idx(MySQLServer.READ_ONLY)
        READ_WRITE = MySQLServer.get_mode_idx(MySQLServer.READ_WRITE)

        SECONDARY = MySQLServer.get_status_idx(MySQLServer.SECONDARY)
        PRIMARY = MySQLServer.get_status_idx(MySQLServer.PRIMARY)

        self.__setofservers = [0, 0, 0,
            [[str(self.__server_1.uuid),
            'GROUPID1', self.__options_1_host,  self.__options_1_port,
            READ_ONLY, SECONDARY, 1.0],
            [str(self.__server_2.uuid),
            'GROUPID1', self.__options_2_host,  self.__options_2_port,
            READ_ONLY, SECONDARY, 1.0],
            [str(self.__server_3.uuid),
            'GROUPID2', self.__options_3_host,  self.__options_3_port,
            READ_WRITE, PRIMARY, 1.0],
            [str(self.__server_4.uuid),
            'GROUPID2', self.__options_4_host,  self.__options_4_port,
            READ_ONLY, SECONDARY, 1.0],
            [str(self.__server_5.uuid),
            'GROUPID3', self.__options_5_host,  self.__options_5_port,
            READ_WRITE, PRIMARY, 1.0],
            [str(self.__server_6.uuid),
            'GROUPID3', self.__options_6_host,  self.__options_6_port,
            READ_ONLY, SECONDARY, 1.0]]]

        self.__setofservers_1 = [0, 0, 0,
                [[str(self.__server_1.uuid),
                'GROUPID1', self.__options_1_host,  self.__options_1_port,
                READ_ONLY, SECONDARY, 1.0],
                [str(self.__server_2.uuid),
                'GROUPID1', self.__options_2_host,  self.__options_2_port,
                READ_ONLY, SECONDARY, 1.0]]]

        self.__setofservers_2 = [0, 0, 0,
                [[str(self.__server_1.uuid),
                'GROUPID1', self.__options_1_host,  self.__options_1_port,
                READ_ONLY, SECONDARY, 1.0],
                [str(self.__server_2.uuid),
                'GROUPID1', self.__options_2_host,  self.__options_2_port,
                READ_ONLY, SECONDARY, 1.0],
                [str(self.__server_3.uuid),
                'GROUPID2', self.__options_3_host,  self.__options_3_port,
                READ_WRITE, PRIMARY, 1.0],
                [str(self.__server_4.uuid),
                'GROUPID2', self.__options_4_host,  self.__options_4_port,
                READ_ONLY, SECONDARY, 1.0]]]

        self.__setofservers_3 = [0, 0, 0,
            [[str(self.__server_1.uuid),
            'GROUPID1', self.__options_1_host,  self.__options_1_port,
            READ_ONLY, SECONDARY, 1.0],
            [str(self.__server_2.uuid),
            'GROUPID1', self.__options_2_host,  self.__options_2_port,
            READ_ONLY, SECONDARY, 1.0],
            [str(self.__server_3.uuid),
            'GROUPID2', self.__options_3_host,  self.__options_3_port,
            READ_WRITE, PRIMARY, 1.0],
            [str(self.__server_4.uuid),
            'GROUPID2', self.__options_4_host,  self.__options_4_port,
            READ_ONLY, SECONDARY, 1.0],
            [str(self.__server_5.uuid),
            'GROUPID3', self.__options_5_host,  self.__options_5_port,
            READ_WRITE, PRIMARY, 1.0],
            [str(self.__server_6.uuid),
            'GROUPID3', self.__options_6_host,  self.__options_6_port,
            READ_ONLY, SECONDARY, 1.0]]]

        self.__setoftables = [0, 0, 0, [['db1', 't1', 'userID1', '1'],
                              ['db2', 't2', 'userID2', '2'],
                              ['db3', 't3', 'userID3', '3'],
                              ['db4', 't4', 'userID4', '4'],
                              ['prune_db', 'prune_table', 'userID', '5']]]
        self.__setoftables_1 = [0, 0, 0, [['db1', 't1', 'userID1', '1']]]
        self.__setoftables_2 = [0, 0, 0, [['db1', 't1', 'userID1', '1'],
                                 ['db2', 't2', 'userID2', '2']]]
        self.__setoftables_3 = [0, 0, 0, [['db1', 't1', 'userID1', '1'],
                                ['db2', 't2', 'userID2', '2'],
                                ['db3', 't3', 'userID3', '3']]]
        self.__setofshardmaps = [0, 0, 0, [['1', 'RANGE', 'GROUPID10'],
                                 ['2', 'RANGE', 'GROUPID11'],
                                 ['3', 'RANGE', 'GROUPID12'],
                                 ['4', 'RANGE', 'GROUPID13'],
                                 ['5', 'RANGE', 'GROUPID14']]]
        self.__setofshardmaps_1 = [0, 0, 0, [['1', 'RANGE', 'GROUPID10']]]
        self.__setofshardmaps_2 = [0, 0, 0, [['1', 'RANGE', 'GROUPID10'],
                                 ['2', 'RANGE', 'GROUPID11']]]
        self.__setofshardmaps_3 = [0, 0, 0, [['1', 'RANGE', 'GROUPID10'],
                                 ['2', 'RANGE', 'GROUPID11'],
                                 ['3', 'RANGE', 'GROUPID12']]]
        self.__setofshardindexes = [0, 0, 0, [['0', '1', '1', 'GROUPID1'],
                                    ['1001', '1', '2', 'GROUPID10'],
                                    ['3001', '2', '4', 'GROUPID4'],
                                    ['4001', '2', '5', 'GROUPID5'],
                                    ['6001', '3', '6', 'GROUPID6'],
                                    ['7001', '3', '7', 'GROUPID7'],
                                    ['8001', '4', '8', 'GROUPID8'],
                                    ['10001', '4', '9', 'GROUPID9'],
                                    ['100', '5', '10', 'GROUPID2'],
                                    ['201', '5', '11', 'GROUPID3']]]
        self.__setofshardindexes_1 = [0, 0, 0, [['0', '1', '1', 'GROUPID1'],
                                      ['1001', '1', '2', 'GROUPID10']]]
        self.__setofshardindexes_3 = [0, 0, 0, [['0', '1', '1', 'GROUPID1'],
                                      ['1001', '1', '2', 'GROUPID10'],
                                      ['3001', '2', '4', 'GROUPID4'],
                                      ['4001', '2', '5', 'GROUPID5'],
                                          ['6001', '3', '6', 'GROUPID6'],
                                          ['7001', '3', '7', 'GROUPID7']]]
        self.__setofshardindexes_5 = [0, 0, 0, [['0', '1', '1', 'GROUPID1'],
                                      ['1001', '1', '2', 'GROUPID10'],
                                      ['3001', '2', '4', 'GROUPID4'],
                                      ['4001', '2', '5', 'GROUPID5'],
                                      ['6001', '3', '6', 'GROUPID6'],
                                      ['7001', '3', '7', 'GROUPID7'],
                                      ['8001', '4', '8', 'GROUPID8'],
                                      ['10001', '4', '9', 'GROUPID9'],
                                      ['100', '5', '10', 'GROUPID2'],
                                      ['201', '5', '11', 'GROUPID3']]]
        self.__shardinginformation_1 = [0, 0, 0, [['db1', 't1', 'userID1', '0',
                                        '1', 'RANGE', 'GROUPID1', 'GROUPID10'],
                                        ['db1', 't1', 'userID1', '1001',
                                        '2', 'RANGE', 'GROUPID10', 'GROUPID10']]]
        self.__shardinginformation_2 = [0, 0, 0, [['db1', 't1', 'userID1', '0',
                                        '1', 'RANGE', 'GROUPID1', 'GROUPID10'],
                                        ['db1', 't1', 'userID1', '1001',
                                        '2', 'RANGE', 'GROUPID10', 'GROUPID10'],
                                        ['db2', 't2', 'userID2', '3001',
                                        '4', 'RANGE', 'GROUPID4', 'GROUPID11'],
                                        ['db2', 't2', 'userID2', '4001',
                                        '5', 'RANGE', 'GROUPID5', 'GROUPID11']]]
        self.__shardinginformation_3 = [0, 0, 0, [['db1', 't1', 'userID1', '0',
                                        '1', 'RANGE', 'GROUPID1', 'GROUPID10'],
                                        ['db1', 't1', 'userID1', '1001',
                                        '2', 'RANGE', 'GROUPID10', 'GROUPID10'],
                                        ['db2', 't2', 'userID2', '3001',
                                        '4','RANGE', 'GROUPID4', 'GROUPID11'],
                                        ['db2', 't2', 'userID2', '4001',
                                        '5', 'RANGE', 'GROUPID5', 'GROUPID11'],
                                        ['db3', 't3', 'userID3', '6001',
                                        '6', 'RANGE', 'GROUPID6', 'GROUPID12'],
                                        ['db3', 't3', 'userID3', '7001',
                                        '7', 'RANGE', 'GROUPID7', 'GROUPID12']]]

    def test_dumps(self):
        """Test dump interface associated to sharding.
        """
        self.assertEqual(self.__setofservers,
            self.proxy.dump.servers(0))

        self.assertEqual(self.__setofservers_1,
            self.proxy.dump.servers(0, "GROUPID1"))
        self.assertEqual(self.__setofservers_2,
            self.proxy.dump.servers(0, "GROUPID1,GROUPID2"))
        self.assertEqual(self.__setofservers_3,
            self.proxy.dump.servers(0, "GROUPID1,GROUPID2,GROUPID3"))
        self.assertEqual(self.proxy.dump.shard_tables(0),
            self.__setoftables)
        self.assertEqual(self.proxy.dump.shard_tables(0,  "1"),
            self.__setoftables_1)
        self.assertEqual(self.proxy.dump.shard_tables(0,  "1,  2"),
                        self.__setoftables_2)
        self.assertEqual(self.proxy.dump.shard_tables(0,  "1,2,  3"),
                        self.__setoftables_3)
        self.assertEqual(self.proxy.dump.shard_maps(0),
                        self.__setofshardmaps)
        self.assertEqual(self.proxy.dump.shard_maps(0, '1'),
                        self.__setofshardmaps_1)
        self.assertEqual(self.proxy.dump.shard_maps(0, '1,  2'),
                        self.__setofshardmaps_2)
        self.assertEqual(self.proxy.dump.shard_maps(0, '1,  2,  3'),
                        self.__setofshardmaps_3)
        self.assertEqual(self.proxy.dump.shard_index(0),
                        self.__setofshardindexes)
        self.assertEqual(self.proxy.dump.shard_index(0,  "1"),
                        self.__setofshardindexes_1)
        self.assertEqual(self.proxy.dump.shard_index(0,  "1, 2,   3"),
                        self.__setofshardindexes_3)
        self.assertEqual(
                self.proxy.dump.shard_index(0,  "1, 2,   3,  4,    5"),
                self.__setofshardindexes_5
        )
        self.assertEqual(
            self.proxy.dump.sharding_information(0, "db1.t1"),
            self.__shardinginformation_1
        )
        self.assertEqual(
            self.proxy.dump.sharding_information(0, "db1.t1,   db2.t2"),
            self.__shardinginformation_2
        )
        self.assertEqual(
            self.proxy.dump.sharding_information(0, "db1.t1,   db2.t2,db3.t3"),
            self.__shardinginformation_3
        )

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

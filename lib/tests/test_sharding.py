import unittest
import uuid as _uuid
import mysql.hub.sharding as _sharding
import tests.utils as _test_utils

from mysql.hub.sharding import ShardMapping
from mysql.hub.sharding import RangeShardingSpecification
from mysql.hub.server import Group, Server

from mysql.hub.persistence import MySQLPersister
from mysql.hub import persistence

from tests.utils import ShardingUtils

class TestSharding(unittest.TestCase):

    def setUp(self):
        from __main__ import options
        persistence.init(host=options.host, port=options.port,
                         user=options.user, password=options.password)
        persistence.init_thread()
        self.__options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_1.mysql.com:3060",
        }
        self.__server_1 = Server(**self.__options_1)
        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_2.mysql.com:3060",
        }
        self.__server_2 = Server(**self.__options_2)
        self.__group_1 = Group.add("GROUPID1", "First description.")
        self.__group_1.add_server(self.__server_1)
        self.__group_1.add_server(self.__server_2)
        self.__group_1.master = self.__options_1["uuid"]

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_3.mysql.com:3060",
        }
        self.__server_3 = Server(**self.__options_3)
        self.__options_4 = {
            "uuid" :  _uuid.UUID("{dd75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_4.mysql.com:3060",
        }
        self.__server_4 = Server(**self.__options_4)
        self.__group_2 = Group.add("GROUPID2", "Second description.")
        self.__group_2.add_server(self.__server_3)
        self.__group_2.add_server(self.__server_4)
        self.__group_2.master = self.__options_3["uuid"]

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{ee75b12b-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_5.mysql.com:3060",
        }
        self.__server_5 = Server(**self.__options_5)
        self.__options_6 = {
            "uuid" :  _uuid.UUID("{ff75a12a-98d1-414c-96af-9e9d4b179678}"),
            "uri"  : "server_6.mysql.com:3060",
        }
        self.__server_6 = Server(**self.__options_6)
        self.__group_3 = Group.add("GROUPID3", "Third description.")
        self.__group_3.add_server(self.__server_5)
        self.__group_3.add_server(self.__server_6)
        self.__group_3.master = self.__options_5["uuid"]

        self.__shard_mapping_1 = ShardMapping.add("db1.t1", "userID1", "RANGE",
                                                "SM1")
        self.__shard_mapping_2 = ShardMapping.add("db2.t2", "userID2", "RANGE",
                                                 "SM2")
        self.__shard_mapping_3 = ShardMapping.add("db3.t3", "userID3", "RANGE",
                                                 "SM3")
        self.__shard_mapping_4 = ShardMapping.add("db4.t4", "userID4", "RANGE",
                                                 "SM4")

        self.__range_sharding_specification_1 = RangeShardingSpecification.add(
                                                "SM1", 0, 1000,
                                                "GROUPID1")
        self.__range_sharding_specification_2 = RangeShardingSpecification.add(
                                                "SM1", 1001, 2000,
                                                "GROUPID2")
        self.__range_sharding_specification_3 = RangeShardingSpecification.add(
                                                "SM1", 2001, 3000,
                                                "GROUPID3")

        self.__range_sharding_specification_4 = RangeShardingSpecification.add(
                                                "SM2", 3001, 4000,
                                                "GROUPID4")
        self.__range_sharding_specification_5 = RangeShardingSpecification.add(
                                                "SM2", 4001, 5000,
                                                "GROUPID5")

        self.__range_sharding_specification_6 = RangeShardingSpecification.add(
                                                "SM3", 6001, 7000,
                                                "GROUPID6")
        self.__range_sharding_specification_7 = RangeShardingSpecification.add(
                                                "SM3", 7001, 8000,
                                                "GROUPID7")

        self.__range_sharding_specification_8 = RangeShardingSpecification.add(
                                                "SM4", 8001, 9000,
                                                "GROUPID8")
        self.__range_sharding_specification_9 = RangeShardingSpecification.add(
                                                "SM4", 10001, 11000,
                                                "GROUPID9")

    def tearDown(self):
        persistence.deinit_thread()
        persistence.deinit()

    def test_fetch_shard_mapping(self):
        shard_mapping_1 = ShardMapping.fetch("db1.t1")
        shard_mapping_2 = ShardMapping.fetch("db2.t2")
        shard_mapping_3 = ShardMapping.fetch("db3.t3")
        shard_mapping_4 = ShardMapping.fetch("db4.t4")
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_1, shard_mapping_1))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_2, shard_mapping_2))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_3, shard_mapping_3))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_4, shard_mapping_4))

    def test_fetch_sharding_scheme(self):
        range_sharding_specifications = RangeShardingSpecification.fetch("SM1")

        self.assertTrue(ShardingUtils.compare_range_specifications
                        (range_sharding_specifications[0],
                         self.__range_sharding_specification_1))
        self.assertTrue(ShardingUtils.compare_range_specifications
                        (range_sharding_specifications[1],
                         self.__range_sharding_specification_2))
        self.assertTrue(ShardingUtils.compare_range_specifications
                        (range_sharding_specifications[2],
                         self.__range_sharding_specification_3))

    def test_lookup_sharding_scheme(self):
        serverid1 = RangeShardingSpecification.lookup(500,
                                                      "SM1")
        self.assertEqual(serverid1.group_id, "GROUPID1")
        serverid2 = RangeShardingSpecification.lookup(3500,
                                                      "SM2")
        self.assertEqual(serverid2.group_id, "GROUPID4")
        serverid3 = RangeShardingSpecification.lookup(6500,
                                                      "SM3")
        self.assertEqual(serverid3.group_id, "GROUPID6")

    def test_lookup(self):
        serveruuid1 = _sharding.lookup("db1.t1", 500)
        self.assertEqual(serveruuid1, str(self.__options_1["uuid"]))

    def test_go_fish_lookup(self):
        server_list = _sharding.go_fish_lookup("db1.t1")
        self.assertEqual(server_list[0], str(self.__options_1["uuid"]))
        self.assertEqual(server_list[1], str(self.__options_3["uuid"]))
        self.assertEqual(server_list[2], str(self.__options_5["uuid"]))

    def test_shard_mapping_list_mappings(self):
        shard_mappings = ShardMapping.list("RANGE")
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_1, shard_mappings[0]))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_2, shard_mappings[1]))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_3, shard_mappings[2]))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_4, shard_mappings[3]))

    def test_shard_mapping_getters(self):
        self.assertEqual(self.__shard_mapping_1.table_name, "db1.t1")
        self.assertEqual(self.__shard_mapping_1.column_name, "userID1")
        self.assertEqual(self.__shard_mapping_1.type_name, "RANGE")
        self.assertEqual(self.__shard_mapping_1.sharding_specification, "SM1")

    def test_range_sharding_specification_getters(self):
        self.assertEqual(self.__range_sharding_specification_1.
                         name, "SM1")
        self.assertEqual(self.__range_sharding_specification_1.lower_bound,
                         0)
        self.assertEqual(self.__range_sharding_specification_1.upper_bound,
                         1000)
        self.assertEqual(self.__range_sharding_specification_1.group_id,
                         "GROUPID1")

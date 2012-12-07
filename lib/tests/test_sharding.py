import unittest
import uuid as _uuid
import mysql.hub.sharding as _sharding
import tests.utils as _test_utils

from mysql.hub.sharding import ShardMapping
from mysql.hub.sharding import RangeShardingSpecification

from mysql.hub.persistence import MySQLPersister
from tests.utils import ShardingUtils

class TestSharding(unittest.TestCase):

    __metaclass__ = _test_utils.SkipTests

    def setUp(self):
        self.__persister = MySQLPersister("localhost:13000","root", "")
        ShardMapping.create(self.__persister)

        self.__shard_mapping_1 = ShardMapping.add(self.__persister,
                                               "db1.t1", "userID1", "RANGE",
                                                "SM1")
        self.__shard_mapping_2 = ShardMapping.add(self.__persister,
                                               "db2.t2", "userID2", "RANGE",
                                                 "SM2")
        self.__shard_mapping_3 = ShardMapping.add(self.__persister,
                                               "db3.t3", "userID3", "RANGE",
                                                 "SM3")
        self.__shard_mapping_4 = ShardMapping.add(self.__persister,
                                               "db4.t4", "userID4", "RANGE",
                                                 "SM4")

        RangeShardingSpecification.create(self.__persister)

        self.__range_sharding_specification_1 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM1", 0, 1000,
                                                "GROUPID1")
        self.__range_sharding_specification_2 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM1", 1001, 2000,
                                                "GROUPID2")
        self.__range_sharding_specification_3 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM1", 2001, 3000,
                                                "GROUPID3")

        self.__range_sharding_specification_4 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM2", 3001, 4000,
                                                "GROUPID4")
        self.__range_sharding_specification_5 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM2", 4001, 5000,
                                                "GROUPID5")

        self.__range_sharding_specification_6 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM3", 6001, 7000,
                                                "GROUPID6")
        self.__range_sharding_specification_7 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM3", 7001, 8000,
                                                "GROUPID7")

        self.__range_sharding_specification_8 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM4", 8001, 9000,
                                                "GROUPID8")
        self.__range_sharding_specification_9 = RangeShardingSpecification.add(
                                                self.__persister,
                                                "SM4", 10001, 11000,
                                                "GROUPID9")

    def tearDown(self):
        self.__range_sharding_specification_1.remove(self.__persister)
        self.__range_sharding_specification_2.remove(self.__persister)
        self.__range_sharding_specification_3.remove(self.__persister)
        self.__range_sharding_specification_4.remove(self.__persister)
        self.__range_sharding_specification_5.remove(self.__persister)
        self.__range_sharding_specification_6.remove(self.__persister)
        self.__range_sharding_specification_7.remove(self.__persister)
        self.__range_sharding_specification_8.remove(self.__persister)
        self.__range_sharding_specification_9.remove(self.__persister)
        self.__shard_mapping_1.remove(self.__persister)
        self.__shard_mapping_2.remove(self.__persister)
        self.__shard_mapping_3.remove(self.__persister)
        self.__shard_mapping_4.remove(self.__persister)
        RangeShardingSpecification.drop(self.__persister)
        ShardMapping.drop(self.__persister)

    def test_fetch_shard_mapping(self):
        shard_mapping_1 = ShardMapping.fetch(self.__persister,
                                             "db1.t1")
        shard_mapping_2 = ShardMapping.fetch(self.__persister,
                                             "db2.t2")
        shard_mapping_3 = ShardMapping.fetch(self.__persister,
                                             "db3.t3")
        shard_mapping_4 = ShardMapping.fetch(self.__persister,
                                             "db4.t4")
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_1, shard_mapping_1))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_2, shard_mapping_2))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_3, shard_mapping_3))
        self.assertTrue(ShardingUtils.compare_shard_mapping
                         (self.__shard_mapping_4, shard_mapping_4))

    def test_fetch_sharding_scheme(self):
        range_sharding_specifications = RangeShardingSpecification.fetch(
                                                    self.__persister,
                                                    "SM1")

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
        serverid1 = RangeShardingSpecification.lookup(self.__persister,
                                                      500,
                                                      "SM1")
        self.assertEqual(serverid1.uuid, "GROUPID1")
        serverid2 = RangeShardingSpecification.lookup(self.__persister,
                                                      3500,
                                                      "SM2")
        self.assertEqual(serverid2.uuid, "GROUPID4")
        serverid3 = RangeShardingSpecification.lookup(self.__persister,
                                                      6500,
                                                      "SM3")
        self.assertEqual(serverid3.uuid, "GROUPID6")

    def test_lookup(self):
        serveruuid1 = _sharding.lookup(self.__persister, "db1.t1", 500)
        self.assertEqual(serveruuid1, "GROUPID1")
        serveruuid2 = _sharding.lookup(self.__persister, "db2.t2",
                                       3500)
        self.assertEqual(serveruuid2, "GROUPID4")
        serveruuid3 = _sharding.lookup(self.__persister, "db3.t3",
                                       6500)
        self.assertEqual(serveruuid3, "GROUPID6")

    def test_go_fish_lookup(self):
        server_list = _sharding.go_fish_lookup(self.__persister,
                                               "db1.t1")
        self.assertEqual(server_list[0], "GROUPID1")
        self.assertEqual(server_list[1], "GROUPID2")
        self.assertEqual(server_list[2], "GROUPID3")

    def test_shard_mapping_list_mappings(self):
        shard_mappings = ShardMapping.list(self.__persister, "RANGE")
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
        self.assertEqual(self.__range_sharding_specification_1.uuid,
                         "GROUPID1")

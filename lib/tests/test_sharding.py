import unittest
import uuid as _uuid
import mysql.fabric.sharding as _sharding
import mysql.fabric.errors as _errors
import tests.utils

from mysql.fabric.sharding import ShardMapping, RangeShardingSpecification, Shards
from mysql.fabric.server import Group, MySQLServer
from mysql.fabric import persistence

from tests.utils import ShardingUtils, MySQLInstances

class TestSharding(unittest.TestCase):

    def setUp(self):
        """Configure the existing environment
        """
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
        self.__group_1.master = self.__options_1["uuid"]

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : "root"
        }
        uuid_server3 = MySQLServer.discover_uuid(**self.__options_3)
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add(self.__server_3)
        self.__server_3.connect()
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS prune_db")
        self.__server_3.exec_stmt("CREATE DATABASE prune_db")
        self.__server_3.exec_stmt("CREATE TABLE prune_db.prune_table"
                                  "(userID INT, name VARCHAR(30))")
        self.__server_3.exec_stmt("INSERT INTO prune_db.prune_table "
                                  "VALUES(101, 'TEST 1')")
        self.__server_3.exec_stmt("INSERT INTO prune_db.prune_table "
                                  "VALUES(202, 'TEST 2')")

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
        self.__group_2.master = self.__options_3["uuid"]

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{ee75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : "root"
        }
        uuid_server5 = MySQLServer.discover_uuid(**self.__options_5)
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)
        self.__server_5.connect()
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS prune_db")
        self.__server_5.exec_stmt("CREATE DATABASE prune_db")
        self.__server_5.exec_stmt("CREATE TABLE prune_db.prune_table"
                                  "(userID INT, name VARCHAR(30))")
        self.__server_5.exec_stmt("INSERT INTO prune_db.prune_table "
                                  "VALUES(101, 'TEST 1')")
        self.__server_5.exec_stmt("INSERT INTO prune_db.prune_table "
                                  "VALUES(202, 'TEST 2')")

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
        self.__group_3.master = self.__options_5["uuid"]

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

        self.__shard_mapping_1 = ShardMapping.add(self.__shard_mapping_id_1, "db1.t1", "userID1")
        self.__shard_mapping_2 = ShardMapping.add(self.__shard_mapping_id_2, "db2.t2", "userID2")
        self.__shard_mapping_3 = ShardMapping.add(self.__shard_mapping_id_3, "db3.t3", "userID3")
        self.__shard_mapping_4 = ShardMapping.add(self.__shard_mapping_id_4, "db4.t4", "userID4")

        self.__shard_mapping_5 = ShardMapping.add(self.__shard_mapping_id_5, "prune_db.prune_table",
                                                  "userID")

        self.__shard_id_1 = Shards.add("GROUPID1")
        self.__shard_id_2 = Shards.add("GROUPID10")
        self.__shard_id_3 = Shards.add("GROUPID11")
        self.__shard_id_4 = Shards.add("GROUPID4")
        self.__shard_id_5 = Shards.add("GROUPID5")
        self.__shard_id_6 = Shards.add("GROUPID6")
        self.__shard_id_7 = Shards.add("GROUPID7")
        self.__shard_id_8 = Shards.add("GROUPID8")
        self.__shard_id_9 = Shards.add("GROUPID9")
        self.__shard_id_10 = Shards.add("GROUPID2")
        self.__shard_id_11 = Shards.add("GROUPID3")

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

    def tearDown(self):
        """Clean up the existing environment
        """
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS prune_db")
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS prune_db")
        tests.utils.cleanup_environment()

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
        range_sharding_specifications = RangeShardingSpecification.list(1)
        self.assertTrue(ShardingUtils.compare_range_specifications(
            range_sharding_specifications[0],
            self.__range_sharding_specification_1))
        self.assertTrue(ShardingUtils.compare_range_specifications(
            range_sharding_specifications[1],
            self.__range_sharding_specification_2))
        self.assertTrue(ShardingUtils.compare_range_specifications(
            range_sharding_specifications[2],
            self.__range_sharding_specification_3))

    def test_lookup_sharding_scheme(self):
        r_spec_1 = RangeShardingSpecification.lookup(500, self.__shard_mapping_id_1)
        self.assertEqual(r_spec_1.shard_id, self.__shard_id_1.shard_id)
        r_spec_2 = RangeShardingSpecification.lookup(3500, self.__shard_mapping_id_2)
        self.assertEqual(r_spec_2.shard_id, self.__shard_id_4.shard_id)
        r_spec_3 = RangeShardingSpecification.lookup(6500, self.__shard_mapping_id_3)
        self.assertEqual(r_spec_3.shard_id, self.__shard_id_6.shard_id)

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
        self.assertEqual(self.__shard_mapping_1.shard_mapping_id, 1)
        self.assertEqual(self.__shard_mapping_1.global_group, "GROUPID10")

    def test_shard_mapping_remove(self):
        self.__range_sharding_specification_1.remove()
        self.__range_sharding_specification_2.remove()
        self.__range_sharding_specification_3.remove()
        self.__range_sharding_specification_4.remove()
        self.__range_sharding_specification_5.remove()
        self.__range_sharding_specification_6.remove()
        self.__range_sharding_specification_7.remove()
        self.__range_sharding_specification_8.remove()
        self.__range_sharding_specification_9.remove()
        self.__range_sharding_specification_10.remove()
        self.__shard_id_1.remove()
        self.__shard_id_2.remove()
        self.__shard_id_3.remove()
        self.__shard_id_4.remove()
        self.__shard_id_5.remove()
        self.__shard_id_6.remove()
        self.__shard_id_7.remove()
        self.__shard_id_8.remove()
        self.__shard_id_9.remove()
        self.__shard_id_10.remove()
        shard_mapping_1 = ShardMapping.fetch("db1.t1")
        shard_mapping_1.remove()

    def test_range_sharding_specification_getters(self):
        self.assertEqual(
            self.__range_sharding_specification_1.shard_mapping_id, 1)
        self.assertEqual(self.__range_sharding_specification_1.lower_bound, 0)
        self.assertEqual(self.__range_sharding_specification_1.shard_id, 1)

    def test_list_shard_mapping(self):
        expected_shard_mapping_list1 =   [1, "RANGE", "GROUPID10"]
        expected_shard_mapping_list2 =   [2, "RANGE", "GROUPID11"]
        expected_shard_mapping_list3 =   [3, "RANGE", "GROUPID12"]
        expected_shard_mapping_list4 =   [4, "RANGE", "GROUPID13"]
        expected_shard_mapping_list5 =   [5, "RANGE", "GROUPID14"]

        obtained_shard_mapping_list = ShardMapping.list_shard_mapping_defn()
        self.assertEqual(set(expected_shard_mapping_list1),  set(obtained_shard_mapping_list[0]))
        self.assertEqual(set(expected_shard_mapping_list2),  set(obtained_shard_mapping_list[1]))
        self.assertEqual(set(expected_shard_mapping_list3),  set(obtained_shard_mapping_list[2]))
        self.assertEqual(set(expected_shard_mapping_list4),  set(obtained_shard_mapping_list[3]))
        self.assertEqual(set(expected_shard_mapping_list5),  set(obtained_shard_mapping_list[4]))

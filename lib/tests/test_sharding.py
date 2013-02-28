import unittest
import uuid as _uuid
import mysql.hub.sharding as _sharding
import mysql.hub.errors as _errors

from mysql.hub.sharding import ShardMapping, RangeShardingSpecification, Shards
from mysql.hub.server import Group, MySQLServer
from mysql.hub import persistence

from tests.utils import ShardingUtils, MySQLInstances

class TestSharding(unittest.TestCase):

    def setUp(self):
        from __main__ import options
        persistence.init(host=options.host, port=options.port,
                         user=options.user, password=options.password)
        persistence.setup()
        persistence.init_thread()
        self.__options_1 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_1.mysql.com:3060",
        }
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__options_1["uuid"], self.__options_1["address"],
                        None, None)
        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa75a12a-98d1-414c-96af-9e9d4b179678}"),
            "address"  : "server_2.mysql.com:3060",
        }
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__options_2["uuid"], self.__options_2["address"],
                        None, None)
        self.__group_1 = Group.add("GROUPID1", "First description.")
        self.__group_1.add_server(self.__server_1)
        self.__group_1.add_server(self.__server_2)
        self.__group_1.master = self.__options_1["uuid"]

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : "root"
        }
        self.__server_3 = MySQLServer(**self.__options_3)
        uuid_server3 = MySQLServer.discover_uuid(**self.__options_3)
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)

        MySQLServer.add(self.__options_3["uuid"], self.__options_3["address"],
                        self.__options_3["user"], None)
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
        MySQLServer.add(self.__options_4["uuid"], self.__options_4["address"],
                        None, None)
        self.__group_2 = Group.add("GROUPID2", "Second description.")
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
        MySQLServer.add(self.__options_5["uuid"], self.__options_5["address"],
                        self.__options_5["user"], None)
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
        MySQLServer.add(self.__options_6["uuid"], self.__options_6["address"],
                        None, None)
        self.__group_3 = Group.add("GROUPID3", "Third description.")
        self.__group_3.add_server(self.__server_5)
        self.__group_3.add_server(self.__server_6)
        self.__group_3.master = self.__options_5["uuid"]

        Group.add("GROUPID4", "4TH description.")
        Group.add("GROUPID5", "5TH description.")
        Group.add("GROUPID6", "6TH description.")
        Group.add("GROUPID7", "7TH description.")
        Group.add("GROUPID8", "8TH description.")
        Group.add("GROUPID9", "9TH description.")
        Group.add("GROUPID10", "10TH description.")
        Group.add("GROUPID11", "11TH description.")
        Group.add("GROUPID12", "12TH description.")
        Group.add("GROUPID13", "13TH description.")
        Group.add("GROUPID14", "14TH description.")

        self.__shard_mapping_id_1 = ShardMapping.define("RANGE", "GROUPID10")
        self.__shard_mapping_id_2 = ShardMapping.define("RANGE", "GROUPID11")
        self.__shard_mapping_id_3 = ShardMapping.define("RANGE", "GROUPID12")
        self.__shard_mapping_id_4 = ShardMapping.define("RANGE", "GROUPID13")
        self.__shard_mapping_id_5 = ShardMapping.define("RANGE", "GROUPID14")

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
                                                0, 1000,
                                                self.__shard_id_1.shard_id)
        self.__range_sharding_specification_2 = RangeShardingSpecification.add(
                                                self.__shard_mapping_1.shard_mapping_id,
                                                1001, 2000,
                                                self.__shard_id_2.shard_id)
        self.__range_sharding_specification_3 = RangeShardingSpecification.add(
                                                self.__shard_mapping_1.shard_mapping_id,
                                                2001, 3000,
                                                self.__shard_id_3.shard_id)

        self.__range_sharding_specification_4 = RangeShardingSpecification.add(
                                                self.__shard_mapping_2.shard_mapping_id,
                                                3001, 4000,
                                                self.__shard_id_4.shard_id)
        self.__range_sharding_specification_5 = RangeShardingSpecification.add(
                                                self.__shard_mapping_2.shard_mapping_id,
                                                4001, 5000,
                                                self.__shard_id_5.shard_id)

        self.__range_sharding_specification_6 = RangeShardingSpecification.add(
                                                self.__shard_mapping_3.shard_mapping_id,
                                                6001, 7000,
                                                self.__shard_id_6.shard_id)
        self.__range_sharding_specification_7 = RangeShardingSpecification.add(
                                                self.__shard_mapping_3.shard_mapping_id,
                                                7001, 8000,
                                                self.__shard_id_7.shard_id)

        self.__range_sharding_specification_8 = RangeShardingSpecification.add(
                                                self.__shard_mapping_4.shard_mapping_id,
                                                8001, 9000,
                                                self.__shard_id_8.shard_id)
        self.__range_sharding_specification_9 = RangeShardingSpecification.add(
                                                self.__shard_mapping_4.shard_mapping_id,
                                                10001, 11000,
                                                self.__shard_id_9.shard_id)

        self.__range_sharding_specification_10 = RangeShardingSpecification.add(
                                        self.__shard_mapping_5.shard_mapping_id,
                                        100, 200, self.__shard_id_10.shard_id)
        self.__range_sharding_specification_11 = RangeShardingSpecification.add(
                                                self.__shard_mapping_5.shard_mapping_id,
                                                201, 300, self.__shard_id_11.shard_id)


    def tearDown(self):
        self.__server_3.exec_stmt("DROP DATABASE prune_db")
        self.__server_5.exec_stmt("DROP DATABASE prune_db")
        persistence.deinit_thread()
        persistence.teardown()

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

    def test_fetch_shard_mapping_exception(self):
        self.assertRaises(_errors.ShardingError, ShardMapping.fetch, "Wrong")

    def test_fetch_sharding_scheme(self):
        range_sharding_specifications = RangeShardingSpecification.list(1)

        self.assertTrue(ShardingUtils.compare_range_specifications
                        (range_sharding_specifications[0],
                         self.__range_sharding_specification_1))
        self.assertTrue(ShardingUtils.compare_range_specifications
                        (range_sharding_specifications[1],
                         self.__range_sharding_specification_2))
        self.assertTrue(ShardingUtils.compare_range_specifications
                        (range_sharding_specifications[2],
                         self.__range_sharding_specification_3))

    def test_fetch_sharding_scheme_exception(self):
        self.assertRaises(_errors.ShardingError,
                          RangeShardingSpecification.list, "Wrong")

    def test_lookup_sharding_scheme(self):
        r_spec_1 = RangeShardingSpecification.lookup(500, self.__shard_mapping_id_1)
        self.assertEqual(r_spec_1.shard_id, self.__shard_id_1.shard_id)
        r_spec_2 = RangeShardingSpecification.lookup(3500, self.__shard_mapping_id_2)
        self.assertEqual(r_spec_2.shard_id, self.__shard_id_4.shard_id)
        r_spec_3 = RangeShardingSpecification.lookup(6500, self.__shard_mapping_id_3)
        self.assertEqual(r_spec_3.shard_id, self.__shard_id_6.shard_id)

    def test_lookup_sharding_scheme_exception_wrong_key(self):
        self.assertRaises(_errors.ShardingError,
                          RangeShardingSpecification.lookup, 30000, self.__shard_mapping_id_1)

    def test_lookup_sharding_scheme_exception_wrong_name(self):
        self.assertRaises(_errors.ShardingError,
                          RangeShardingSpecification.lookup, 500, 32000)

    def test_lookup(self):
        expected_server_list = [
                                ["bb75b12b-98d1-414c-96af-9e9d4b179678",
                                 "server_1.mysql.com:3060",
                                 True],
                                ["aa75a12a-98d1-414c-96af-9e9d4b179678",
                                 "server_2.mysql.com:3060",
                                 False]
                                ]

        obtained_server_list = _sharding.lookup_servers("db1.t1", 500)

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

    def test_lookup_wrong_table_exception(self):
        self.assertRaises(_errors.ShardingError,
                          _sharding.lookup_servers, "Wrong", 500)

    def test_lookup_wrong_key_exception(self):
        self.assertRaises(_errors.ShardingError,
                          _sharding.lookup_servers, "db1.t1", 55000)

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

    def test_shard_mapping_list_exceptions(self):
        self.assertRaises(_errors.ShardingError, ShardMapping.list,
                          "NOT EXISTS")

    def test_shard_mapping_getters(self):
        self.assertEqual(self.__shard_mapping_1.table_name, "db1.t1")
        self.assertEqual(self.__shard_mapping_1.column_name, "userID1")
        self.assertEqual(self.__shard_mapping_1.type_name, "RANGE")
        self.assertEqual(self.__shard_mapping_1.shard_mapping_id, 1)
        self.assertEqual(self.__shard_mapping_1.global_group, "GROUPID10")

    def test_shard_mapping_remove(self):
        shard_mapping_1 = ShardMapping.fetch("db1.t1")
        shard_mapping_1.remove()
        self.assertRaises(_errors.ShardingError, ShardMapping.fetch, "db1.t1")

    def test_range_sharding_specification_getters(self):
        self.assertEqual(self.__range_sharding_specification_1.
                         shard_mapping_id, 1)
        self.assertEqual(self.__range_sharding_specification_1.lower_bound,
                         0)
        self.assertEqual(self.__range_sharding_specification_1.upper_bound,
                         1000)
        self.assertEqual(self.__range_sharding_specification_1.shard_id,
                         1)

    def test_shard_prune(self):
        RangeShardingSpecification.delete_from_shard_db("prune_db.prune_table")
        rows = self.__server_3.exec_stmt(
                                    "SELECT NAME FROM prune_db.prune_table",
                                    {"fetch" : True})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'TEST 1')
        rows = self.__server_5.exec_stmt(
                                    "SELECT NAME FROM prune_db.prune_table",
                                    {"fetch" : True})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'TEST 2')

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

import unittest
import uuid as _uuid

import mysql.hub.executor as _executor
import mysql.hub.persistence as _persistence

from mysql.hub.server import Group, MySQLServer

import tests.utils

class TestShardingServices(unittest.TestCase):

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()
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
            "address"  : "server_3.mysql.com:3060",
        }
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add(self.__options_3["uuid"], self.__options_3["address"],
                        None, None)
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
            "address"  : "server_5.mysql.com:3060",
        }
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__options_5["uuid"], self.__options_5["address"],
                        None, None)
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


        Group.add("GROUPID4", "4th description.")
        Group.add("GROUPID5", "5th description.")
        Group.add("GROUPID6", "6th description.")
        Group.add("GROUPID7", "7th description.")
        Group.add("GROUPID8", "8th description.")
        Group.add("GROUPID9", "9th description.")
        Group.add("GROUPID10", "10th description.")
        Group.add("GROUPID11", "11th description.")
        Group.add("GROUPID12", "12th description.")
        Group.add("GROUPID13", "13th description.")
        Group.add("GROUPID14", "14th description.")

        status = self.proxy.sharding.define("RANGE", "GROUPID10")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 1)

        status = self.proxy.sharding.define("RANGE", "GROUPID11")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 2)

        status = self.proxy.sharding.define("RANGE", "GROUPID12")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 3)

        status = self.proxy.sharding.define("RANGE", "GROUPID13")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 4)

        # Add a new shard mapping.
        status = self.proxy.sharding.add_mapping(1, "db1.t1", "userID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_mapping(2, "db2.t2", "userID2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_mapping(3, "db3.t3", "userID3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_mapping(4, "db4.t4", "userID4")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")


        status = self.proxy.sharding.add_shard(1, 0, 1000, "GROUPID1",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(1, 1001, 2000, "GROUPID2",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(1, 2001, 3000, "GROUPID3",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard(2, 3001, 4000, "GROUPID4",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(2, 4001, 5000, "GROUPID5",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard(3, 6001, 7000, "GROUPID6",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(3, 7001, 8000, "GROUPID7",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard(4, 8001, 9000, "GROUPID8",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard(4, 10001, 11000, "GROUPID9",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

    def tearDown(self):
        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_remove_shard_mapping(self):
        status = self.proxy.sharding.remove_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping).")

        status = self.proxy.sharding.lookup_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_shard_mapping).")

    def test_remove_shard_mapping_exception(self):
        status = self.proxy.sharding.remove_mapping("Wrong")
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
        status = self.proxy.sharding.lookup("db1.t1", 500)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup).")

    def test_remove_sharding_specification_wrong_key_exception(self):
        status = self.proxy.sharding.remove_shard(55500)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard).")

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"shard_mapping_id":1,
                                     "table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID10"})

        status = self.proxy.sharding.lookup_mapping("db2.t2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"shard_mapping_id":2,
                                     "table_name":"db2.t2",
                                     "column_name":"userID2",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID11"})

        status = self.proxy.sharding.lookup_mapping("db3.t3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"shard_mapping_id":3,
                                     "table_name":"db3.t3",
                                     "column_name":"userID3",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID12"})

        status = self.proxy.sharding.lookup_mapping("db4.t4")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"shard_mapping_id":4,
                                     "table_name":"db4.t4",
                                     "column_name":"userID4",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID13"})

    def test_lookup_shard_mapping_exception(self):
        status = self.proxy.sharding.lookup_mapping("Wrong")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_shard_mapping).")

    def test_list(self):
        status = self.proxy.sharding.list_mappings("RANGE")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_list).")
        self.assertEqual(status[2], [{"shard_mapping_id":1,
                                     "table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID10"},
                                     {"shard_mapping_id":2,
                                     "table_name":"db2.t2",
                                     "column_name":"userID2",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID11"},
                                     {"shard_mapping_id":3,
                                     "table_name":"db3.t3",
                                     "column_name":"userID3",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID12"},
                                     {"shard_mapping_id":4,
                                     "table_name":"db4.t4",
                                     "column_name":"userID4",
                                     "type_name":"RANGE",
                                     "global_group":"GROUPID13"}])

    def test_list_exception(self):
        status = self.proxy.sharding.list_mappings("Wrong")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_list).")

    def test_lookup(self):
        expected_server_list = [
                        ["bb75b12b-98d1-414c-96af-9e9d4b179678",
                         "server_1.mysql.com:3060",
                         True],
                        ["aa75a12a-98d1-414c-96af-9e9d4b179678",
                         "server_2.mysql.com:3060",
                         False]
                        ]
        status = self.proxy.sharding.lookup("db1.t1", 500)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        obtained_server_list = status[2]
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
        status = self.proxy.sharding.lookup("Wrong", 500)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup).")

    def test_lookup_wrong_key_exception(self):
        status = self.proxy.sharding.lookup("db1.t1", 55500)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup).")
    def test_list_shard_mappings(self):
        expected_shard_mapping_list1 =   [1, "RANGE", "GROUPID10"]
        expected_shard_mapping_list2 =   [2, "RANGE", "GROUPID11"] 
        expected_shard_mapping_list3 =   [3, "RANGE", "GROUPID12"] 
        expected_shard_mapping_list4 =   [4, "RANGE", "GROUPID13"]
        status = self.proxy.sharding.list_definitions()
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_list_definitions).")
        obtained_shard_mapping_list = status[2]
        self.assertEqual(set(expected_shard_mapping_list1),  set(obtained_shard_mapping_list[0]))
        self.assertEqual(set(expected_shard_mapping_list2),  set(obtained_shard_mapping_list[1]))
        self.assertEqual(set(expected_shard_mapping_list3),  set(obtained_shard_mapping_list[2]))
        self.assertEqual(set(expected_shard_mapping_list4),  set(obtained_shard_mapping_list[3]))

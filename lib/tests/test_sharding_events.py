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


        Group.add("GROUPID4", "First description.")
        Group.add("GROUPID5", "First description.")
        Group.add("GROUPID6", "First description.")
        Group.add("GROUPID7", "First description.")
        Group.add("GROUPID8", "First description.")
        Group.add("GROUPID9", "First description.")


        status = self.proxy.sharding.add_shard("RANGE", "SM1", 0, 1000,
                                               "GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM1", 1001, 2000,
                                                "GROUPID2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM1", 2001, 3000,
                                                "GROUPID3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard("RANGE", "SM2", 3001, 4000,
                                                "GROUPID4")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM2", 4001, 5000,
                                                "GROUPID5")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard("RANGE", "SM3", 6001, 7000,
                                                "GROUPID6")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM3", 7001, 8000,
                                                "GROUPID7")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard("RANGE", "SM4", 8001, 9000,
                                                "GROUPID8")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM4", 10001, 11000,
                                                "GROUPID9")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        # Add a new shard mapping.
        status = self.proxy.sharding.add_shard_mapping("db1.t1",
                                                       "userID1",
                                                       "RANGE",
                                                       "SM1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard_mapping("db2.t2",
                                                       "userID2",
                                                       "RANGE",
                                                       "SM2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard_mapping("db3.t3",
                                                       "userID3",
                                                       "RANGE",
                                                       "SM3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard_mapping("db4.t4",
                                                       "userID4",
                                                       "RANGE",
                                                       "SM4")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

    def tearDown(self):
        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_remove_shard_mapping(self):
        status = self.proxy.sharding.remove_shard_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping).")

        status = self.proxy.sharding.lookup_shard_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_shard_mapping).")

    def test_remove_shard_mapping_exception(self):
        status = self.proxy.sharding.remove_shard_mapping("Wrong")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard_mapping).")

    def test_remove_sharding_specification(self):
        status = self.proxy.sharding.remove_shard("RANGE", "SM1", 500)
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
        status = self.proxy.sharding.remove_shard("RANGE", "SM1", 55500)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard).")

    def test_remove_sharding_specification_wrong_name_exception(self):
        status = self.proxy.sharding.remove_shard("RANGE", "Wrong", 500)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_shard).")

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_shard_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM1"})

        status = self.proxy.sharding.lookup_shard_mapping("db2.t2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db2.t2",
                                     "column_name":"userID2",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM2"})

        status = self.proxy.sharding.lookup_shard_mapping("db3.t3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db3.t3",
                                     "column_name":"userID3",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM3"})

        status = self.proxy.sharding.lookup_shard_mapping("db4.t4")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db4.t4",
                                     "column_name":"userID4",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM4"})

    def test_lookup_shard_mapping_exception(self):
        status = self.proxy.sharding.lookup_shard_mapping("Wrong")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_shard_mapping).")

    def test_list(self):
        status = self.proxy.sharding.list("RANGE")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_list).")
        self.assertEqual(status[2], [{"table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM1"},
                                     {"table_name":"db2.t2",
                                     "column_name":"userID2",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM2"},
                                     {"table_name":"db3.t3",
                                     "column_name":"userID3",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM3"},
                                     {"table_name":"db4.t4",
                                     "column_name":"userID4",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM4"}])

    def test_list_exception(self):
        status = self.proxy.sharding.list("Wrong")
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

    def test_go_fish_lookup(self):
        status = self.proxy.sharding.go_fish_lookup("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_go_fish_lookup).")
        server_list = status[2]
        GROUPID1_expected_uuid_list = [str(self.__options_1["uuid"]),
                                       str(self.__options_2["uuid"])]

        GROUPID2_expected_uuid_list = [str(self.__options_3["uuid"]),
                                       str(self.__options_4["uuid"])]

        GROUPID3_expected_uuid_list = [str(self.__options_5["uuid"]),
                                       str(self.__options_6["uuid"])]

        GROUPID1_obtained_server_list = server_list["GROUPID1"]
        GROUPID2_obtained_server_list = server_list["GROUPID2"]
        GROUPID3_obtained_server_list = server_list["GROUPID3"]

        GROUPID1_obtained_uuid_list = [GROUPID1_obtained_server_list[0][0],
                                       GROUPID1_obtained_server_list[1][0]]

        GROUPID2_obtained_uuid_list = [GROUPID2_obtained_server_list[0][0],
                                       GROUPID2_obtained_server_list[1][0]]

        GROUPID3_obtained_uuid_list = [GROUPID3_obtained_server_list[0][0],
                                       GROUPID3_obtained_server_list[1][0]]

        self.assertEqual(set(GROUPID1_expected_uuid_list),
                         set(GROUPID1_obtained_uuid_list))

        self.assertEqual(set(GROUPID2_expected_uuid_list),
                         set(GROUPID2_obtained_uuid_list))

        self.assertEqual(set(GROUPID3_expected_uuid_list),
                         set(GROUPID3_obtained_uuid_list))

    def test_go_fish_lookup_exception(self):
        status = self.proxy.sharding.go_fish_lookup("Wrong")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_go_fish_lookup).")

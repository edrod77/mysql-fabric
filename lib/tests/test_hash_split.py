import unittest
import uuid as _uuid

from mysql.fabric import (
    executor as _executor,
    errors as _errors,
    persistence as _persistence,
    sharding as _sharding,
    replication as _replication,
)

from time import time, sleep

from mysql.fabric.server import Group, MySQLServer
from mysql.fabric.sharding import ShardMapping, HashShardingSpecification, Shards

import tests.utils

from tests.utils import MySQLInstances, ShardingUtils

class TestShardSplit(unittest.TestCase):

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        """Creates the following topology for testing,

        GROUPID1 - localhost:13001, localhost:13002 - Global Group
        GROUPID2 - localhost:13003, localhost:13004 - shard 1
        GROUPID3 - localhost:13005, localhost:13006 - shard 2
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        status = self.proxy.group.create("GROUPID1", "First description.")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")
        status = self.proxy.group.add("GROUPID1", MySQLInstances().get_address(0), "root","")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        status = self.proxy.group.add("GROUPID1", MySQLInstances().get_address(1), "root","")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        status = self.proxy.group.create("GROUPID2", "Second description.")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")
        status = self.proxy.group.add("GROUPID2", MySQLInstances().get_address(2), "root","")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        status =  self.proxy.group.add("GROUPID2", MySQLInstances().get_address(3), "root","")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        status = self.proxy.group.create("GROUPID3", "Third description.")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")
        status = self.proxy.group.add("GROUPID3", MySQLInstances().get_address(4), "root","")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")
        status = self.proxy.group.add("GROUPID3", MySQLInstances().get_address(5), "root","")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_server).")

        status = self.proxy.group.promote("GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")
        status = self.proxy.group.promote("GROUPID2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")
        status = self.proxy.group.promote("GROUPID3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_change_to_candidate).")

        status = self.proxy.sharding.define("HASH", "GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 1)

        status = self.proxy.sharding.add_mapping(1, "db1.t1", "userID")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard(1, "GROUPID2",
                                               "ENABLED")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            if obtained_server_list[idx][2]:
                shard_uuid = obtained_server_list[idx][0]
                shard_server = MySQLServer.fetch(shard_uuid)
                shard_server.connect()
        shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")
        shard_server.exec_stmt("CREATE DATABASE db1")
        shard_server.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")

        for i in range(1,  100):
            shard_server.exec_stmt("INSERT INTO db1.t1 "
                                      "VALUES(%s, 'TEST %s')" % (i, i))

    def test_shard_split(self):
        split_cnt_1 = 0
        split_cnt_2 = 0
        shard_server_1 = None
        shard_server_2 = None
        expected_address_list_1 = [MySQLInstances().get_address(2), MySQLInstances().get_address(3)]
        expected_address_list_2 = [MySQLInstances().get_address(4), MySQLInstances().get_address(5)]
        status = self.proxy.sharding.split("1", "GROUPID3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables_after_split).")

        for i in range(1,  100):
            status = self.proxy.sharding.lookup_servers("db1.t1", i, "LOCAL")
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_lookup).")

            obtained_server_list = status[2]
            obtained_uuid_list = [obtained_server_list[0][0],
                                  obtained_server_list[1][0]]
            obtained_address_list = [obtained_server_list[0][1],
                                    obtained_server_list[1][1]]
            try:
                self.assertEqual(set(expected_address_list_1), set(obtained_address_list))
                split_cnt_1 = split_cnt_1 + 1
                if shard_server_1 is None:
                    shard_server_1 = MySQLServer.fetch(obtained_uuid_list[0])
            except AssertionError:
                self.assertEqual(set(expected_address_list_2), set(obtained_address_list))
                split_cnt_2 = split_cnt_2 + 1
                if shard_server_2 is None:
                    shard_server_2 = MySQLServer.fetch(obtained_uuid_list[0])

        #Ensure that both the splits have been utilized.
        self.assertTrue(split_cnt_1 > 0)
        self.assertTrue(split_cnt_2 > 0)

        shard_server_1.connect()
        shard_server_2.connect()

        row_cnt_shard_1 = shard_server_1.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )

        row_cnt_shard_2 = shard_server_2.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )

        #Ensure that the split has happened, the number of values in
        #each shard should be less than the original.
        self.assertTrue(int(row_cnt_shard_1[0][0]) < 100)
        self.assertTrue(int(row_cnt_shard_2[0][0]) < 100)

        #Ensure tha two new shard_ids have been generated.
        hash_sharding_specifications = HashShardingSpecification.list(1)
        self.assertTrue(ShardingUtils.compare_hash_specifications(
            hash_sharding_specifications[1],
            HashShardingSpecification.fetch(2)))
        self.assertTrue(ShardingUtils.compare_hash_specifications(
            hash_sharding_specifications[0],
            HashShardingSpecification.fetch(3)))

    def tearDown(self):
        self.proxy.sharding.enable_shard("2")

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "GLOBAL")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            shard_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")

        status = self.proxy.sharding.lookup_servers("db1.t1", 500,  "LOCAL")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            shard_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")
            shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")

        status = self.proxy.sharding.lookup_servers("db1.t1", 800,  "LOCAL")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        obtained_server_list = status[2]
        for idx in range(0, 2):
            shard_uuid = obtained_server_list[idx][0]
            shard_server = MySQLServer.fetch(shard_uuid)
            shard_server.connect()
            shard_server.exec_stmt("DROP DATABASE IF EXISTS global_db")
            shard_server.exec_stmt("DROP DATABASE IF EXISTS db1")

        status = self.proxy.sharding.disable_shard("2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.disable_shard("3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.remove_shard("2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")

        status = self.proxy.sharding.remove_shard("3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")

        status = self.proxy.sharding.remove_mapping("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping).")

        self.proxy.group.demote("GROUPID1")
        self.proxy.group.demote("GROUPID2")
        self.proxy.group.demote("GROUPID3")
        for group_id in ("GROUPID1", "GROUPID2", "GROUPID3"):
            status = self.proxy.group.lookup_servers(group_id)
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_lookup_servers).")
            obtained_server_list = status[2]
            status = self.proxy.group.remove(group_id, obtained_server_list[0][0])
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_remove_server).")

            status = self.proxy.group.remove(group_id, obtained_server_list[1][0])
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_remove_server).")

            status = self.proxy.group.destroy(group_id)
            self.assertStatus(status, _executor.Job.SUCCESS)
            self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
            self.assertEqual(status[1][-1]["description"],
                             "Executed action (_destroy_group).")

        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

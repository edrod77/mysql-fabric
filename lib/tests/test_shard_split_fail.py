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

import tests.utils

from tests.utils import MySQLInstances

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

        status = self.proxy.sharding.define("RANGE", "GROUPID1")
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

        status = self.proxy.sharding.add_shard(1, 0, "GROUPID2",
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
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(101, 'TEST 1')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(102, 'TEST 2')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(103, 'TEST 3')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(701, 'TEST 4')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(702, 'TEST 5')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(703, 'TEST 6')")
        shard_server.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(704, 'TEST 7')")

    def test_shard_split_invalid_split_range_failed(self):
        status = self.proxy.sharding.split("1", "GROUPID3", "ABCEDFGH")
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(
            status[1][-1]["description"],
            "Tried to execute action (_backup_source_shard)."
        )

    def tearDown(self):
        self.proxy.sharding.enable_shard("1")

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

        status = self.proxy.sharding.disable_shard("1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_disable_shard).")

        status = self.proxy.sharding.remove_shard("1")
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

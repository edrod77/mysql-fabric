import unittest
import xmlrpclib
import uuid as _uuid
import os

import mysql.hub.config as _config
import mysql.hub.executor as _executor
import tests.utils as _test_utils

from mysql.hub.persistence import MySQLPersister
from mysql.hub.sharding import ShardMapping, RangeShardingSpecification

class TestShardingServices(unittest.TestCase):

    __metaclass__ = _test_utils.SkipTests

    def setUp(self):
        params = {
                "protocol.xmlrpc": {
                "address": "localhost:" + os.getenv("HTTP_PORT", "15500")
                },
            }
        config = _config.Config(None, params, True)

        # Set up the manager
        from mysql.hub.commands.start import start
        start(config)

        # Set up the client proxy.
        url = "http://%s" % (config.get("protocol.xmlrpc", "address"),)
        self.proxy = xmlrpclib.ServerProxy(url)

        executor = _executor.Executor()
        executor.persister = MySQLPersister("localhost:13000","root", "")

        ShardMapping.create(executor.persister)

        # Add a new shard mapping.
        status = self.proxy.sharding.add_shard_mapping("db1.t1",
                                                       "userID1",
                                                       "RANGE",
                                                       "SM1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard_mapping("db2.t2",
                                                       "userID2",
                                                       "RANGE",
                                                       "SM2")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard_mapping("db3.t3",
                                                       "userID3",
                                                       "RANGE",
                                                       "SM3")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard_mapping("db4.t4",
                                                       "userID4",
                                                       "RANGE",
                                                       "SM4")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        RangeShardingSpecification.create(executor.persister)

        status = self.proxy.sharding.add_shard("RANGE", "SM1", 0, 1000,
                                               "GROUPID1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM1", 1001, 2000,
                                                "GROUPID2")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM1", 2001, 3000,
                                                "GROUPID3")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard("RANGE", "SM2", 3001, 4000,
                                                "GROUPID4")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM2", 4001, 5000,
                                                "GROUPID5")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard("RANGE", "SM3", 6001, 7000,
                                                "GROUPID6")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM3", 7001, 8000,
                                                "GROUPID7")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.add_shard("RANGE", "SM4", 8001, 9000,
                                                "GROUPID8")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")
        status = self.proxy.sharding.add_shard("RANGE", "SM4", 10001, 11000,
                                                "GROUPID9")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")


    def tearDown(self):
        self.proxy.shutdown()
        executor = _executor.Executor()
        RangeShardingSpecification.drop(executor.persister)
        ShardMapping.drop(executor.persister)

    def test_remove_shard_mapping(self):
        status = self.proxy.sharding.remove_shard_mapping("db1.t1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard_mapping).")

        status = self.proxy.sharding.lookup_shard_mapping("db1.t1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"",
                                     "column_name":"",
                                     "type_name":"",
                                     "sharding_specification":""})

    def test_remove_sharding_specification(self):
        status = self.proxy.sharding.remove_shard("RANGE", "SM1", 500)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_shard).")
        status = self.proxy.sharding.lookup("db1.t1",500)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        self.assertEqual(status[2], "")

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_shard_mapping("db1.t1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM1"})

        status = self.proxy.sharding.lookup_shard_mapping("db2.t2")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db2.t2",
                                     "column_name":"userID2",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM2"})

        status = self.proxy.sharding.lookup_shard_mapping("db3.t3")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db3.t3",
                                     "column_name":"userID3",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM3"})

        status = self.proxy.sharding.lookup_shard_mapping("db4.t4")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_shard_mapping).")
        self.assertEqual(status[2], {"table_name":"db4.t4",
                                     "column_name":"userID4",
                                     "type_name":"RANGE",
                                     "sharding_specification":"SM4"})

    def test_list(self):
        status = self.proxy.sharding.list("RANGE")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
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

    def test_lookup(self):
        status = self.proxy.sharding.lookup("db1.t1", 500)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        self.assertEqual(status[2], "GROUPID1")

        status = self.proxy.sharding.lookup("db2.t2", 4500)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        self.assertEqual(status[2], "GROUPID5")

        status = self.proxy.sharding.lookup("db3.t3", 6500)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup).")
        self.assertEqual(status[2], "GROUPID6")

    def test_go_fish_lookup(self):
        status = self.proxy.sharding.go_fish_lookup("db1.t1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_go_fish_lookup).")
        self.assertEqual(status[2], ["GROUPID1",
                                     "GROUPID2",
                                     "GROUPID3"])

        status = self.proxy.sharding.go_fish_lookup("db2.t2")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_go_fish_lookup).")
        self.assertEqual(status[2], ["GROUPID4",
                                     "GROUPID5"])

import unittest
import uuid as _uuid

from mysql.fabric import (
    executor as _executor,
    persistence as _persistence,
)

from mysql.fabric.server import Group, MySQLServer
from mysql.fabric.sharding import HashShardingSpecification

import tests.utils

from tests.utils import MySQLInstances

class TestShardingServices(unittest.TestCase):

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        """Configure the existing environment
        """
        tests.utils.cleanup_environment()
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server1 = MySQLServer.discover_uuid(self.__options_1["address"])
        self.__options_1["uuid"] = _uuid.UUID(uuid_server1)
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__server_1)

        self.__group_1 = Group("GROUPID1", "First description.")
        Group.add(self.__group_1)
        self.__group_1.add_server(self.__server_1)
        tests.utils.configure_decoupled_master(self.__group_1, self.__server_1)

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(1),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server2 = MySQLServer.discover_uuid(self.__options_2["address"])
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)
        self.__server_2.connect()
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_2.exec_stmt("CREATE DATABASE db1")
        self.__server_2.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID1 INT, name VARCHAR(30))")
        for i in range(1, 201):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_2.exec_stmt("CREATE DATABASE db2")
        self.__server_2.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID2 INT, name VARCHAR(30))")
        for i in range(201, 401):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_2.exec_stmt("CREATE DATABASE db3")
        self.__server_2.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID3 INT, name VARCHAR(30))")
        for i in range(401, 601):
            self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_2 = Group("GROUPID2", "Second description.")
        Group.add(self.__group_2)
        self.__group_2.add_server(self.__server_2)
        tests.utils.configure_decoupled_master(self.__group_2, self.__server_2)

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)
        self.__server_3.connect()
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_3.exec_stmt("CREATE DATABASE db1")
        self.__server_3.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID1 INT, name VARCHAR(30))")
        for i in range(1, 201):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_3.exec_stmt("CREATE DATABASE db2")
        self.__server_3.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID2 INT, name VARCHAR(30))")
        for i in range(201, 401):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_3.exec_stmt("CREATE DATABASE db3")
        self.__server_3.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID3 INT, name VARCHAR(30))")
        for i in range(401, 601):
            self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add( self.__group_3)
        self.__group_3.add_server(self.__server_3)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{bb45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(3),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server4 = MySQLServer.discover_uuid(self.__options_4["address"])
        self.__options_4["uuid"] = _uuid.UUID(uuid_server4)
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)
        self.__server_4.connect()
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_4.exec_stmt("CREATE DATABASE db1")
        self.__server_4.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID1 INT, name VARCHAR(30))")
        for i in range(1, 201):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_4.exec_stmt("CREATE DATABASE db2")
        self.__server_4.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID2 INT, name VARCHAR(30))")
        for i in range(201, 401):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_4.exec_stmt("CREATE DATABASE db3")
        self.__server_4.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID3 INT, name VARCHAR(30))")
        for i in range(401, 601):
            self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_4 = Group("GROUPID4", "Fourth description.")
        Group.add( self.__group_4)
        self.__group_4.add_server(self.__server_4)
        tests.utils.configure_decoupled_master(self.__group_4, self.__server_4)

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(4),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server5 = MySQLServer.discover_uuid(self.__options_5["address"])
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)
        self.__server_5.connect()
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_5.exec_stmt("CREATE DATABASE db1")
        self.__server_5.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID1 INT, name VARCHAR(30))")
        for i in range(1, 201):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_5.exec_stmt("CREATE DATABASE db2")
        self.__server_5.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID2 INT, name VARCHAR(30))")
        for i in range(201, 401):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_5.exec_stmt("CREATE DATABASE db3")
        self.__server_5.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID3 INT, name VARCHAR(30))")
        for i in range(401, 601):
            self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_5 = Group("GROUPID5", "Fifth description.")
        Group.add( self.__group_5)
        self.__group_5.add_server(self.__server_5)
        tests.utils.configure_decoupled_master(self.__group_5, self.__server_5)

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{cc45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(5),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server6 = MySQLServer.discover_uuid(self.__options_6["address"])
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)
        self.__server_6.connect()
        self.__server_6.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_6.exec_stmt("CREATE DATABASE db1")
        self.__server_6.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID1 INT, name VARCHAR(30))")
        for i in range(1, 201):
            self.__server_6.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_6.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_6.exec_stmt("CREATE DATABASE db2")
        self.__server_6.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID2 INT, name VARCHAR(30))")
        for i in range(201, 401):
            self.__server_6.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_6.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_6.exec_stmt("CREATE DATABASE db3")
        self.__server_6.exec_stmt("CREATE TABLE db3.t3"
                                  "(userID3 INT, name VARCHAR(30))")
        for i in range(401, 601):
            self.__server_6.exec_stmt("INSERT INTO db3.t3 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))

        self.__group_6 = Group("GROUPID6", "Sixth description.")
        Group.add( self.__group_6)
        self.__group_6.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_6, self.__server_6)

        status = self.proxy.sharding.create_definition("HASH", "GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_define_shard_mapping).")
        self.assertEqual(status[2], 1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_table(1, "db2.t2", "userID2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_table(1, "db3.t3", "userID3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard_mapping).")

        status = self.proxy.sharding.add_shard(
            1,
            "GROUPID2,GROUPID3,GROUPID4,GROUPID5,GROUPID6",
            "ENABLED"
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_add_shard).")

        status = self.proxy.sharding.prune_shard("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")

        status = self.proxy.sharding.prune_shard("db2.t2")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")

        status = self.proxy.sharding.prune_shard("db3.t3")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_prune_shard_tables).")

    def tearDown(self):
        """Clean up the existing environment
        """
        self.proxy.sharding.disable_shard(1)
        self.proxy.sharding.remove_shard(1)

        self.proxy.sharding.disable_shard(2)
        self.proxy.sharding.remove_shard(2)

        self.proxy.sharding.disable_shard(3)
        self.proxy.sharding.remove_shard(3)

        self.proxy.sharding.disable_shard(4)
        self.proxy.sharding.remove_shard(4)

        self.proxy.sharding.disable_shard(5)
        self.proxy.sharding.remove_shard(5)

        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_prune_lookup_shard1(self):
        '''Verify that after the prune the lookup of any pruned value in the
        shard results in looking up the same shard.
        '''
        self.proxy.sharding.prune_shard("db1.t1")
        rows =  self.__server_2.exec_stmt("SELECT userID1 FROM db1.t1",
                                          {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_1 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_1.shard_id,
                             1
            )

        rows =  self.__server_3.exec_stmt(
                                            "SELECT userID1 FROM db1.t1",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_2 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_2.shard_id,
                             2
            )

        rows =  self.__server_4.exec_stmt(
                                            "SELECT userID1 FROM db1.t1",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_3 = HashShardingSpecification.lookup(val, 1, 
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_3.shard_id,
                             3
            )

        rows =  self.__server_5.exec_stmt(
                                            "SELECT userID1 FROM db1.t1",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_4 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_4.shard_id,
                             4
            )

        rows =  self.__server_6.exec_stmt(
                                            "SELECT userID1 FROM db1.t1",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_5 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_5.shard_id,
                             5
            )

    def test_prune_lookup_shard2(self):
        '''Verify that after the prune the lookup of any pruned value in the
        shard results in looking up the same shard.
        '''
        self.proxy.sharding.prune_shard("db2.t2")
        rows =  self.__server_2.exec_stmt("SELECT userID2 FROM db2.t2",
                                          {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_1 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_1.shard_id,
                             1
            )

        rows =  self.__server_3.exec_stmt(
                                            "SELECT userID2 FROM db2.t2",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_2 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_2.shard_id,
                             2
            )

        rows =  self.__server_4.exec_stmt(
                                            "SELECT userID2 FROM db2.t2",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_3 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_3.shard_id,
                             3
            )

        rows =  self.__server_5.exec_stmt(
                                            "SELECT userID2 FROM db2.t2",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_4 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_4.shard_id,
                             4
            )

        rows =  self.__server_6.exec_stmt(
                                            "SELECT userID2 FROM db2.t2",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_5 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_5.shard_id,
                             5
            )

    def test_prune_lookup_shard3(self):
        '''Verify that after the prune the lookup of any pruned value in the
        shard results in looking up the same shard.
        '''
        self.proxy.sharding.prune_shard("db3.t3")
        rows =  self.__server_2.exec_stmt("SELECT userID3 FROM db3.t3",
                                          {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_1 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_1.shard_id,
                             1
            )

        rows =  self.__server_3.exec_stmt(
                                            "SELECT userID3 FROM db3.t3",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_2 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_2.shard_id,
                             2
            )

        rows =  self.__server_4.exec_stmt(
                                            "SELECT userID3 FROM db3.t3",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_3 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_3.shard_id,
                             3
            )

        rows =  self.__server_5.exec_stmt(
                                            "SELECT userID3 FROM db3.t3",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_4 = HashShardingSpecification.lookup(
                                    val,
                                    1,
                                     "HASH"
                                )
            self.assertEqual(
                             hash_sharding_spec_4.shard_id,
                             4
            )

        rows =  self.__server_6.exec_stmt(
                                            "SELECT userID3 FROM db3.t3",
                                            {"fetch" : True})
        for val in rows[0:len(rows)][0]:
            hash_sharding_spec_5 = HashShardingSpecification.lookup(val, 1,
                "HASH")
            self.assertEqual(
                             hash_sharding_spec_5.shard_id,
                             5
            )

    def test_list_shard_mappings(self):
        expected_shard_mapping_list1 =   [1, "HASH", "GROUPID1"]
        status = self.proxy.sharding.list_definitions()
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        obtained_shard_mapping_list = status[2]
        self.assertEqual(set(expected_shard_mapping_list1),
                         set(obtained_shard_mapping_list[0]))

    def test_lookup_shard_mapping(self):
        status = self.proxy.sharding.lookup_table("db1.t1")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        self.assertEqual(status[2], {"shard_mapping_id":1,
                                     "table_name":"db1.t1",
                                     "column_name":"userID1",
                                     "type_name":"HASH",
                                     "global_group":"GROUPID1"})

        status = self.proxy.sharding.lookup_table("db2.t2")
        self.assertEqual(status[0], True)
        self.assertEqual(status[1], "")
        self.assertEqual(status[2], {"shard_mapping_id":1,
                                     "table_name":"db2.t2",
                                     "column_name":"userID2",
                                     "type_name":"HASH",
                                     "global_group":"GROUPID1"})

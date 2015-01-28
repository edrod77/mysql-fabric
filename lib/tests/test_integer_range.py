#
# Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#

"""Tests RANGE shard splits and also the impact of the splits
on the global server.
"""

import unittest
import uuid as _uuid
import time
import tests.utils

from mysql.fabric import executor as _executor
from mysql.fabric.server import (
    Group,
    MySQLServer,
)

import mysql.fabric.errors as _errors

from tests.utils import MySQLInstances

class TestShardingPrune(tests.utils.TestCase):
    """Contains unit tests for testing the shard split operation and for
    verifying that the global server configuration remains constant after
    the shard split configuration.
    """

    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.check_xmlrpc_command_result(status)

    def setUp(self):
        """Creates the topology for testing.
        """
        tests.utils.cleanup_environment()
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd": MySQLInstances().passwd,
        }

        uuid_server1 = MySQLServer.discover_uuid(self.__options_1["address"])
        self.__options_1["uuid"] = _uuid.UUID(uuid_server1)
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__server_1)
        self.__server_1.connect()
        self.__server_1.exec_stmt("CREATE DATABASE IF NOT EXISTS db1")
        self.__server_1.exec_stmt("CREATE TABLE IF NOT EXISTS db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        self.__server_1.exec_stmt("CREATE DATABASE IF NOT EXISTS db2")
        self.__server_1.exec_stmt("CREATE TABLE IF NOT EXISTS db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")

        self.__group_1 = Group("GROUPID1", "First description.")
        Group.add(self.__group_1)
        self.__group_1.add_server(self.__server_1)
        tests.utils.configure_decoupled_master(self.__group_1, self.__server_1)

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(1),
            "user" : MySQLInstances().user,
            "passwd": MySQLInstances().passwd,
        }

        uuid_server2 = MySQLServer.discover_uuid(self.__options_2["address"])
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)
        self.__server_2.connect()
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_2.exec_stmt("CREATE DATABASE db1")
        self.__server_2.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_2.exec_stmt("CREATE DATABASE db2")
        self.__server_2.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))


        self.__group_2 = Group("GROUPID2", "Second description.")
        Group.add(self.__group_2)
        self.__group_2.add_server(self.__server_2)
        tests.utils.configure_decoupled_master(self.__group_2, self.__server_2)

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{bb75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(2),
            "user" : MySQLInstances().user,
            "passwd": MySQLInstances().passwd,
        }

        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        MySQLServer.add( self.__server_3)
        self.__server_3.connect()
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_3.exec_stmt("CREATE DATABASE db1")
        self.__server_3.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_3.exec_stmt("CREATE DATABASE db2")
        self.__server_3.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))

        self.__group_3 = Group("GROUPID3", "Third description.")
        Group.add( self.__group_3)
        self.__group_3.add_server(self.__server_3)
        tests.utils.configure_decoupled_master(self.__group_3, self.__server_3)

        self.__options_4 = {
            "uuid" :  _uuid.UUID("{bb45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(3),
            "user" : MySQLInstances().user,
            "passwd": MySQLInstances().passwd,
        }

        uuid_server4 = MySQLServer.discover_uuid(self.__options_4["address"])
        self.__options_4["uuid"] = _uuid.UUID(uuid_server4)
        self.__server_4 = MySQLServer(**self.__options_4)
        MySQLServer.add(self.__server_4)
        self.__server_4.connect()
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_4.exec_stmt("CREATE DATABASE db1")
        self.__server_4.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_4.exec_stmt("CREATE DATABASE db2")
        self.__server_4.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))

        self.__group_4 = Group("GROUPID4", "Fourth description.")
        Group.add( self.__group_4)
        self.__group_4.add_server(self.__server_4)
        tests.utils.configure_decoupled_master(self.__group_4, self.__server_4)

        self.__options_5 = {
            "uuid" :  _uuid.UUID("{cc75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(4),
            "user" : MySQLInstances().user,
            "passwd": MySQLInstances().passwd,
        }

        uuid_server5 = MySQLServer.discover_uuid(self.__options_5["address"])
        self.__options_5["uuid"] = _uuid.UUID(uuid_server5)
        self.__server_5 = MySQLServer(**self.__options_5)
        MySQLServer.add(self.__server_5)
        self.__server_5.connect()
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_5.exec_stmt("CREATE DATABASE db1")
        self.__server_5.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT PRIMARY KEY, name VARCHAR(30))")
        for i in range(1, 71):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(101, 301):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(1001, 1201):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(10001, 10201):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        for i in range(100001, 100201):
            self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(%s, 'TEST %s')" % (i, i))
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_5.exec_stmt("CREATE DATABASE db2")
        self.__server_5.exec_stmt("CREATE TABLE db2.t2"
                                  "(userID INT, salary INT, "
                                  "CONSTRAINT FOREIGN KEY(userID) "
                                  "REFERENCES db1.t1(userID))")
        for i in range(1, 71):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(101, 301):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(1001, 1201):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(10001, 10201):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))
        for i in range(100001, 100201):
            self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                                  "VALUES(%s, %s)" % (i, i))


        self.__group_5 = Group("GROUPID5", "Fifth description.")
        Group.add( self.__group_5)
        self.__group_5.add_server(self.__server_5)
        tests.utils.configure_decoupled_master(self.__group_5, self.__server_5)

        self.__options_6 = {
            "uuid" :  _uuid.UUID("{cc45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address"  : MySQLInstances().get_address(5),
            "user" : MySQLInstances().user,
            "passwd": MySQLInstances().passwd,
        }

        uuid_server6 = MySQLServer.discover_uuid(self.__options_6["address"])
        self.__options_6["uuid"] = _uuid.UUID(uuid_server6)
        self.__server_6 = MySQLServer(**self.__options_6)
        MySQLServer.add(self.__server_6)
        self.__server_6.connect()

        self.__group_6 = Group("GROUPID6", "Sixth description.")
        Group.add( self.__group_6)
        self.__group_6.add_server(self.__server_6)
        tests.utils.configure_decoupled_master(self.__group_6, self.__server_6)

        expected_shard_mapping_id = tests.utils.make_mapping_result([[1, "RANGE", "GROUPID1"]])
        status = self.proxy.sharding.create_definition("RANGE", "GROUPID1")
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "userID", True)
        self.assertStatus(status, _executor.Job.SUCCESS)
        status = self.proxy.sharding.add_table(1, "db2.t2", "userID")
        self.assertStatus(status, _executor.Job.SUCCESS)

        status = self.proxy.sharding.add_shard(
            1,
            "GROUPID2/1,GROUPID3/101,GROUPID4/1001,GROUPID5/10001",
            "ENABLED"
        )
        self.assertStatus(status, _executor.Job.SUCCESS)

        status = self.proxy.sharding.prune_shard("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)

    def test_shard_range(self):
        """Verify that only the valid shard keys are allowed to be inserted
        into the shards and ensure that the triggers defined for this purpose
        are working.
        """
        status = self.proxy.sharding.lookup_servers("db1.t1", "3",  "LOCAL")
        found = False
        for obtained_server in  self.check_xmlrpc_iter(status):
            if obtained_server['status'] == "PRIMARY" and obtained_server['mode'] == "READ_WRITE":
                found = True
                break;
        self.assertTrue(found) 
        shard_uuid = obtained_server['server_uuid']
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        #Inserting a row out of range should fail.
        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
                          "INSERT INTO db1.t1 VALUES('110','Data220')")
        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
                          "INSERT INTO db1.t1 VALUES('1000','Data220')")
        #Inserting a value within the valid range should pass.
        shard_server.exec_stmt(
            "INSERT INTO db1.t1 VALUES('81','Data007')"
        )

        status = self.proxy.sharding.lookup_servers("db1.t1", "301",  "LOCAL")
        found = False
        for obtained_server in  self.check_xmlrpc_iter(status):
            if obtained_server['status'] == "PRIMARY" and obtained_server['mode'] == "READ_WRITE":
                found = True
                break;
        self.assertTrue(found) 
        shard_uuid = obtained_server['server_uuid']
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        #Inserting a row out of range should fail.
        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
                          "INSERT INTO db1.t1 VALUES('10000','Data220')")
        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
                          "INSERT INTO db1.t1 VALUES('2000000','Data220')")
        #Inserting a value within the valid range should pass.
        shard_server.exec_stmt(
            "INSERT INTO db1.t1 VALUES('401','Data007')"
        )

        status = self.proxy.sharding.lookup_servers("db1.t1", "1301",  "LOCAL")
        found = False
        for obtained_server in  self.check_xmlrpc_iter(status):
            if obtained_server['status'] == "PRIMARY" and obtained_server['mode'] == "READ_WRITE":
                found = True
                break;
        self.assertTrue(found) 
        shard_uuid = obtained_server['server_uuid']
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        #Inserting a row out of range should fail.
        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
                          "INSERT INTO db1.t1 VALUES('101','Data220')")
        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
                          "INSERT INTO db1.t1 VALUES('1000001','Data220')")
        #Inserting a value within the valid range should pass.
        shard_server.exec_stmt(
            "INSERT INTO db1.t1 VALUES('1301','Data007')"
        )

        status = self.proxy.sharding.lookup_servers("db1.t1", "12000",  "LOCAL")
        found = False
        for obtained_server in  self.check_xmlrpc_iter(status):
            if obtained_server['status'] == "PRIMARY" and obtained_server['mode'] == "READ_WRITE":
                found = True
                break;
        self.assertTrue(found) 
        shard_uuid = obtained_server['server_uuid']
        shard_server = MySQLServer.fetch(shard_uuid)
        shard_server.connect()
        #Inserting a row out of range should fail.
        self.assertRaises(_errors.DatabaseError, shard_server.exec_stmt,
                          "INSERT INTO db1.t1 VALUES('9001','Data220')")
        #Inserting a value within the valid range should pass.
        shard_server.exec_stmt(
            "INSERT INTO db1.t1 VALUES('12000','Data220')"
        )

    def tearDown(self):
        status = self.proxy.sharding.disable_shard("1")
        status = self.proxy.sharding.disable_shard("2")
        status = self.proxy.sharding.disable_shard("3")
        status = self.proxy.sharding.disable_shard("4")
        status = self.proxy.sharding.disable_shard("5")
        status = self.proxy.sharding.disable_shard("6")

        status = self.proxy.sharding.remove_shard("1")
        status = self.proxy.sharding.remove_shard("2")
        status = self.proxy.sharding.remove_shard("3")
        status = self.proxy.sharding.remove_shard("4")
        status = self.proxy.sharding.remove_shard("5")
        status = self.proxy.sharding.remove_shard("6")

        status = self.proxy.sharding.remove_table("db1.t1")
        self.assertStatus(status, _executor.Job.SUCCESS)

        status = self.proxy.sharding.remove_table("db2.t2")
        self.assertStatus(status, _executor.Job.SUCCESS)

        status = self.proxy.sharding.remove_definition("1")
        self.assertStatus(status, _executor.Job.SUCCESS)

        self.proxy.group.demote("GROUPID1")
        self.proxy.group.demote("GROUPID2")
        self.proxy.group.demote("GROUPID3")
        self.proxy.group.demote("GROUPID4")
        self.proxy.group.demote("GROUPID5")
        self.proxy.group.demote("GROUPID6")

        for group_id in ("GROUPID1", "GROUPID2", "GROUPID3",
            "GROUPID4", "GROUPID5", "GROUPID6"):
            status = self.proxy.group.lookup_servers(group_id)
            for obtained_server in  self.check_xmlrpc_iter(status):
                status = self.proxy.group.remove(
                    group_id, obtained_server["server_uuid"]
                )
                self.check_xmlrpc_command_result(status)
            status = self.proxy.group.destroy(group_id)
            self.assertStatus(status, _executor.Job.SUCCESS)

        tests.utils.cleanup_environment()


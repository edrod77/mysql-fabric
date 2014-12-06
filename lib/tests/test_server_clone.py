#
# Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
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

import uuid as _uuid

import unittest
import tests.utils
from tests.utils import MySQLInstances

from mysql.fabric import executor as _executor
from mysql.fabric.server import MySQLServer, Group
from mysql.fabric.errors import DatabaseError

class TestServerClone(tests.utils.TestCase):
    """Tests the mysqlfabric clone command to clone all the data in a server
    registered in Fabric into another server.  Create a Group and add some
    servers. Insert data into one of the servers. Now clone this server into
    the other servers in the group. Now start replication and ensure everything
    works fine.
    """
    def setUp(self):
        """Create the setup for performing the testing of the server clone
        command.
        """
        from __main__ import mysqldump_path, mysqlclient_path
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address": MySQLInstances().get_address(0),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server1 = MySQLServer.discover_uuid(self.__options_1["address"])
        self.__options_1["uuid"] = _uuid.UUID(uuid_server1)
        self.__server_1 = MySQLServer(**self.__options_1)
        self.__server_1.status = MySQLServer.SECONDARY
        MySQLServer.add(self.__server_1)
        self.__server_1.connect()

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address":MySQLInstances().get_address(1),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server2 = MySQLServer.discover_uuid(self.__options_2["address"])
        self.__options_2["uuid"] = _uuid.UUID(uuid_server2)
        self.__server_2 = MySQLServer(**self.__options_2)
        MySQLServer.add(self.__server_2)
        self.__server_2.status = MySQLServer.SECONDARY
        self.__server_2.connect()

        self.__options_3 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            "address":MySQLInstances().get_address(2),
            "user" : MySQLInstances().user,
            "passwd" : MySQLInstances().passwd,
        }

        uuid_server3 = MySQLServer.discover_uuid(self.__options_3["address"])
        self.__options_3["uuid"] = _uuid.UUID(uuid_server3)
        self.__server_3 = MySQLServer(**self.__options_3)
        self.__server_3.status = MySQLServer.SECONDARY
        MySQLServer.add(self.__server_3)
        self.__server_3.connect()

        #Insert data into Server 1
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS db1")
        self.__server_1.exec_stmt("CREATE DATABASE db1")
        self.__server_1.exec_stmt("CREATE TABLE db1.t1"
                                  "(userID INT, name VARCHAR(30))")
        self.__server_1.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(101, 'TEST 1')")
        self.__server_1.exec_stmt("INSERT INTO db1.t1 "
                                  "VALUES(202, 'TEST 2')")
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_1.exec_stmt("CREATE DATABASE db2")
        self.__server_1.exec_stmt("CREATE TABLE db2.t1"
                                  "(userID INT, name VARCHAR(30))")
        self.__server_1.exec_stmt("INSERT INTO db2.t1 "
                                  "VALUES(101, 'TEST 1')")
        self.__server_1.exec_stmt("INSERT INTO db2.t1 "
                                  "VALUES(202, 'TEST 2')")

    def test_clone_from_group(self):
        """Verify the clone operation from a group.
        """
        self.__group_1 = Group("GROUPID1", "First description.")
        Group.add(self.__group_1)
        self.__group_1.add_server(self.__server_1)
        self.__group_1.add_server(self.__server_2)
        self.__group_1.master = self.__server_2.uuid

        try:
            status = self.proxy.server.clone("GROUPID1", self.__server_2.address,
                                        str(self.__server_1.uuid))
            raise Exception("Cloning to a server inside Fabric should "
                "throw a fault")
        except:
            pass

        try:
            status = self.proxy.server.clone("GROUPID1", self.__server_2.address,
                                uuid_server3)
            raise Exception("Cloning from a server outside the "
                "source group should throw a fault")
        except:
            pass

        status = self.proxy.server.clone("GROUPID1", self.__server_3.address,
                                         None)
        self.check_xmlrpc_command_result(status)
        rows = self.__server_3.exec_stmt(
                                    "SELECT NAME FROM db1.t1",
                                    {"fetch" : True})
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 'TEST 1')
        self.assertEqual(rows[1][0], 'TEST 2')
        rows = self.__server_3.exec_stmt(
                                    "SELECT NAME FROM db2.t1",
                                    {"fetch" : True})
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 'TEST 1')
        self.assertEqual(rows[1][0], 'TEST 2')

    def test_clone_error(self):
        """Verify that the clone operations throws an error
        for wrong input values.
        """
        try:
            status = self.proxy.server.clone(None, self.__server_3.address,
                                    str(self.__server_1.uuid))
            raise Exception("Not providing a group "\
                "should throw a fault")
        except:
            pass

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

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
from tests.utils import MySQLInstances

class TestShardingPrune(tests.utils.TestCase):
    """Contains unit tests for testing the shard split operation and for
    verifying that the global server configuration remains constant after
    the shard split configuration.
    """
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
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")
        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_2.exec_stmt("CREATE DATABASE db2")
        self.__server_2.exec_stmt("CREATE TABLE db2.t2"
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_2.exec_stmt("CREATE DATABASE db3")
        self.__server_2.exec_stmt("CREATE TABLE db3.t3"
                                  "(date_of_joining VARCHAR(30), name VARCHAR(30))")

        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("DROP DATABASE IF EXISTS db4")
        self.__server_2.exec_stmt("CREATE DATABASE db4")
        self.__server_2.exec_stmt("CREATE TABLE db4.t4"
                                  "(date_of_joining TEXT, name VARCHAR(30))")

        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_2.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

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
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")
        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_3.exec_stmt("CREATE DATABASE db2")
        self.__server_3.exec_stmt("CREATE TABLE db2.t2"
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_3.exec_stmt("CREATE DATABASE db3")
        self.__server_3.exec_stmt("CREATE TABLE db3.t3"
                                  "(date_of_joining VARCHAR(30), name VARCHAR(30))")

        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("DROP DATABASE IF EXISTS db4")
        self.__server_3.exec_stmt("CREATE DATABASE db4")
        self.__server_3.exec_stmt("CREATE TABLE db4.t4"
                                  "(date_of_joining TEXT, name VARCHAR(30))")

        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_3.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

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
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")
        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_4.exec_stmt("CREATE DATABASE db2")
        self.__server_4.exec_stmt("CREATE TABLE db2.t2"
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_4.exec_stmt("CREATE DATABASE db3")
        self.__server_4.exec_stmt("CREATE TABLE db3.t3"
                                  "(date_of_joining VARCHAR(30), name VARCHAR(30))")

        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("DROP DATABASE IF EXISTS db4")
        self.__server_4.exec_stmt("CREATE DATABASE db4")
        self.__server_4.exec_stmt("CREATE TABLE db4.t4"
                                  "(date_of_joining TEXT, name VARCHAR(30))")

        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_4.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

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
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db1.t1 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")
        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db2")
        self.__server_5.exec_stmt("CREATE DATABASE db2")
        self.__server_5.exec_stmt("CREATE TABLE db2.t2"
                                  "(date_of_joining DATETIME, name VARCHAR(30))")

        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db2.t2 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db3")
        self.__server_5.exec_stmt("CREATE DATABASE db3")
        self.__server_5.exec_stmt("CREATE TABLE db3.t3"
                                  "(date_of_joining VARCHAR(30), name VARCHAR(30))")

        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db3.t3 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("DROP DATABASE IF EXISTS db4")
        self.__server_5.exec_stmt("CREATE DATABASE db4")
        self.__server_5.exec_stmt("CREATE TABLE db4.t4"
                                  "(date_of_joining TEXT, name VARCHAR(30))")

        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2014-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2013-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2012-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2011-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2010-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('2009-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1914-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1913-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1912-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1911-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1910-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1909-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1814-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1813-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1812-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1811-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1810-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1809-05-23 13:45:00', 'name 10')")

        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-27 11:45:00', 'name 1')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-26 10:45:00', 'name 2')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-25 09:45:00', 'name 3')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-24 08:45:00', 'name 4')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1714-05-23 13:45:00', 'name 5')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1713-05-27 11:45:00', 'name 6')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1712-05-26 10:45:00', 'name 7')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1711-05-25 09:45:00', 'name 8')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1710-05-24 08:45:00', 'name 9')")
        self.__server_5.exec_stmt("INSERT INTO db4.t4 "
                              "VALUES('1709-05-23 13:45:00', 'name 10')")


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

        status = self.proxy.sharding.create_definition("RANGE_DATETIME", "GROUPID1")
        self.check_xmlrpc_command_result(status, returns=1)

        status = self.proxy.sharding.add_table(1, "db1.t1", "date_of_joining")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_table(1, "db2.t2", "date_of_joining")
        self.check_xmlrpc_command_result(status)
        
        status = self.proxy.sharding.add_table(1, "db3.t3", "date_of_joining")
        self.check_xmlrpc_command_result(status)
        
        status = self.proxy.sharding.add_table(1, "db4.t4", "date_of_joining")
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.add_shard(
            1,
            "GROUPID2/1700-01-01,GROUPID3/1800-01-01,GROUPID4/1900-01-01,GROUPID5/2000-01-01",
            "ENABLED"
        )
        self.check_xmlrpc_command_result(status)

        status = self.proxy.sharding.prune_shard("db1.t1")
        self.check_xmlrpc_command_result(status)

    def test_split_shard_4(self):
        '''Test the split of shard 4 and the global server configuration
        after that. The test splits shard 4 between GROUPID5 and GROUPID6.
        After the split is done, it verifies the count on GROUPID6 to check
        that the group has tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples.
        '''
        row_cnt_shard_before_split_db1_t1 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_before_split_db2_t2 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_before_split_db3_t3 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_before_split_db4_t4 = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db4.t4",
                    {"fetch" : True}
                )

        row_cnt_shard_before_split_db1_t1 = \
            int(row_cnt_shard_before_split_db1_t1[0][0])
        row_cnt_shard_before_split_db2_t2 = \
            int(row_cnt_shard_before_split_db2_t2[0][0])
        row_cnt_shard_before_split_db3_t3 = \
            int(row_cnt_shard_before_split_db3_t3[0][0])
        row_cnt_shard_before_split_db4_t4 = \
            int(row_cnt_shard_before_split_db4_t4[0][0])

        self.assertEqual(row_cnt_shard_before_split_db1_t1, 10)
        self.assertEqual(row_cnt_shard_before_split_db2_t2, 10)
        self.assertEqual(row_cnt_shard_before_split_db3_t3, 10)
        self.assertEqual(row_cnt_shard_before_split_db4_t4, 10)

        status = self.proxy.sharding.split_shard("4", "GROUPID6", "2014-01-01")
        self.check_xmlrpc_command_result(status)

        row_cnt_shard_after_split_db1_t1_a = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db1_t1_a = \
            int(row_cnt_shard_after_split_db1_t1_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db1_t1_a, 5)

        row_cnt_shard_after_split_db2_t2_a = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db2_t2_a = \
            int(row_cnt_shard_after_split_db2_t2_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db2_t2_a, 5)

        row_cnt_shard_after_split_db3_t3_a = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db3_t3_a = \
            int(row_cnt_shard_after_split_db3_t3_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db3_t3_a, 5)

        row_cnt_shard_after_split_db4_t4_a = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM db4.t4",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db4_t4_a = \
            int(row_cnt_shard_after_split_db4_t4_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db4_t4_a, 5)

        row_cnt_shard_after_split_db1_t1_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db1_t1_b = \
            int(row_cnt_shard_after_split_db1_t1_b[0][0])
        self.assertEqual(row_cnt_shard_after_split_db1_t1_b, 5)

        row_cnt_shard_after_split_db2_t2_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db2_t2_b = \
            int(row_cnt_shard_after_split_db2_t2_b[0][0])
        self.assertEqual(row_cnt_shard_after_split_db2_t2_b, 5)

        row_cnt_shard_after_split_db3_t3_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db3_t3_b = \
            int(row_cnt_shard_after_split_db3_t3_b[0][0])
        self.assertEqual(row_cnt_shard_after_split_db3_t3_b, 5)

        row_cnt_shard_after_split_db4_t4_b = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM db4.t4",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db4_t4_b = \
            int(row_cnt_shard_after_split_db4_t4_b[0][0])
        self.assertEqual(row_cnt_shard_after_split_db4_t4_b, 5)

        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID VARCHAR(20), name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES('%s', 'TEST %s')" % (i, i))
        time.sleep(5)
        #Verify that the data is there in the first shard.
        global_table_count = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the second shard.
        global_table_count = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the third shard.
        global_table_count = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fifth shard.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def test_split_shard_2(self):
        '''Test the split of shard 2 and the global server configuration
        after that. The test splits shard 2 between GROUPID3 and GROUPID6.
        After the split is done, it verifies the count on GROUPID6 to check
        that the group has tuples from the earlier group. Now it fires
        an INSERT on the global group and verifies that all the shards have got
        the inserted tuples.
        '''
        row_cnt_shard_before_split_db1_t1 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_before_split_db2_t2 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_before_split_db3_t3 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_before_split_db4_t4 = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db4.t4",
                    {"fetch" : True}
                )

        row_cnt_shard_before_split_db1_t1 = \
            int(row_cnt_shard_before_split_db1_t1[0][0])
        row_cnt_shard_before_split_db2_t2 = \
            int(row_cnt_shard_before_split_db2_t2[0][0])
        row_cnt_shard_before_split_db3_t3 = \
            int(row_cnt_shard_before_split_db3_t3[0][0])
        row_cnt_shard_before_split_db4_t4 = \
            int(row_cnt_shard_before_split_db4_t4[0][0])

        self.assertEqual(row_cnt_shard_before_split_db1_t1, 10)
        self.assertEqual(row_cnt_shard_before_split_db2_t2, 10)
        self.assertEqual(row_cnt_shard_before_split_db3_t3, 10)
        self.assertEqual(row_cnt_shard_before_split_db4_t4, 10)

        status = self.proxy.sharding.split_shard("2", "GROUPID6", "1814-01-01")
        self.check_xmlrpc_command_result(status)

        row_cnt_shard_after_split_db1_t1_a = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db1.t1",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db1_t1_a = \
            int(row_cnt_shard_after_split_db1_t1_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db1_t1_a, 5)

        row_cnt_shard_after_split_db2_t2_a = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db2.t2",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db2_t2_a = \
            int(row_cnt_shard_after_split_db2_t2_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db2_t2_a, 5)

        row_cnt_shard_after_split_db3_t3_a = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db3.t3",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db3_t3_a = \
            int(row_cnt_shard_after_split_db3_t3_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db3_t3_a, 5)

        row_cnt_shard_after_split_db4_t4_a = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM db4.t4",
                    {"fetch" : True}
                )
        row_cnt_shard_after_split_db4_t4_a = \
            int(row_cnt_shard_after_split_db4_t4_a[0][0])
        self.assertEqual(row_cnt_shard_after_split_db4_t4_a, 5)
        #Enter data into the global server
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS global")
        self.__server_1.exec_stmt("CREATE DATABASE global")
        self.__server_1.exec_stmt("CREATE TABLE global.global_table"
                                  "(userID VARCHAR(20), name VARCHAR(30))")
        for i in range(1, 11):
            self.__server_1.exec_stmt("INSERT INTO global.global_table "
                                  "VALUES('%s', 'TEST %s')" % (i, i))
        time.sleep(5)
        #Verify that the data is there in the first shard.
        global_table_count = self.__server_2.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the second shard.
        global_table_count = self.__server_3.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the third shard.
        global_table_count = self.__server_4.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fourth shard.
        global_table_count = self.__server_5.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)
        #Verify that the data is there in the fifth shard.
        global_table_count = self.__server_6.exec_stmt(
                    "SELECT COUNT(*) FROM global.global_table",
                    {"fetch" : True}
                )
        global_table_count = int(global_table_count[0][0])
        self.assertTrue(global_table_count == 10)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

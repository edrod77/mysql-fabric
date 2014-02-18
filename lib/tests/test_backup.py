#
# Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
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

import unittest
import uuid as _uuid
import tests.utils

from tests.utils import MySQLInstances
from mysql.fabric.backup import MySQLDump
from mysql.fabric.server import MySQLServer

class TestBackupMySQLDump(unittest.TestCase):
    """Test taking a backup from a source group to a destination group. The
    source group handles the source shard and the destination group handles
    the destination shard. The backup and restore helps take up a backup of
    the source shard and setup the destination shard.
    """
    def setUp(self):
        """Configure the existing environment
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
        self.__server_2.connect()

        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS backup_db")
        self.__server_1.exec_stmt("CREATE DATABASE backup_db")
        self.__server_1.exec_stmt("CREATE TABLE backup_db.backup_table"
                                  "(userID INT, name VARCHAR(30))")
        self.__server_1.exec_stmt("INSERT INTO backup_db.backup_table "
                                  "VALUES(101, 'TEST 1')")
        self.__server_1.exec_stmt("INSERT INTO backup_db.backup_table "
                                  "VALUES(202, 'TEST 2')")

        self.mysqldump_path = mysqldump_path
        self.mysqlclient_path = mysqlclient_path

    def test_backup(self):
        restore_server = MySQLServer.fetch(self.__server_2.uuid)
        image = MySQLDump.backup(self.__server_1, self.mysqldump_path)
        restore_server.connect()
        MySQLDump.restore(restore_server, image, self.mysqlclient_path)
        rows = restore_server.exec_stmt(
                                    "SELECT NAME FROM backup_db.backup_table",
                                    {"fetch" : True})
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 'TEST 1')
        self.assertEqual(rows[1][0], 'TEST 2')

    def tearDown(self):
        """Clean up the existing environment
        """
        self.__server_1.exec_stmt("DROP DATABASE IF EXISTS backup_db")

        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

if __name__ == "__main__":
    unittest.main()

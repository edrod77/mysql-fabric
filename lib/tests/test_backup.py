import unittest
import uuid as _uuid

import mysql.hub.server_utils as _server_utils

import os

import tests.utils

from mysql.hub.backup import BackupImage,  MySQLDump
from mysql.hub.server import Group, MySQLServer
import mysql.hub.persistence as _persistence

from tests.utils import MySQLInstances

class TestBackupMySQLDump(unittest.TestCase):
    """Test taking a backup from a source group to a destination group. The
    source group handles the source shard and the destination group handles
    the destination shard. The backup and restore helps take up a backup of
    the source shard and setup the destination shard.
    """
    def setUp(self):
        from __main__ import mysqldump_path, mysqlclient_path
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()

        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

        self.__options_1 = {
            "uuid" :  _uuid.UUID("{aa75b12b-98d1-414c-96af-9e9d4b179678}"),
            "address":MySQLInstances().get_address(0),
            "user" : "root"
        }

        uuid_server1 = MySQLServer.discover_uuid(**self.__options_1)
        self.__options_1["uuid"] = _uuid.UUID(uuid_server1)
        self.__server_1 = MySQLServer(**self.__options_1)
        MySQLServer.add(self.__server_1)
        self.__server_1.connect()

        self.__options_2 = {
            "uuid" :  _uuid.UUID("{aa45b12b-98d1-414c-96af-9e9d4b179678}"),
            #Using localhost causes problems while connecting to running MySQL
            #server.
            "address":MySQLInstances().get_address(1),
            "user" : "root"
        }

        uuid_server2 = MySQLServer.discover_uuid(**self.__options_2)
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
        self.__server_1.exec_stmt("DROP DATABASE backup_db")
        self.__server_2.exec_stmt("DROP DATABASE backup_db")

        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

if __name__ == "__main__":
    unittest.main()

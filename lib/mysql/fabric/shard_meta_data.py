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

import mysql.fabric.persistence as _persistence

from mysql.fabric.server import (
    Group,
    MySQLServer,
)

class ShardMetaData(_persistence.Persistable):
    """Class contains the logic for defining the meta data to be stored in
    the groups that contain the shards. The metadata for now contains a table
    that stores the valid range of values that can be stored in the MySQL
    servers that form the shard. Going forward the metadata could evolve into
    anything.
    """

    #Create the database that stores the sharding metadata
    #in the shard.
    CREATE_SHARDING_SCHEMA_DATABASE = (
        "CREATE DATABASE IF NOT EXISTS mysql_fabric"
    )

    #Create the schema for storing the valid range of shard
    #values in each shard.
    CREATE_SHARDING_SCHEMA_TABLE = (
        "CREATE TABLE IF NOT EXISTS "
        "mysql_fabric.mysql_shard_range ("
        "shard_id INT,"
        "lower_bound VARBINARY(32),"
        "upper_bound VARBINARY(32)"
        ")"
    )

    #Insert the ranges into the range verification tables
    INSERT_SHARDING_METADATA = (
        "INSERT INTO mysql_fabric.mysql_shard_range VALUES(%s, %s, %s)"
    )

    #Delete the ranges from the range verification tables.
    DELETE_SHARDING_METADATA = (
        "DELETE FROM mysql_fabric.mysql_shard_range WHERE shard_id = %s"
    )

    #Drop the schema for storing the valid range of shard values.
    DROP_SHARDING_SCHEMA_TABLE = (
        "DROP TABLE IF EXISTS mysql_fabric.mysql_shard_range"
    )

    #Drop the sharding schema database.
    DROP_SHARDING_SCHEMA_DATABASE = (
        "DROP DATABASE IF EXISTS mysql_fabric"
    )

    #Fetch the ranges from the shard
    FETCH_SHARDING_METADATA = (
        "SELECT lower_bound, upper_bound "
        "FROM mysql_fabric.mysql_shard_range "
        "WHERE shard_id = %s"
    )

    @staticmethod
    def _fetch_master_of_group(group_id):
        """Return a reference to the master of the group.

        :param group_id: ID of the group whose master needs to be fetched.

        :return: MySQLServer object referring to the group master.
        """
        global_group = Group.fetch(group_id)
        master_server = MySQLServer.fetch(global_group.master)
        master_server.connect()
        return master_server

    @staticmethod
    def create_shard_meta_data(group_id):
        """Create the schema (database and table) for storing metadata in
        the shards.

        :param group_id: Group ID of the global group for
                                the shard definition.
        """
        master_server = ShardMetaData._fetch_master_of_group(group_id)
        #Create the schemas on the master and it will be replicated
        #to all the other servers on the shard.
        master_server.exec_stmt(ShardMetaData.CREATE_SHARDING_SCHEMA_DATABASE)
        master_server.exec_stmt(ShardMetaData.CREATE_SHARDING_SCHEMA_TABLE)

    @staticmethod
    def drop_shard_meta_data(group_id):
        """Drop the schemas (database and the table) used for storing the sharding
        metadata in the shards.

        :param group_id: Group ID of the global group for
                                the shard definition.
        """
        #Fetch the master of the group.
        master_server = ShardMetaData._fetch_master_of_group(group_id)
        #Drop the schemas on the master and the drop will be replicated
        #to all the other servers in the shard.
        master_server.exec_stmt(ShardMetaData.DROP_SHARDING_SCHEMA_TABLE)
        master_server.exec_stmt(ShardMetaData.DROP_SHARDING_SCHEMA_DATABASE)

    @staticmethod
    def insert_shard_meta_data(shard_id, lower_bound, upper_bound, group_id):
        """Insert the valid shard ranges on the shard server.

        :param shard_id: The Shard ID of the shard whose metadata needs to
                        be updated.
        :param lower_bound: The lower bound of the valid range of shard key
                            values.
        :param upper_bound: The upper bound of the valid range of shard key
                            values.
        :param group_id: Group ID of the global group for
                                the shard definition.
        """
        #Fetch the master of the group.
        master_server = ShardMetaData._fetch_master_of_group(group_id)
        #Perform the insert on the master and it will get replicated to
        #all the servers of the shard.
        master_server.exec_stmt(
            ShardMetaData.INSERT_SHARDING_METADATA,
            {"params": (shard_id, lower_bound, upper_bound, )}
        )

    @staticmethod
    def delete_shard_meta_data(group_id, shard_id):
        """Remove the shard range defined for the shard_id.

        :param group_id: The ID of the group containing the shards data.
        :param shard_id: The shard ID of the shard whose metadata needs to be
                        deleted.
        """
        master_server = ShardMetaData._fetch_master_of_group(group_id)
        #Perform the delete on the master so that it gets
        #replicated to all the servers.
        row = master_server.exec_stmt(
            ShardMetaData.DELETE_SHARDING_METADATA,
            {
                "raw" : False,
                "fetch" : True,
                "params":(shard_id, )
            }
        )

    @staticmethod
    def fetch_shard_meta_data(shard_id, group_id):
        """Fetch the metadata for the particular shard.

        :param shard_id: The shard ID of the shard whose metadata needs to be
                         fetched.
        :param group_id: The ID of the group in which the shard id present.
        :return: Dictionary of the lower_bound and upper_bound for a
                 particular shard.
        """
        master_server = ShardMetaData._fetch_master_of_group(group_id)
        #Fetch the metadata from the master.
        rows = master_server.exec_stmt(ShardMetaData.FETCH_SHARDING_METADATA,
                                {"raw" : False,
                                  "fetch" : True,
                                  "params":(shard_id, )
                                }
                            )
        if rows:
            return {"lower_bound": rows[0][0], "upper_bound": rows[0][1]}

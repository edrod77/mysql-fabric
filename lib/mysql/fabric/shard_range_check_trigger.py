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

_TRIGGER_DEFN = {}

_TRIGGER_DEFN["RANGE"] = """
CREATE TRIGGER {trigger_name} BEFORE {operation} ON {table_name}
FOR EACH ROW BEGIN
DECLARE lb VARCHAR(16);
DECLARE ub VARCHAR(16);
SELECT lower_bound INTO lb FROM mysql_fabric.mysql_shard_range;
SELECT upper_bound INTO ub FROM mysql_fabric.mysql_shard_range;
IF
CAST({column_name} AS SIGNED) >= CAST(ub AS SIGNED)
OR
CAST({column_name} AS SIGNED) < CAST(lb AS SIGNED)
THEN
SIGNAL SQLSTATE '22003'
SET MESSAGE_TEXT = 'Sharding key out of range';
END IF;
END;
"""

_TRIGGER_DEFN["RANGE_STRING"] = """
CREATE TRIGGER {trigger_name} BEFORE {operation} ON {table_name}
FOR EACH ROW
BEGIN
DECLARE lb VARCHAR(16);
DECLARE ub VARCHAR(16);
SELECT lower_bound INTO lb FROM mysql_fabric.mysql_shard_range;
SELECT upper_bound INTO ub FROM mysql_fabric.mysql_shard_range;
IF
CAST({column_name} AS CHAR CHARACTER SET utf8) COLLATE utf8_unicode_ci
>=
CAST(ub AS CHAR CHARACTER SET utf8) COLLATE utf8_unicode_ci
OR
CAST({column_name} AS CHAR CHARACTER SET utf8) COLLATE utf8_unicode_ci
<
CAST(lb AS CHAR CHARACTER SET utf8) COLLATE utf8_unicode_ci
THEN
SIGNAL SQLSTATE '22003' SET MESSAGE_TEXT = 'Sharding key out of range';
END IF;
END;
"""

_TRIGGER_DEFN["RANGE_INTEGER"] = _TRIGGER_DEFN["RANGE"]

_TRIGGER_DEFN["RANGE_DATETIME"] = """
CREATE TRIGGER {trigger_name} BEFORE {operation} ON {table_name}
FOR EACH ROW BEGIN
DECLARE lb VARCHAR(16);
DECLARE ub VARCHAR(16);
SELECT lower_bound INTO lb FROM mysql_fabric.mysql_shard_range;
SELECT upper_bound INTO ub FROM mysql_fabric.mysql_shard_range;
IF
CAST({column_name} AS DATETIME) >= CAST(ub AS DATETIME)
OR
CAST({column_name} AS DATETIME) < CAST(lb AS DATETIME)
THEN
SIGNAL SQLSTATE '22003' SET MESSAGE_TEXT = 'Sharding key out of range';
END IF;
END;
"""

_TRIGGER_DEFN["HASH"] = """
CREATE TRIGGER {trigger_name} BEFORE {operation} ON {table_name}
FOR EACH ROW BEGIN
DECLARE lb VARCHAR(16);
DECLARE ub VARCHAR(16);
SELECT lower_bound INTO lb FROM mysql_fabric.mysql_shard_range;
SELECT upper_bound INTO ub FROM mysql_fabric.mysql_shard_range;
IF MD5({column_name}) >= ub OR MD5({column_name}) < lb THEN
SIGNAL SQLSTATE '22003' SET MESSAGE_TEXT = 'Sharding key out of range';
END IF;
END;
"""

#Used as the generic prefix for all MySQL Fabric insert specific triggers.
_TRIGGER_PREFIX_INSERT = "myfab_chk_insert_"

#Used as the generic prefix for all MySQL Fabric update specific triggers.
_TRIGGER_PREFIX_UPDATE = "myfab_chk_update_"

_DROP_TRIGGER_DEFN = "DROP TRIGGER IF EXISTS {trigger_name}"

class ShardMetaDataCheck(_persistence.Persistable):
    """Class used for creating and dropping the triggers that perform the
    boundary checks on the tables that are sharded. The class contains the
    DDL for INSERT and UPDATE triggers.
    """

    @staticmethod
    def add_shard_range_trigger(group_id, sharding_type, table_name,
                                column_name):
        """Add a trigger on the shard table to ensure that values
        inserted fall within the valid shard ranges.

        :param group_id: The ID of the group on which the trigger definition
                         is applied. The trigger is created on the master of
                         this group.
        :param sharding_type: The datatype supported by the shards. Used to
                              name the trigger.
        :param table_name: The name of the table. This is used to name the
                           trigger being created.
        :param column_name: The name of the column in the table being sharded.
                            This is used to create the name of the trigger.
        """
        global_group = Group.fetch(group_id)
        master_server = MySQLServer.fetch(global_group.master)
        master_server.connect()

        #Create an INSERT trigger on the sharded table.
        db, table = table_name.split(".")
        trigger_tmpl = _TRIGGER_DEFN[sharding_type]
        trigger_name = db + "." + _TRIGGER_PREFIX_INSERT + table
 
        create_insert_trigger = trigger_tmpl.format(
            trigger_name=trigger_name,
            operation="INSERT",
            table_name=table_name,
            column_name="NEW"+"."+column_name
        )
        master_server.exec_stmt(create_insert_trigger)

        #Create an UPDATE trigger on the sharded table.
        trigger_tmpl = _TRIGGER_DEFN[sharding_type]
        trigger_name = db + "." + _TRIGGER_PREFIX_UPDATE + table
        create_update_trigger =trigger_tmpl.format(
                trigger_name=trigger_name,
                operation="UPDATE",
                table_name=table_name,
                column_name="NEW"+"."+column_name
            )
        master_server.exec_stmt(create_update_trigger)

    @staticmethod
    def drop_shard_range_trigger(group_id, sharding_type, table_name,
                                    column_name):
        """Drop a trigger on the shard table.

        :param group_id: The ID of the group on which the trigger definition
                         is applied. The trigger is created on the master of
                         this group.
        :param sharding_type: The datatype supported by the shards. Used to
                              name the trigger.
        :param table_name: The name of the table. This is used to name the
                           trigger being created.
        :param column_name: The name of the column in the table being sharded.
                            This is used to create the name of the trigger.
        """
        global_group = Group.fetch(group_id)
        master_server = MySQLServer.fetch(global_group.master)
        master_server.connect()

        db, table = table_name.split(".")

        #Drop the INSERT trigger on the sharded table.
        trigger_name = db + "." + _TRIGGER_PREFIX_INSERT+table
        drop_insert_trigger = _DROP_TRIGGER_DEFN.format(
            trigger_name=trigger_name
        )
        master_server.exec_stmt(drop_insert_trigger)

        #Drop the UPDATE trigger on the sharded table.
        trigger_name = db + "." + _TRIGGER_PREFIX_UPDATE + table
        drop_update_trigger = _DROP_TRIGGER_DEFN.format(
            trigger_name=trigger_name
        )
        master_server.exec_stmt(drop_update_trigger)

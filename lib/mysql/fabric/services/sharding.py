#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
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

"""This module provides the necessary interfaces for working with the shards
in FABRIC.
"""

import logging

from mysql.fabric import (
    errors as _errors,
    events as _events,
    group_replication as _group_replication,
    replication as _replication,
    backup as _backup,
)

from mysql.fabric.server import (
    Group,
    MySQLServer,
)

from mysql.fabric.sharding import (
    ShardMapping,
    RangeShardingSpecification,
    HashShardingSpecification,
    Shards,
    RangeShardingIntegerHandler,
    HashShardingIntegerHandler,
    SHARDING_DATATYPE_HANDLER,
    SHARDING_SPECIFICATION_HANDLER,
)

from mysql.fabric.command import (
    ProcedureShard,
    Command,
)

import mysql.fabric.utils as _utils

_LOGGER = logging.getLogger(__name__)

#Error messages
INVALID_SHARDING_TYPE = "Invalid Sharding Type %s"
TABLE_NAME_NOT_FOUND = "Table name %s not found"
CANNOT_REMOVE_SHARD_MAPPING = "Cannot remove mapping, while, " \
                                                "shards still exist"
INVALID_SHARD_STATE = "Invalid Shard State %s"
INVALID_SHARDING_RANGE = "Invalid sharding range"
SHARD_MAPPING_NOT_FOUND = "Shard Mapping with shard_mapping_id %s not found"
SHARD_MAPPING_DEFN_NOT_FOUND = "Shard Mapping Definition with "\
    "shard_mapping_id %s not found"
SHARD_NOT_DISABLED = "Shard not disabled"
SHARD_NOT_ENABLED = "Shard not enabled"
INVALID_SHARDING_KEY = "Invalid Key %s"
SHARD_NOT_FOUND = "Shard %s not found"
SHARD_LOCATION_NOT_FOUND = "Shard location not found"
INVALID_SHARDING_HINT = "Unknown lookup hint"
SHARD_GROUP_NOT_FOUND = "Shard group %s not found"
SHARD_GROUP_MASTER_NOT_FOUND = "Shard group master not found"
SHARD_MOVE_DESTINATION_NOT_EMPTY = "Shard move destination %s already "\
    "hosts a shard"
INVALID_SHARD_SPLIT_VALUE = "The chosen split value must be between the " \
                            "lower bound and upper bound of the shard"
CONFIG_NOT_FOUND = "Configuration option not found %s . %s"
INVALID_LOWER_BOUND = "Invalid lower_bound value for RANGE sharding " \
    "specification"
INVALID_LOWER_BOUND_VALUE = "Invalid lower_bound value for RANGE sharding " \
    "specification %s"
SHARDS_ALREADY_EXIST = "Shards are already present in the definition, "\
        "use split_shard to create further shards."
LOWER_BOUND_GROUP_ID_COUNT_MISMATCH = "Lower Bound, Group ID pair mismatch "\
                                    "format should be group-id/lower_bound, "\
                                    "group-id/lower_bound...."
LOWER_BOUND_AUTO_GENERATED = "Lower Bounds are auto-generated in hash "\
                            "based sharding"
SPLIT_VALUE_NOT_DEFINED = "Splitting a RANGE shard definition requires a split"\
            " value to be defined"
INVALID_SPLIT_VALUE = "Invalid value given for shard splitting"
NO_LOWER_BOUND_FOR_HASH_SHARDING = "Lower bound should not be specified "\
                                "for hash based sharding"
MYSQLDUMP_NOT_FOUND = "Unable to find MySQLDump in location %s"
MYSQLCLIENT_NOT_FOUND = "Unable to find MySQL Client in location %s"

DEFINE_SHARD_MAPPING = _events.Event("DEFINE_SHARD_MAPPING")
class DefineShardMapping(ProcedureShard):
    """Define a shard mapping.
    """
    group_name = "sharding"
    command_name = "create_definition"
    def execute(self, type_name, group_id, synchronous=True):
        """Define a shard mapping.

        :param type_name: The type of sharding scheme - RANGE, HASH, LIST etc
        :param group_id: Every shard mapping is associated with a global group
                         that stores the global updates and the schema changes
                         for this shard mapping and dissipates these to the
                         shards.
        """
        procedures = _events.trigger(
            DEFINE_SHARD_MAPPING, self.get_lockable_objects(),
            type_name, group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

ADD_SHARD_MAPPING = _events.Event("ADD_SHARD_MAPPING")
class AddShardMapping(ProcedureShard):
    """Add a table to a shard mapping.
    """
    group_name = "sharding"
    command_name = "add_table"
    def execute(self, shard_mapping_id, table_name, column_name,
                synchronous=True):
        """Add a table to a shard mapping.

        :param shard_mapping_id: The shard mapping id to which the input
                                    table is attached.
        :param table_name: The table being sharded.
        :param column_name: The column whose value is used in the sharding
                            scheme being applied
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """

        procedures = _events.trigger(
            ADD_SHARD_MAPPING, self.get_lockable_objects(),
            shard_mapping_id, table_name, column_name
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD_MAPPING = _events.Event("REMOVE_SHARD_MAPPING")
class RemoveShardMapping(ProcedureShard):
    """Remove the shard mapping represented by the Shard Mapping object.
    """
    group_name = "sharding"
    command_name = "remove_table"
    def execute(self, table_name, synchronous=True):
        """Remove the shard mapping corresponding to the table passed as input.
        This method is exposed through the XML-RPC framework and creates a job
        and enqueues it in the executor.

        :param table_name: The name of the table whose sharding specification is
                            being removed.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        procedures = _events.trigger(
            REMOVE_SHARD_MAPPING, self.get_lockable_objects(), table_name
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD_MAPPING_DEFN = _events.Event("REMOVE_SHARD_MAPPING_DEFN")
class RemoveShardMappingDefn(ProcedureShard):
    """Remove the shard mapping definition represented by the Shard Mapping
    ID.
    """
    group_name = "sharding"
    command_name = "remove_definition"
    def execute(self, shard_mapping_id, synchronous=True):
        """Remove the shard mapping definition represented by the Shard Mapping
        ID. This method is exposed through the XML-RPC framework and creates a
        job and enqueues it in the executor.

        :param shard_mapping_id: The shard mapping ID of the shard mapping
                                definition that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        procedures = _events.trigger(
            REMOVE_SHARD_MAPPING_DEFN, self.get_lockable_objects(), shard_mapping_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class LookupShardMapping(Command):
    """Fetch the shard specification mapping for the given table
    """
    group_name = "sharding"
    command_name = "lookup_table"
    def execute(self, table_name):
        """Fetch the shard specification mapping for the given table

        :param table_name: The name of the table for which the sharding
                           specification is being queried.

        :return: The a dictionary that contains the shard mapping information
                 for the given table.
        """
        return Command.generate_output_pattern(_lookup_shard_mapping,
                                               table_name)

class ListShardMappings(Command):
    """Returns all the shard mappings of a particular
    sharding_type.
    """
    group_name = "sharding"
    command_name = "list_tables"
    def execute(self, sharding_type):
        """The method returns all the shard mappings (names) of a
        particular sharding_type. For example if the method is called
        with 'range' it returns all the sharding specifications that exist
        of type range.

        :param sharding_type: The sharding type for which the sharding
                              specification needs to be returned.

        :return: A list of dictionaries of shard mappings that are of
                     the sharding type
                     An empty list of the sharding type is valid but no
                     shard mapping definition is found
                     An error if the sharding type is invalid.
        """
        return Command.generate_output_pattern(_list, sharding_type)

class ListShardMappingDefinitions(Command):
    """Lists all the shard mapping definitions.
    """
    group_name = "sharding"
    command_name = "list_definitions"
    def execute(self):
        """The method returns all the shard mapping definitions.

        :return: A list of shard mapping definitions
                    An Empty List if no shard mapping definition is found.
        """
        return Command.generate_output_pattern(
                            ShardMapping.list_shard_mapping_defn)

ADD_SHARD = _events.Event("ADD_SHARD")
class AddShard(ProcedureShard):
    """Add a shard.
    """
    group_name = "sharding"
    command_name = "add_shard"
    def execute(self, shard_mapping_id, groupid_lb_list, state="DISABLED",
                synchronous=True):
        """Add the RANGE shard specification. This represents a single instance
        of a shard specification that maps a key RANGE to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param state: Indicates whether a given shard is ENABLED or DISABLED
        :param groupid_lb_list: The list of group_id, lower_bounds pairs in
                                the format, group_id/lower_bound,
                                group_id/lower_bound...
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        :return: A dictionary representing the current Range specification.
        """
        procedures = _events.trigger(ADD_SHARD, self.get_lockable_objects(),
            shard_mapping_id, groupid_lb_list, state
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD = \
        _events.Event("REMOVE_SHARD")
class RemoveShard(ProcedureShard):
    """Remove a Shard.
    """
    group_name = "sharding"
    command_name = "remove_shard"
    def execute(self, shard_id, synchronous=True):
        """Remove the RANGE specification mapping represented by the current
        RANGE shard specification object.

        :param shard_id: The shard ID of the shard that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                        or not.
        """

        procedures = _events.trigger(
            REMOVE_SHARD, self.get_lockable_objects(), shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

SHARD_ENABLE = \
        _events.Event("SHARD_ENABLE")
class EnableShard(ProcedureShard):
    """Enable a shard.
    """
    group_name = "sharding"
    command_name = "enable_shard"
    def execute(self, shard_id, synchronous=True):
        """Enable the Shard represented by the shard_id.

        :param shard_id: The shard ID of the shard that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                        or not.
        """
        procedures = _events.trigger(
            SHARD_ENABLE, self.get_lockable_objects(), shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

SHARD_DISABLE = \
        _events.Event("SHARD_DISABLE")
class DisableShard(ProcedureShard):
    """Disable a shard.
    """
    group_name = "sharding"
    command_name = "disable_shard"
    def execute(self, shard_id, synchronous=True):
        """Disable the Shard represented by the shard_id.

        :param shard_id: The shard ID of the shard that needs to be removed.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """

        procedures = _events.trigger(
            SHARD_DISABLE, self.get_lockable_objects(), shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class LookupShardServers(Command):
    """Lookup a shard based on the give sharding key.
    """
    group_name = "sharding"
    command_name = "lookup_servers"
    def execute(self, table_name, key, hint="LOCAL"):
        """Given a table name and a key return the server where the shard of
        this table can be found.

        :param table_name: The table whose sharding specification needs to be
                            looked up.
        :param key: The key value that needs to be looked up
        :param hint: A hint indicates if the query is LOCAL or GLOBAL

        :return: The Group UUID that contains the range in which the key
                 belongs.
        """
        return Command.generate_output_pattern(_lookup, table_name, key, hint)

PRUNE_SHARD_TABLES = _events.Event("PRUNE_SHARD_TABLES")
class PruneShardTables(ProcedureShard):
    """Given the table name prune the tables according to the defined
    sharding specification for the table.
    """
    group_name = "sharding"
    command_name = "prune_shard"
    def execute(self, table_name, synchronous=True):
        """Given the table name prune the tables according to the defined
        sharding specification for the table. The command prunes all the
        tables that are part of this shard. There might be multiple tables that
        are part of the same shard, these tables will be related together by
        the same sharding key.

        :param table_name: The table that needs to be sharded.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        procedures = _events.trigger(
            PRUNE_SHARD_TABLES, self.get_lockable_objects(), table_name
        )
        return self.wait_for_procedures(procedures, synchronous)

CHECK_SHARD_INFORMATION = _events.Event("CHECK_SHARD_INFORMATION")
BACKUP_SOURCE_SHARD = _events.Event("BACKUP_SOURCE_SHARD")
RESTORE_SHARD_BACKUP = _events.Event("RESTORE_SHARD_BACKUP")
SETUP_MOVE_SYNC = _events.Event("SETUP_MOVE_SYNC")
SETUP_RESHARDING_SWITCH = _events.Event("SETUP_RESHARDING_SWITCH")
PRUNE_SHARDS = _events.Event("PRUNE_SHARDS")
class MoveShardServer(ProcedureShard):
    """Move the shard represented by the shard_id to the destination group.

    By default this operation takes a backup, restores it on the destination
    group and guarantees that source and destination groups are synchronized
    before pointing the shard to the new group. If users just want to update
    the state store and skip these provisioning steps, the update_only
    parameter must be set to true.
    """
    group_name = "sharding"
    command_name = "move_shard"
    def execute(self, shard_id, group_id, update_only=False,
                synchronous=True):
        """Move the shard represented by the shard_id to the destination group.

        :param shard_id: The ID of the shard that needs to be moved.
        :param group_id: The ID of the group to which the shard needs to
                         be moved.
        :update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution finishes
                        or not.
        """
        mysqldump_binary = _read_config_value(self.config, 'sharding',
                                            'mysqldump_program')
        mysqlclient_binary = _read_config_value(self.config, 'sharding',
                                                'mysqlclient_program')

        procedures = _events.trigger(
            CHECK_SHARD_INFORMATION, self.get_lockable_objects(), shard_id,
            group_id, mysqldump_binary, mysqlclient_binary, None, "MOVE",
            update_only
        )
        return self.wait_for_procedures(procedures, synchronous)

class SplitShardServer(ProcedureShard):
    """Split the shard represented by the shard_id into the destination group.

    By default this operation takes a backup, restores it on the destination
    group and guarantees that source and destination groups are synchronized
    before pointing the shard to the new group. If users just want to update
    the state store and skip these provisioning steps, the update_only
    parameter must be set to true.

    """
    group_name = "sharding"
    command_name = "split_shard"
    def execute(self, shard_id,  group_id,  split_value = None,
                update_only=False, synchronous=True):
        """Split the shard represented by the shard_id into the destination
        group.

        :param shard_id: The shard_id of the shard that needs to be split.
        :param group_id: The ID of the group into which the split data needs
                         to be moved.
        :param split_value: The value at which the range needs to be split.
        :update_only: Only update the state store and skip provisioning.
        :param synchronous: Whether one should wait until the execution
                            finishes
        """
        mysqldump_binary = _read_config_value(self.config, 'sharding',
                                            'mysqldump_program')
        mysqlclient_binary =  _read_config_value(self.config, 'sharding',
                                                'mysqlclient_program')

        procedures = _events.trigger(
            CHECK_SHARD_INFORMATION, self.get_lockable_objects(),
            shard_id, group_id, mysqldump_binary, mysqlclient_binary,
            split_value, "SPLIT", update_only)
        return self.wait_for_procedures(procedures, synchronous)

class DumpShardTables(Command):
    """Return information about all tables belonging to mappings
    matching any of the provided patterns. If no patterns are provided,
    dump information about all tables.
    """
    group_name = "dump"
    command_name = "shard_tables"

    def execute(self, connector_version=None, patterns=""):
        """Return information about all tables belonging to mappings
        matching any of the provided patterns.

        :param connector_version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        """
        return ShardMapping.dump_shard_tables(connector_version, patterns)

class DumpShardingInformation(Command):
    """Return all the sharding information about the tables passed as patterns.
    If no patterns are provided, dump sharding information about all tables.
    """
    group_name = "dump"
    command_name = "sharding_information"

    def execute(self, connector_version=None, patterns=""):
        """Return all the sharding information about the tables passed as
        patterns. If no patterns are provided, dump sharding information
        about all tables.

        :param connector_version: The connectors version of the data.
        :param patterns: shard table pattern.
        """
        return ShardMapping.dump_sharding_info(connector_version, patterns)

class DumpShardMappings(Command):
    """Return information about all shard mappings matching any of the
    provided patterns. If no patterns are provided, dump information about
    all shard mappings.
    """
    group_name = "dump"
    command_name = "shard_maps"

    def execute(self, connector_version=None, patterns=""):
        """Return information about all shard mappings matching any of the
        provided patterns.

        :param connector_version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        """
        return ShardMapping.dump_shard_maps(connector_version, patterns)

class DumpShardIndex(Command):
    """Return information about the index for all mappings matching
    any of the patterns provided. If no pattern is provided, dump the
    entire index. The lower_bound that is returned is a string that is
    a md-5 hash of the group-id in which the data is stored.
    """
    group_name = "dump"
    command_name = "shard_index"

    def execute(self, connector_version=None, patterns=""):
        """Return information about the index for all mappings matching
        any of the patterns provided.

        :param connector_version: The connectors version of the data.
        :param patterns: group pattern.
        """
        return Shards.dump_shard_indexes(connector_version, patterns)

@_events.on_event(DEFINE_SHARD_MAPPING)
def _define_shard_mapping(type_name, global_group_id):
    """Define a shard mapping.

    :param type_name: The type of sharding scheme - RANGE, HASH, LIST etc
    :param global_group: Every shard mapping is associated with a
                        Global Group that stores the global updates
                        and the schema changes for this shard mapping
                        and dissipates these to the shards.
    :return: The shard_mapping_id generated for the shard mapping.
    :raises: ShardingError if the sharding type is invalid.
    """
    type_name = type_name.upper()
    if type_name not in Shards.VALID_SHARDING_TYPES:
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % (type_name, ))
    shard_mapping_id = ShardMapping.define(type_name, global_group_id)
    return shard_mapping_id

@_events.on_event(ADD_SHARD_MAPPING)
def _add_shard_mapping(shard_mapping_id, table_name, column_name):
    """Add a table to a shard mapping.

    :param shard_mapping_id: The shard mapping id to which the input
                                table is attached.
    :param table_name: The table being sharded.
    :param column_name: The column whose value is used in the sharding
                        scheme being applied

    :return: True if the the table was successfully added.
                False otherwise.
    """
    ShardMapping.add(shard_mapping_id, table_name, column_name)

@_events.on_event(REMOVE_SHARD_MAPPING)
def _remove_shard_mapping(table_name):
    """Remove the shard mapping for the given table.

    :param table_name: The name of the table for which the shard mapping
                        needs to be removed.

    :return: True if the remove succeeded
            False if the query failed
    :raises: ShardingError if the table name is not found.
    """
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping is None:
        raise _errors.ShardingError(TABLE_NAME_NOT_FOUND % (table_name, ))
    shard_mapping.remove()

@_events.on_event(REMOVE_SHARD_MAPPING_DEFN)
def _remove_shard_mapping_defn(shard_mapping_id):
    """Remove the shard mapping definition of the given table.

    :param shard_mapping_id: The shard mapping ID of the shard mapping
                            definition that needs to be removed.
    """
    ShardMapping.remove_sharding_definition(shard_mapping_id)

def _lookup_shard_mapping(table_name):
    """Fetch the shard specification mapping for the given table

    :param table_name: The name of the table for which the sharding
                        specification is being queried.

    :return: A dictionary that contains the shard mapping information for
                the given table.
    """
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping is not None:
        return {"shard_mapping_id":shard_mapping.shard_mapping_id,
                "table_name":shard_mapping.table_name,
                "column_name":shard_mapping.column_name,
                "type_name":shard_mapping.type_name,
                "global_group":shard_mapping.global_group}
    else:
        #We return an empty shard mapping because if an Error is thrown
        #it would cause the executor to rollback which is an unnecessary
        #action. It is enough if we inform the user that the lookup returned
        #nothing.
        return {"shard_mapping_id":"",
                "table_name":"",
                "column_name":"",
                "type_name":"",
                "global_group":""}

def _list(sharding_type):
    """The method returns all the shard mappings (names) of a
    particular sharding_type. For example if the method is called
    with 'range' it returns all the sharding specifications that exist
    of type range.

    :param sharding_type: The sharding type for which the sharding
                          specification needs to be returned.

    :return: A list of dictionaries of shard mappings that are of
                 the sharding type
                 An empty list of the sharding type is valid but no
                 shard mapping definition is found
                 An error if the sharding type is invalid.

    :raises: Sharding Error if Sharding type is not found.
    """
    if sharding_type not in Shards.VALID_SHARDING_TYPES:
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % (sharding_type,))

    ret_shard_mappings = []
    shard_mappings = ShardMapping.list(sharding_type)
    for shard_mapping in shard_mappings:
        ret_shard_mappings.append({
                    "shard_mapping_id":shard_mapping.shard_mapping_id,
                    "table_name":shard_mapping.table_name,
                    "column_name":shard_mapping.column_name,
                    "type_name":shard_mapping.type_name,
                    "global_group":shard_mapping.global_group})
    return ret_shard_mappings

@_events.on_event(ADD_SHARD)
def _add_shard(shard_mapping_id, groupid_lb_list, state):
    """Add the RANGE shard specification. This represents a single instance
    of a shard specification that maps a key RANGE to a server.

    :param shard_mapping_id: The unique identification for a shard mapping.
    :param groupid_lb_list: The list of group_id, lower_bounds pairs in the
                        format, group_id/lower_bound, group_id/lower_bound... .
    :param state: Indicates whether a given shard is ENABLED or DISABLED

    :return: True if the add succeeded.
                False otherwise.
    :raises: ShardingError If the group on which the shard is being
                           created does not exist,
                           If the shard_mapping_id is not found,
                           If adding the shard definition fails,
                           If the state of the shard is an invalid
                           value,
                           If the range definition is invalid.
    """
    shard_mapping = ShardMapping.fetch_shard_mapping_defn(shard_mapping_id)
    if shard_mapping is None:
        raise _errors.ShardingError(SHARD_MAPPING_NOT_FOUND % \
                                                    (shard_mapping_id,  ))

    schema_type = shard_mapping[1]

    if len(RangeShardingSpecification.list(shard_mapping_id)) != 0:
        raise _errors.ShardingError(SHARDS_ALREADY_EXIST)

    group_id_list, lower_bound_list =\
        _utils.get_group_lower_bound_list(groupid_lb_list)

    if (len(group_id_list) != len(lower_bound_list)) and\
        schema_type == "RANGE":
        raise _errors.ShardingError(LOWER_BOUND_GROUP_ID_COUNT_MISMATCH)

    if len(lower_bound_list) != 0 and schema_type == "HASH":
        raise _errors.ShardingError(LOWER_BOUND_AUTO_GENERATED)

    if schema_type == "RANGE":
        for lower_bound in lower_bound_list:
            if(not SHARDING_DATATYPE_HANDLER[schema_type].\
                        is_valid_lower_bound(lower_bound)):
                raise _errors.ShardingError(
                                INVALID_LOWER_BOUND_VALUE % (lower_bound, ))

    state = state.upper()
    if state not in Shards.VALID_SHARD_STATES:
        raise _errors.ShardingError(INVALID_SHARD_STATE % (state,  ))

    for index, group_id in enumerate(group_id_list):
        shard = Shards.add(group_id, state)

        shard_id = shard.shard_id

        if schema_type == "RANGE":
            range_sharding_specification = RangeShardingSpecification.add(
                                                shard_mapping_id,
                                                lower_bound_list[index],
                                                shard_id
                                            )
            _LOGGER.debug(
                "Added Shard (map id = %s, lower bound = %s, id = %s).",
                range_sharding_specification.shard_mapping_id,
                range_sharding_specification.lower_bound,
                range_sharding_specification.shard_id
            )
        elif schema_type == "HASH":
            HashShardingSpecification.add(
                shard_mapping_id,
                shard_id
            )
            _LOGGER.debug(
                "Added Shard (map id = %s, id = %s).",
                shard_mapping_id,
                shard_id
            )
        else:
            raise _errors.ShardingError(INVALID_SHARDING_TYPE % (schema_type, ))

        #If the shard is added in a DISABLED state  do not setup replication
        #with the primary of the global group. Basically setup replication only
        #if the shard is ENABLED.
        if state == "ENABLED":
            _setup_shard_group_replication(shard_id)

@_events.on_event(REMOVE_SHARD)
def _remove_shard(shard_id):
    """Remove the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True if the remove succeeded
            False if the query failed
    :raises: ShardingError if the shard id is not found,
        :       ShardingError if the shard is not disabled.
    """
    range_sharding_specification, shard, _, _ = \
        _verify_and_fetch_shard(shard_id)
    if shard.state == "ENABLED":
        raise _errors.ShardingError(SHARD_NOT_DISABLED)
    #Stop the replication of the shard group with the global
    #group. Also clear the references of the master and the
    #slave group from the current group.
    #NOTE: When we do the stopping of the shard group
    #replication in shard remove we are actually just clearing
    #the references, since a shard cannot  be removed unless
    #it is disabled and when it is disabled the replication is
    #stopped but the references are not cleared.
    _stop_shard_group_replication(shard_id,  True)
    range_sharding_specification.remove()
    shard.remove()
    _LOGGER.debug("Removed Shard (%s).", shard_id)

def _lookup(lookup_arg, key,  hint):
    """Given a table name and a key return the servers of the Group where the
    shard of this table can be found

    :param lookup_arg: table name for "LOCAL" lookups
                Shard Mapping ID for "GLOBAL" lookups.
    :param key: The key value that needs to be looked up
    :param hint: A hint indicates if the query is LOCAL or GLOBAL

    :return: The servers of the Group that contains the range in which the
            key belongs.
    """
    VALID_HINTS = ('LOCAL',  'GLOBAL')
    hint = hint.upper()
    if hint not in VALID_HINTS:
        raise _errors.ShardingError(INVALID_SHARDING_HINT)

    group = None

    #Perform the lookup for the group contaning the lookup data.
    if hint == "GLOBAL":
        #Fetch the shard mapping object. In the case of GLOBAL lookups
        #the shard mapping ID is passed directly. In the case of "LOCAL"
        #lookups it is the table name that is passed.
        shard_mapping = ShardMapping.fetch_by_id(lookup_arg)
        if shard_mapping is None:
            raise _errors.ShardingError(
                SHARD_MAPPING_NOT_FOUND % (lookup_arg,  )
                )
        #GLOBAL lookups. There can be only one global group, hence using
        #shard_mapping[0] is safe.
        group = Group.fetch(shard_mapping[0].global_group)
    else:
        shard_mapping = ShardMapping.fetch(lookup_arg)
        if shard_mapping is None:
            raise _errors.ShardingError(TABLE_NAME_NOT_FOUND % (lookup_arg,  ))
        sharding_specification =\
            SHARDING_SPECIFICATION_HANDLER[shard_mapping.type_name].\
            lookup(key, shard_mapping.shard_mapping_id, shard_mapping.type_name)
        if sharding_specification is None:
            raise _errors.ShardingError(INVALID_SHARDING_KEY % (key,  ))
        shard = Shards.fetch(str(sharding_specification.shard_id))
        if shard.state == "DISABLED":
            raise _errors.ShardingError(SHARD_NOT_ENABLED)
        #group cannot be None since there is a foreign key on the group_id.
        #An exception will be thrown nevertheless.
        group = Group.fetch(shard.group_id)
        if group is None:
            raise _errors.ShardingError(SHARD_LOCATION_NOT_FOUND)

    ret = []
    #An empty list will be returned if the registered group has not
    #servers.
    for server in group.servers():
        ret.append([str(server.uuid), server.address,
                   group.master == server.uuid])
    return ret

@_events.on_event(SHARD_ENABLE)
def _enable_shard(shard_id):
    """Enable the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True Placeholder return value
    :raises: ShardingError if the shard_id is not found.
    """
    _, shard, _, _ = _verify_and_fetch_shard(shard_id)
    #When you enable a shard, setup replication with the global server
    #of the shard mapping associated with this shard.
    _setup_shard_group_replication(shard_id)
    shard.enable()

@_events.on_event(SHARD_DISABLE)
def _disable_shard(shard_id):
    """Disable the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True Placeholder return value
    :raises: ShardingError if the shard_id is not found.
    """
    _, shard, _, _ = _verify_and_fetch_shard(shard_id)
    #When you disable a shard, disable replication with the global server
    #of the shard mapping associated with the shard.
    _stop_shard_group_replication(shard_id,  False)
    shard.disable()

@_events.on_event(PRUNE_SHARD_TABLES)
def _prune_shard_tables(table_name):
    """Delete the data from the copied data directories based on the
    sharding configuration uploaded in the sharding tables of the state
    store. The basic logic consists of

    a) Querying the sharding scheme name corresponding to the sharding table
    b) Querying the sharding key range using the sharding scheme name.
    c) Deleting the sharding keys that fall outside the range for a given
        server.

    :param table_name: The table_name who's shards need to be pruned.
    """
    shard_mapping = ShardMapping.fetch(table_name)
    SHARDING_SPECIFICATION_HANDLER[shard_mapping.type_name].delete_from_shard_db\
        (table_name, shard_mapping.type_name)

def _is_valid_binary(binary):
    """Prints if the binary was found in the given path.

    :param binary: The full path to the binary that needs to be verified.

    :return True: If the binary was found
        False: If the binary was not found.
    """
    import os
    return os.path.isfile(binary) and os.access(binary, os.X_OK)

@_events.on_event(CHECK_SHARD_INFORMATION)
def _check_shard_information(shard_id,  destn_group_id, mysqldump_binary,
                             mysqlclient_binary, split_value, cmd,
                             update_only):
    """Verify the sharding information before starting a re-sharding operation.

    :param shard_id: The destination shard ID.
    :param destn_group_id: The Destination group ID.
    :param mysqldump_binary: The path to the mysqldump binary.
    :param mysqlclient_binary: The path to the mysqlclient binary.
    :param split_value: The point at which the sharding definition should be split.
    :param cmd: Indicates if it is a split or a move being executed.
    :param update_only: If the operation is a update only operation.
    """

    if not _is_valid_binary(mysqldump_binary):
        raise _errors.ShardingError(MYSQLDUMP_NOT_FOUND % mysqldump_binary)

    if not _is_valid_binary(mysqlclient_binary):
        raise _errors.ShardingError(MYSQLCLIENT_NOT_FOUND % mysqlclient_binary)

    if cmd == "SPLIT":
        range_sharding_spec, _,  shard_mappings, _ = \
            _verify_and_fetch_shard(shard_id)
        upper_bound =\
            SHARDING_SPECIFICATION_HANDLER[shard_mappings[0].type_name].\
                        get_upper_bound(
                            range_sharding_spec.lower_bound,
                            range_sharding_spec.shard_mapping_id,
                            shard_mappings[0].type_name
                          )
        #If the underlying sharding scheme is a HASH. When a shard is split, all
        #the tables that are part of the shard, have the same sharding scheme.
        #All the shard mappings associated with this shard_id will be of the
        #same sharding type. Hence it is safe to use one of the shard mappings.
        if shard_mappings[0].type_name == "HASH":
            if split_value is not None:
                raise _errors.ShardingError(NO_LOWER_BOUND_FOR_HASH_SHARDING)
            if  upper_bound is None:
                #While splitting a range, retrieve the next upper bound and
                #find the mid-point, in the case where the next upper_bound
                #is unavailable pick the maximum value in the set of values in
                #the shard.
                upper_bound = HashShardingSpecification.fetch_max_key(shard_id)

            #Calculate the split value.
            split_value =\
                SHARDING_DATATYPE_HANDLER[shard_mappings[0].type_name].\
                split_value(
                    range_sharding_spec.lower_bound,
                    upper_bound
                )
        elif shard_mappings[0].type_name == "RANGE" and split_value is not None:
            if not (SHARDING_DATATYPE_HANDLER[shard_mappings[0].type_name].\
                    is_valid_split_value(
                        split_value, range_sharding_spec.lower_bound,
                        upper_bound
                    )
                ):
                raise _errors.ShardingError(INVALID_LOWER_BOUND_VALUE % 
                                            (split_value, ))
        elif shard_mappings[0].type_name == "RANGE" and split_value is None:
            raise _errors.ShardingError(SPLIT_VALUE_NOT_DEFINED)

    #Ensure that the group does not already contain a shard.
    if (Shards.lookup_shard_id(destn_group_id) is not None):
        raise _errors.ShardingError(
            SHARD_MOVE_DESTINATION_NOT_EMPTY % (destn_group_id, )
        )

    #Fetch the group information for the source shard that
    #needs to be moved.
    source_shard = Shards.fetch(shard_id)
    if source_shard is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id, ))

    #Fetch the group_id and the group that hosts the source shard.
    source_group_id = source_shard.group_id
    source_group = Group.fetch(source_group_id)

    destn_group = Group.fetch(destn_group_id)
    if destn_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND % (destn_group_id, ))

    if not update_only:
        _events.trigger_within_procedure(
            BACKUP_SOURCE_SHARD, shard_id, source_group_id, destn_group_id,
            mysqldump_binary, mysqlclient_binary, split_value, cmd,
            update_only
        )
    else:
        _events.trigger_within_procedure(
            SETUP_RESHARDING_SWITCH, shard_id, source_group_id, destn_group_id,
            split_value, cmd, update_only
        )

@_events.on_event(BACKUP_SOURCE_SHARD)
def _backup_source_shard(shard_id, source_group_id, destn_group_id,
                         mysqldump_binary, mysqlclient_binary, split_value,
                         cmd, update_only):
    """Backup the source shard.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param mysqldump_binary: The fully qualified mysqldump binary.
    :param mysqlclient_binary: The fully qualified mysql client binary.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param cmd: Indicates the type of re-sharding operation (move, split)
    :update_only: Only update the state store and skip provisioning.
    """
    source_group = Group.fetch(source_group_id)
    move_source_server = _fetch_backup_server(source_group)

    #Do the backup of the group hosting the source shard.
    backup_image = _backup.MySQLDump.backup(
                        move_source_server,
                        mysqldump_binary
                    )

    #Change the master for the server that is master of the group which hosts
    #the destination shard.
    _events.trigger_within_procedure(
                                     RESTORE_SHARD_BACKUP,
                                     shard_id,
                                     source_group_id,
                                     destn_group_id,
                                     mysqlclient_binary,
                                     backup_image.path,
                                     split_value,
                                     cmd
                                     )

@_events.on_event(RESTORE_SHARD_BACKUP)
def _restore_shard_backup(shard_id,  source_group_id, destn_group_id,
                            mysqlclient_binary, backup_image,
                            split_value, cmd):
    """Restore the backup on the destination Group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param mysqlclient_binary: The fully qualified mysqlclient binary.
    :param backup_image: The destination file that contains the backup
                         of the source shard.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param cmd: Indicates the type of re-sharding operation
    """
    destn_group = Group.fetch(destn_group_id)
    if destn_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND % (destn_group_id, ))

    #Build a backup image that will be used for restoring
    bk_img = _backup.BackupImage(backup_image)

    for destn_group_server in destn_group.servers():
        destn_group_server.connect()
        _backup.MySQLDump.restore(
            destn_group_server,
            bk_img,
            mysqlclient_binary
        )

    #Setup sync between the source and the destination groups.
    _events.trigger_within_procedure(
                                     SETUP_MOVE_SYNC,
                                     shard_id,
                                     source_group_id,
                                     destn_group_id,
                                     split_value,
                                     cmd
                                     )

@_events.on_event(SETUP_MOVE_SYNC)
def _setup_move_sync(shard_id, source_group_id, destn_group_id, split_value,
                                        cmd):
    """Setup replication between the source and the destination groups and
    ensure that they are in sync.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param cmd: Indicates the type of re-sharding operation
    """
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND % (source_group_id, ))

    destination_group = Group.fetch(destn_group_id)
    if destination_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND % (destination_group_id, ))

    master = MySQLServer.fetch(source_group.master)
    if master is None:
        raise _errors.ShardingError(SHARD_GROUP_MASTER_NOT_FOUND)
    master.connect()

    slave = MySQLServer.fetch(destination_group.master)
    if slave is None:
        raise _errors.ShardingError(SHARD_GROUP_MASTER_NOT_FOUND)
    slave.connect()

    #Stop and reset any slave that  might be running on the slave server.
    _replication.stop_slave(slave, wait=True)
    _replication.reset_slave(slave, clean=True)

    #Change the master to the shard group master.
    _replication.switch_master(slave,  master,  master. user,  master.passwd)

    #Start the slave so that syncing of the data begins
    _replication.start_slave(slave, wait=True)

    #Synchronize until the slave catches up with the master.
    _replication.synchronize_with_read_only(slave, master)

    #Reset replication once the syncing is done.
    _replication.stop_slave(slave, wait=True)
    _replication.reset_slave(slave, clean=True)

    #Trigger changing the mappings for the shard that was copied
    _events.trigger_within_procedure(
                                     SETUP_RESHARDING_SWITCH,
                                     shard_id,
                                     source_group_id,
                                     destn_group_id,
                                     split_value,
                                     cmd
                                     )

@_events.on_event(SETUP_RESHARDING_SWITCH)
def _setup_resharding_switch(shard_id,  source_group_id,  destination_group_id,
                             split_value, cmd, update_only=False):
    """Setup the shard move or shard split workflow based on the command
    argument.

    :param shard_id: The ID of the shard that needs to be re-sharded.
    :param source_group_id: The ID of the source group.
    :param destination_group_id: The ID of the destination group.
    :param split_value: The value at which the shard needs to be split
                        (in the case of a shard split operation).
    :param cmd: whether the operation that needs to be split is a
                MOVE or a SPLIT operation.
    :param cmd: whether the operation that needs to be split is a
                MOVE or a SPLIT operation.
    :update_only: Only update the state store and skip provisioning.
    """
    if cmd == "MOVE":
        _setup_shard_switch_move(
            shard_id, source_group_id, destination_group_id,
            update_only
        )
    elif cmd == "SPLIT":
        _setup_shard_switch_split(
            shard_id,  source_group_id, destination_group_id, split_value,
            cmd, update_only
        )

def _setup_shard_switch_split(shard_id,  source_group_id,  destination_group_id,
                              split_value, cmd, update_only):
    """Setup the moved shard to map to the new group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                           be moved.
    :param split_value: Indicates the value at which the range for the
                        particular shard will be split. Will be set only
                        for shard split operations.
    :param cmd: Indicates the type of re-sharding operation.
    :update_only: Only update the state store and skip provisioning.
    """
    #Fetch the Range sharding specification.
    range_sharding_spec, source_shard,  shard_mappings,  shard_mapping_defn = \
            _verify_and_fetch_shard(shard_id)

    #Disable the old shard
    source_shard.disable()

    #Remove the old shard.
    range_sharding_spec.remove()
    source_shard.remove()

    #Add the new shards. Generate new shard IDs for the shard being
    #split and also for the shard that is created as a result of the split.
    new_shard_1 = Shards.add(source_shard.group_id, "DISABLED")
    new_shard_2 = Shards.add(destination_group_id, "DISABLED")

    #Both of the shard mappings associated with this shard_id should
    #be of the same sharding type. Hence it is safe to use one of the
    #shard mappings.
    if shard_mappings[0].type_name == "HASH":
        #In the case of a split involving a HASH sharding scheme,
        #the shard that is split gets a new shard_id, while the split
        #gets the new computed lower_bound and also a new shard id.
        #NOTE: How the shard that is split retains its lower_bound.
        HashShardingSpecification.add_hash_split(
            range_sharding_spec.shard_mapping_id,
            new_shard_1.shard_id,
            range_sharding_spec.lower_bound
        )
        HashShardingSpecification.add_hash_split(
            range_sharding_spec.shard_mapping_id,
            new_shard_2.shard_id,
            split_value
        )
    else:
        #Add the new ranges. Note that the shard being split retains
        #its lower_bound, while the new shard gets the computed,
        #lower_bound.
        RangeShardingSpecification.add(
            range_sharding_spec.shard_mapping_id,
            range_sharding_spec.lower_bound,
            new_shard_1.shard_id
        )
        RangeShardingSpecification.add(
            range_sharding_spec.shard_mapping_id,
            split_value,
            new_shard_2.shard_id
        )

    #The source shard group master would have been marked as read only
    #during the sync. Remove the read_only flag.
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND % (source_group_id, ))

    source_group_master = MySQLServer.fetch(source_group.master)
    if source_group_master is None:
        raise _errors.ShardingError(SHARD_GROUP_MASTER_NOT_FOUND)
    source_group_master.connect()
    source_group_master.read_only = False

    #Setup replication for the new group from the global server
    _group_replication.setup_group_replication \
            (shard_mapping_defn[2],  destination_group_id)

    #Enable the split shards
    new_shard_1.enable()
    new_shard_2.enable()

    #Trigger changing the mappings for the shard that was copied
    if not update_only:
        _events.trigger_within_procedure(
            PRUNE_SHARDS, new_shard_1.shard_id, new_shard_2.shard_id
        )

@_events.on_event(PRUNE_SHARDS)
def _prune_shard_tables_after_split(shard_id_1, shard_id_2):
    """Prune the two shards generated after a split.

    :param shard_id_1: The first shard id after the split.
    :param shard_id_2: The second shard id after the split.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, _,  shard_mappings,  _ = _verify_and_fetch_shard(shard_id_1)

    #All the shard mappings associated with this shard_id should be
    #of the same type. Hence it is safe to use one of them.
    SHARDING_SPECIFICATION_HANDLER[shard_mappings[0].type_name].\
    prune_shard_id(shard_id_1, shard_mappings[0].type_name)
    SHARDING_SPECIFICATION_HANDLER[shard_mappings[0].type_name].\
    prune_shard_id(shard_id_2, shard_mappings[0].type_name)

def _setup_shard_switch_move(shard_id,  source_group_id, destination_group_id,
                             update_only):
    """Setup the moved shard to map to the new group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destination_group_id: The ID of the group to which the shard needs to
                                 be moved.
    :update_only: Only update the state store and skip provisioning.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, source_shard,  _,  shard_mapping_defn = \
        _verify_and_fetch_shard(shard_id)

    #Setup replication between the shard group and the global group.
    _group_replication.setup_group_replication \
            (shard_mapping_defn[2],  destination_group_id)
    #set the shard to point to the new group.
    source_shard.group_id = destination_group_id
    #Stop the replication between the global server and the original
    #group associated with the shard.
    _group_replication.stop_group_slave\
            (shard_mapping_defn[2],  source_group_id,  True)

    #Reset the read only flag on the source server.
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND % (source_group_id, ))

    master = MySQLServer.fetch(source_group.master)
    if master is None:
        raise _errors.ShardingError(SHARD_GROUP_MASTER_NOT_FOUND)

    if not update_only:
        master.connect()
        master.read_only = False

def _fetch_backup_server(source_group):
    """Fetch a spare, slave or master from a group in that order of
    availability.

    :param source_group: The group from which the server needs to
                         be fetched.
    """
    #Get a slave server whose status is spare.
    backup_server = None
    for server in source_group.servers():
        if source_group.master != server.uuid and \
            server.status == "SPARE":
            backup_server = server

    #If there is no spare check if a running slave is available
    if backup_server is None:
        for server in source_group.servers():
            if source_group.master != server.uuid and \
                server.status == "SECONDARY":
                backup_server = server

    #If there is no running slave just use the master
    if backup_server is None:
        backup_server = MySQLServer.fetch(source_group.master)

    #If there is no master throw an exception
    if backup_server is None:
        raise _errors.ShardingError(SHARD_GROUP_MASTER_NOT_FOUND)

    return backup_server

def _verify_and_fetch_shard(shard_id):
    """Find out if the shard_id exists and return the sharding specification for
    it. If it does not exist throw an exception.

    :param shard_id: The ID for the shard whose specification needs to be
                     fetched.

    :return: The sharding specification class representing the shard ID.

    :raises: ShardingError if the shard ID is not found.
    """
    #Here the underlying sharding specification might be a RANGE
    #or a HASH. The type of sharding specification is obtained from the
    #shard mapping.
    range_sharding_spec = RangeShardingSpecification.fetch(shard_id)
    if range_sharding_spec is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id,  ))

    #Fetch the shard mappings and use them to find the type of sharding
    #scheme.
    shard_mappings = ShardMapping.fetch_by_id(
                        range_sharding_spec.shard_mapping_id
                    )
    if shard_mappings is None:
        raise _errors.ShardingError(
                    SHARD_MAPPING_NOT_FOUND % (
                        range_sharding_spec.shard_mapping_id,
                    )
                )

    #Fetch the shard mapping definition. There is only one shard mapping
    #definition associated with all of the shard mappings.
    shard_mapping_defn =  ShardMapping.fetch_shard_mapping_defn(
                        range_sharding_spec.shard_mapping_id
                    )
    if shard_mapping_defn is None:
        raise _errors.ShardingError(
                    SHARD_MAPPING_DEFN_NOT_FOUND % (
                        range_sharding_spec.shard_mapping_id,
                    )
                )

    shard = Shards.fetch(shard_id)
    if shard is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id,  ))

    #Both of the shard_mappings retrieved will be of the same sharding
    #type. Hence it is safe to use one of them to retireve the sharding type.
    if shard_mappings[0].type_name == "HASH":
        return HashShardingSpecification.fetch(shard_id), \
            shard,  shard_mappings, shard_mapping_defn
    else:
        return range_sharding_spec, shard, shard_mappings, shard_mapping_defn

def _setup_shard_group_replication(shard_id):
    """Setup the replication between the master group and the
    shard group. This is a utility method that given a shard id
    will lookup the group associated with this shard, and setup
    replication between the group and the global group.

    :param shard_id: The ID of the shard, whose group needs to
                     be setup as a slave.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, shard, _, shard_mapping_defn  = \
        _verify_and_fetch_shard(shard_id)

    #Setup replication between the shard group and the global group.
    _group_replication.setup_group_replication \
            (shard_mapping_defn[2],  shard.group_id)

def _stop_shard_group_replication(shard_id,  clear_ref):
    """Stop the replication between the master group and the shard group.

    :param shard_id: The ID of the shard, whose group needs to
                     be atopped as a slave.
    :param clear_ref: Indicates whether removing the shard should result
                      in the shard group losing all its slave group references.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    _, shard, _, shard_mapping_defn  = \
        _verify_and_fetch_shard(shard_id)

    #Stop the replication between the shard group and the global group. Also
    #based on the clear_ref flag decide if you want to clear the references
    #associated with the group.
    _group_replication.stop_group_slave(shard_mapping_defn[2],  shard.group_id,
                                                                clear_ref)

def _read_config_value(config,  config_group,  config_name):
    """Read the value of the configuration option from the config files.

    :param config: The config class that encapsulates the config parsing
                            logic.
    :param config_group: The configuration group to which the configuration
                                       belongs
    :param config_name: The name of the configuration that needs to be read,
    """
    config_value = None

    try:
        config_value =  config.get(config_group, config_name)
    except AttributeError:
        pass

    if config_value is None:
        raise _errors.ShardingError(CONFIG_NOT_FOUND %
                                    (config_group,  config_name))

    return config_value

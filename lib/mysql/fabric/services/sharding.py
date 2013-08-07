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
    sharding as _sharding,
    utils as _utils,
)

from mysql.fabric.server import (
    Group,
    MySQLServer,
)

from mysql.fabric.sharding import (
    ShardMapping,
    RangeShardingSpecification,
    HashShardingSpecification,
    Shards
)

from mysql.fabric.command import (
    ProcedureShard,
    ProcedureCommand,
    Command,
)

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
SHARD_GROUP_NOT_FOUND = "Shard group not found"
SHARD_GROUP_MASTER_NOT_FOUND = "Shard group master not found"
SHARD_MOVE_DESTINATION_NOT_EMPTY = "Shard move destination already "\
    "hosts a shard"
INVALID_SHARD_SPLIT_VALUE = "The chosen split value must be between the " \
                            "lower bound and upper bound of the shard"
CONFIG_NOT_FOUND = "Configuration option not found %s . %s"
INVALID_LOWER_BOUND = "Invalid lower_bound value for RANGE sharding specification"

DEFINE_SHARD_MAPPING = _events.Event("DEFINE_SHARD_MAPPING")
class DefineShardMapping(ProcedureShard):
    """Define a shard mapping.
    """
    group_name = "sharding"
    command_name = "define"
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
    command_name = "add_mapping"
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
    command_name = "remove_mapping"
    def execute(self, table_name, synchronous=True):
        """Remove the shard mapping represented by the Shard Mapping object.
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

LOOKUP_SHARD_MAPPING = _events.Event("LOOKUP_SHARD_MAPPING")
class LookupShardMapping(ProcedureCommand):
    """Fetch the shard specification mapping for the given table
    """
    group_name = "sharding"
    command_name = "lookup_mapping"
    def execute(self, table_name, synchronous=True):
        """Fetch the shard specification mapping for the given table

        :param table_name: The name of the table for which the sharding
                           specification is being queried.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        :return: The a dictionary that contains the shard mapping information for
                 the given table.
        """
        procedures = _events.trigger(
            LOOKUP_SHARD_MAPPING, self.get_lockable_objects(), table_name
        )
        return self.wait_for_procedures(procedures, synchronous)

LIST_SHARD_MAPPINGS = _events.Event("LIST_SHARD_MAPPINGS")
class ListShardMappings(ProcedureCommand):
    """Returns all the shard mappings of a particular
    sharding_type.
    """
    group_name = "sharding"
    command_name = "list_mappings"
    def execute(self, sharding_type, synchronous=True):
        """The method returns all the shard mappings (names) of a
        particular sharding_type. For example if the method is called
        with 'range' it returns all the sharding specifications that exist
        of type range.

        :param sharding_type: The sharding type for which the sharding
                              specification needs to be returned.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        :return: A list of dictionaries of shard mappings that are of
                     the sharding type
                     An empty list of the sharding type is valid but no
                     shard mapping definition is found
                     An error if the sharding type is invalid.
        """
        procedures = _events.trigger(
            LIST_SHARD_MAPPINGS, self.get_lockable_objects(), sharding_type
        )
        return self.wait_for_procedures(procedures, synchronous)

LIST_SHARD_MAPPING_DEFINITIONS = _events.Event("LIST_SHARD_MAPPING_DEFINITIONS")
class ListShardMappingDefinitions(ProcedureCommand):
    """Lists all the shard mapping definitions.
    """
    group_name = "sharding"
    command_name = "list_definitions"
    def execute(self, synchronous=True):
        """The method returns all the shard mapping definitions.

        :return: A list of shard mapping definitions
                    An Empty List if no shard mapping definition is found.
        """
        procedures = _events.trigger(
            LIST_SHARD_MAPPING_DEFINITIONS, self.get_lockable_objects()
        )
        return self.wait_for_procedures(procedures, synchronous)

ADD_SHARD = _events.Event("ADD_SHARD")
class AddShard(ProcedureShard):
    """Add a shard.
    """
    group_name = "sharding"
    command_name = "add_shard"
    def execute(self, shard_mapping_id, group_id, state="DISABLED",
                lower_bound="None", synchronous=True):
        """Add the RANGE shard specification. This represents a single instance
        of a shard specification that maps a key RANGE to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param group_id: The group that contains the shard information.
        :param state: Indicates whether a given shard is ENABLED or DISABLED
        :param lower_bound: The lower bound of the given RANGE sharding
                            definition, the parameter is optional for HASH
                            sharding. The lower bound is generated by hashing
                            the group ID for HASH sharding. Hence it is a
                            optional parameter that need not be set for HASH based
                            sharding.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        :return: A dictionary representing the current Range specification.
        """
        procedures = _events.trigger(ADD_SHARD, self.get_lockable_objects(),
            shard_mapping_id, lower_bound, group_id, state
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

LOOKUP_SHARD_SERVERS = \
        _events.Event("LOOKUP_SHARD_SERVERS")
class LookupShardServers(ProcedureCommand):
    """Lookup a shard based on the give sharding key.
    """
    group_name = "sharding"
    command_name = "lookup_servers"
    def execute(self, table_name, key, hint="LOCAL",  synchronous=True):
        """Given a table name and a key return the server where the shard of
        this table can be found.

        :param table_name: The table whose sharding specification needs to be
                            looked up.
        :param key: The key value that needs to be looked up
        :param hint: A hint indicates if the query is LOCAL or GLOBAL
        :param synchronous: Whether one should wait until the execution finishes
                        or not.

        :return: The Group UUID that contains the range in which the key belongs.
        """
        #TODO: A GLOBAL lookup should not pass a key. They key should point
        #TODO: to either  sentinel value or should be None. This case needs to
        #TODO: be handled.
        procedures = _events.trigger(
            LOOKUP_SHARD_SERVERS, self.get_lockable_objects(),
            table_name, key, hint
        )
        return self.wait_for_procedures(procedures, synchronous)

PRUNE_SHARD_TABLES = _events.Event("PRUNE_SHARD_TABLES")
class PruneShardTables(ProcedureShard):
    """Given the table name prune the tables according to the defined
    sharding specification for the table.
    """
    group_name = "sharding"
    command_name = "prune_shard"
    def execute(self, table_name, synchronous=True):
        """Given the table name prune the tables according to the defined
        sharding specification for the table.

        :param table_name: The table that needs to be sharded.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        """
        procedures = _events.trigger(
            PRUNE_SHARD_TABLES, self.get_lockable_objects(), table_name
        )
        return self.wait_for_procedures(procedures, synchronous)

BACKUP_SOURCE_SHARD =  _events.Event("BACKUP_SOURCE_SHARD")
RESTORE_SHARD_BACKUP = _events.Event("RESTORE_SHARD_BACKUP")
SETUP_MOVE_SYNC = _events.Event("SETUP_MOVE_SYNC")
SETUP_RESHARDING_SWITCH = _events.Event("SETUP_RESHARDING_SWITCH")
PRUNE_SHARDS = _events.Event("PRUNE_SHARDS")
class MoveShardServer(ProcedureShard):
    """Move the shard represented by the shard_id to the destination group.
    """
    group_name = "sharding"
    command_name = "move"
    def execute(self,  shard_id,  group_id,  synchronous=True):
        """Move the shard represented by the shard_id to the destination group.

        :param shard_id: The ID of the shard that needs to be moved.
        :param group_id: The ID of the group to which the shard needs to
                         be moved.
        :param synchronous: Whether one should wait until the execution finishes
                        or not.
        """
        #TODO: Add a configurable timeout option. The option will allow the
        #TODO: code to wait until timeout without taking a lock and after timeout
        #TODO: will take a read lock on the master.

        #TODO: Change MOVE to an integer constant

        #TODO: Once we have the subsystem init patch implemented, we
        #TODO: should read these programs from the configuration file
        #TODO: when initializing the module, not each time a command is
        #TODO: dispatched.

        mysqldump_binary = _read_config_value(self.config, 'sharding',
                                            'mysqldump_program')
        mysqlclient_binary = _read_config_value(self.config, 'sharding',
                                                'mysqlclient_program')

        procedures = _events.trigger(
            BACKUP_SOURCE_SHARD, self.get_lockable_objects(), shard_id,
            group_id, mysqldump_binary, mysqlclient_binary, None, "MOVE"
        )
        return self.wait_for_procedures(procedures, synchronous)

class SplitShardServer(ProcedureShard):
    """Split the shard represented by the shard_id into the destination group.
    """
    group_name = "sharding"
    command_name = "split"
    def execute(self, shard_id,  group_id,  split_value = None,
                synchronous=True):
        """Split the shard represented by the shard_id into the destination
        group.

        :param shard_id: The shard_id of the shard that needs to be split.
        :param group_id: The ID of the group into which the split data needs
                         to be moved.
        :param split_value: The value at which the range needs to be split.
        :param synchronous: Whether one should wait until the execution
                            finishes
        """
        #TODO: Add a configurable timeout option. The option will allow the
        #TODO: code to wait until timeout without taking a lock and after timeout
        #TODO: will take a read lock on the master.

        #TODO: Change SPLIT to an integer constant

        #TODO: Once we have the subsystem init patch implemented, we
        #TODO: should read these programs from the configuration file
        #TODO: when initializing the module, not each time a command is
        #TODO: dispatched.

        mysqldump_binary = _read_config_value(self.config, 'sharding',
                                            'mysqldump_program')
        mysqlclient_binary =  _read_config_value(self.config, 'sharding',
                                                'mysqlclient_program')

        procedures = _events.trigger(
            BACKUP_SOURCE_SHARD, self.get_lockable_objects(),
            shard_id, group_id, mysqldump_binary, mysqlclient_binary,
            split_value, "SPLIT")
        return self.wait_for_procedures(procedures, synchronous)

class DumpShardTables(Command):
    """Return information about all tables belonging to mappings
    matching any of the provided patterns. If no patterns are provided,
    dump information about all tables.
    """
    group_name = "store"
    command_name = "dump_shard_tables"

    def execute(self, version=None, patterns=""):
        """Return information about all tables belonging to mappings
        matching any of the provided patterns.

        :param version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        """
        return ShardMapping.dump_shard_tables(version, patterns)

class DumpShardingInformation(Command):
    """Return all the sharding information about the tables passed as patterns.
    If no patterns are provided, dump sharding information about all tables.
    """
    group_name = "store"
    command_name = "dump_sharding_information"

    def execute(self, version=None, patterns=""):
        """Return all the sharding information about the tables passed as
        patterns. If no patterns are provided, dump sharding information
        about all tables.

        :param version: The connectors version of the data.
        :param patterns: shard table pattern.
        """
        return ShardMapping.dump_sharding_info(version, patterns)

class DumpShardMappings(Command):
    """Return information about all shard mappings matching any of the
    provided patterns. If no patterns are provided, dump information about
    all shard mappings.
    """
    group_name = "store"
    command_name = "dump_shard_maps"

    def execute(self, version=None, patterns=""):
        """Return information about all shard mappings matching any of the
        provided patterns.

        :param version: The connectors version of the data.
        :param patterns: shard mapping pattern.
        """
        return ShardMapping.dump_shard_maps(version, patterns)

class DumpShardIndex(Command):
    """Return information about the index for all mappings matching
    any of the patterns provided. If no pattern is provided, dump the
    entire index. The lower_bound that is returned is a string that is
    a md-5 hash of the group-id in which the data is stored.
    """
    group_name = "store"
    command_name = "dump_shard_index"

    def execute(self, version=None, patterns=""):
        """Return information about the index for all mappings matching
        any of the patterns provided.

        :param version: The connectors version of the data.
        :param patterns: group pattern.
        """
        return Shards.dump_shard_indexes(version, patterns)

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
    if shard_mapping.type_name in Shards.VALID_SHARDING_TYPES:
        if not RangeShardingSpecification.list(shard_mapping.shard_mapping_id):
            shard_mapping.remove()
            _LOGGER.debug("Removed Shard Mapping (%s, %s, %s, %s, %s).",
                          shard_mapping.shard_mapping_id,
                          shard_mapping.table_name,
                          shard_mapping.column_name,
                          shard_mapping.type_name,
                          shard_mapping.global_group)
        else:
            raise _errors.ShardingError(CANNOT_REMOVE_SHARD_MAPPING)
    else:
        #This can happen only if there is a state store anomaly.
        raise _errors.ShardingError(INVALID_SHARDING_TYPE %
                                     (shard_mapping.type_name, ))

@_events.on_event(LOOKUP_SHARD_MAPPING)
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

@_events.on_event(LIST_SHARD_MAPPINGS)
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
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % (sharding_type,  ))

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

@_events.on_event(LIST_SHARD_MAPPING_DEFINITIONS)
def _list_definitions():
    """This method lists all the shard mapping definitions

    :return: A list of shard mapping definitions
                An Empty List if no shard mapping definition is found.
    """
    return ShardMapping.list_shard_mapping_defn()

@_events.on_event(ADD_SHARD)
def _add_shard(shard_mapping_id, lower_bound, group_id, state):
    """Add the RANGE shard specification. This represents a single instance
    of a shard specification that maps a key RANGE to a server.

    :param shard_mapping_id: The unique identification for a shard mapping.
    :param lower_bound: The lower bound of the given RANGE sharding defn
    :param group_id: The Group that contains the shard information.
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
    state = state.upper()
    if state not in Shards.VALID_SHARD_STATES:
        raise _errors.ShardingError(INVALID_SHARD_STATE % (state,  ))

    shard_mapping = ShardMapping.fetch_shard_mapping_defn(shard_mapping_id)
    if shard_mapping is None:
        raise _errors.ShardingError(SHARD_MAPPING_NOT_FOUND % \
                                                    (shard_mapping_id,  ))

    schema_type = shard_mapping[1]
    #TODO: Currently the RANGE sharding type supports only integer bounds.
    if schema_type == "RANGE":
        try:
            split_value = int(lower_bound)
        except ValueError:
            raise _errors.ShardingError(INVALID_LOWER_BOUND)

    shard = Shards.add(group_id, state)

    shard_id = shard.shard_id

    if schema_type == "RANGE":
        range_sharding_specification = RangeShardingSpecification.add(
                                            shard_mapping_id,
                                            lower_bound,
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
    range_sharding_specification, shard, _, _ = _verify_and_fetch_shard(shard_id)
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

@_events.on_event(LOOKUP_SHARD_SERVERS)
def _lookup(table_name, key,  hint):
    """Given a table name and a key return the servers of the Group where the
    shard of this table can be found

    :param table_name: The table whose sharding specification needs to be
                        looked up.
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
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping is None:
        raise _errors.ShardingError(TABLE_NAME_NOT_FOUND % (table_name,  ))
    if hint == "GLOBAL":
        group = Group.fetch(shard_mapping.global_group)
    elif shard_mapping.type_name == "RANGE":
        range_sharding_specification = RangeShardingSpecification.lookup \
                                        (key, shard_mapping.shard_mapping_id)
        if range_sharding_specification is None:
            raise _errors.ShardingError(INVALID_SHARDING_KEY % (key,  ))
        shard = Shards.fetch(str(range_sharding_specification.shard_id))
        if shard.state == "DISABLED":
            raise _errors.ShardingError(SHARD_NOT_ENABLED)

        #group cannot be None since there is a foreign key on the group_id.
        #An exception will be thrown nevertheless.
        group = Group.fetch(shard.group_id)
        if group is None:
            raise _errors.ShardingError(SHARD_LOCATION_NOT_FOUND)
    elif shard_mapping.type_name == "HASH":
        #TODO: Use a dictionary that maps the type to the prune method,
        #TODO: instead of duplicating code between RANGE and HASH.
        hash_sharding_specification = HashShardingSpecification.lookup \
                                        (key, shard_mapping.shard_mapping_id)
        if hash_sharding_specification is None:
            raise _errors.ShardingError(INVALID_SHARDING_KEY % (key,  ))
        shard = Shards.fetch(str(hash_sharding_specification.shard_id))
        if shard.state == "DISABLED":
            raise _errors.ShardingError(SHARD_NOT_ENABLED)

        #group cannot be None since there is a foreign key on the group_id.
        #An exception will be thrown nevertheless.
        group = Group.fetch(shard.group_id)
        if group is None:
            raise _errors.ShardingError(SHARD_LOCATION_NOT_FOUND)
    else:
        #A Shard Mapping cannot have a sharding type that is not
        #recognized. This will point to an anomaly in the state store.
        #If this case occurs we still need to degrade gracefully. Hence
        #we will throw an exception indicating the sharding type
        #was wrong.
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % ("",  ))

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
    range_sharding_spec, shard, _, _ = _verify_and_fetch_shard(shard_id)
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
    if shard_mapping.type_name == "RANGE":
        RangeShardingSpecification.delete_from_shard_db(table_name)
    elif shard_mapping.type_name == "HASH":
        HashShardingSpecification.delete_from_shard_db(table_name)

@_events.on_event(BACKUP_SOURCE_SHARD)
def _backup_source_shard(shard_id,  destn_group_id, mysqldump_binary,
                         mysqlclient_binary, split_value, cmd):
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
    """

    if cmd == "SPLIT":
        range_sharding_spec, shard,  shard_mapping,  shard_mapping_defn = \
            _verify_and_fetch_shard(shard_id)

        #If the underlying sharding scheme is a HASH
        if shard_mapping.type_name == "HASH":
            #Calculate the split. The split indicates the point at which the
            #given shard is separated into two.
            lower_bound = int(range_sharding_spec.lower_bound,  16)
            upper_bound = HashShardingSpecification.get_upper_bound(
                                range_sharding_spec.lower_bound,
                                range_sharding_spec.shard_mapping_id
                          )
            if  upper_bound is None:
                #While splitting a range, retrieve the next upper bound and
                #find the mid-point, in the case where the next upper_bound
                #is unavailable pick the maximum value in the set of values in
                #the shard.
                upper_bound = HashShardingSpecification.fetch_max_key(shard_id)
            #Retrieve an integer representation of the hexadecimal
            #lower_bound. The value is actually a long. Python automatically
            #returns a long value.
            upper_bound = int(upper_bound, 16)
            #split value after the below computation is actually a long.
            split_value = lower_bound + (upper_bound - lower_bound) / 2
            #split_value after the hex computation gets stored with a prefix
            #0x indicating a hexadecimal value and a suffix of L indicating a
            #Long. Extract the hexadecimal string from this value.
            split_value = "%x" % (split_value)
        #TODO:
        #Factor code like the following (based on the type name) into
        #subclasses that does the work for us.
        #e.g. shard_mapping.compute_split_value()
        elif shard_mapping.type_name == "RANGE" and split_value is None:
            #If the underlying sharding specification is a RANGE, and the
            #split value is not given, then calculate it as the mid value
            #between the current lower_bound and its next lower_bound.
            lower_bound = int(range_sharding_spec.lower_bound)
            upper_bound = int(RangeShardingSpecification.get_upper_bound)
            split_value = lower_bound + (upper_bound - lower_bound) / 2
        #TODO: Currently the RANGE sharding type supports only integer bounds.
        elif shard_mapping.type_name == "RANGE" and split_value is not None:
            try:
                split_value = int(split_value)
            except ValueError:
                raise _errors.ShardingError(INVALID_LOWER_BOUND)

    #Ensure that the group does not already contain a shard.
    if (Shards.lookup_shard_id(destn_group_id) is not None):
        raise _errors.ShardingError(SHARD_MOVE_DESTINATION_NOT_EMPTY)

    #Fetch the group information for the source shard that
    #needs to be moved.
    source_shard = Shards.fetch(shard_id)
    if source_shard is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id, ))

    #Fetch the group_id and the group that hosts the source shard.
    source_group_id = source_shard.group_id
    source_group = Group.fetch(source_group_id)
    if source_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND)

    #TODO: Alfranio: The code that choses a SPARE or a SLAVE needs to be
    #TODO: Alfranio: factored into the HA code.

    move_source_server = _fetch_backup_server(source_group)

    #TODO: The backup method should generic based on the backup tool
    #TODO: used to do the backup. Change this code to support generic
    #TODO: backups.

    #Do the backup of the group hosting the source shard.
    backup_image = _backup.MySQLDump.backup(
                        move_source_server,
                        mysqldump_binary
                    )

    #TODO: the backup image path should be handled in a more generic manner.
    #TODO: it is not right to just pass the path. This may work for MySQLDump
    #TODO: but we will need to do better to handle it for heterogenous backup
    #TODO: mechanisms.

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
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND)

    #Build a backup image that will be used for restoring
    bk_img = _backup.BackupImage(backup_image)

#TODO: convert to start one thread for each restore later.
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
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND)

    destination_group = Group.fetch(destn_group_id)
    if destination_group is None:
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND)

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
    #TODO: Make the timeout configurable.
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
                                split_value, cmd):
    """Setup the shard move or shard split workflow based on the command
    argument.

    :param shard_id: The ID of the shard that needs to be re-sharded.
    :param source_group_id: The ID of the source group.
    :param destination_group_id: The ID of the destination group.
    :param split_value: The value at which the shard needs to be split
                        (in the case of a shard split operation).
    :param cmd: whether the operation that needs to be split is a
                MOVE or a SPLIT operation.
    """
    if cmd == "MOVE":
        _setup_shard_switch_move(shard_id,  source_group_id,
                                 destination_group_id)
    elif cmd == "SPLIT":
        _setup_shard_switch_split(shard_id,  source_group_id,
                                  destination_group_id, split_value, cmd)

def _setup_shard_switch_split(shard_id,  source_group_id,  destination_group_id,
                                                       split_value, cmd):
    """Setup the moved shard to map to the new group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                                           be moved.
    :param split_value: Indicates the value at which the range for the
                                   particular shard will be split. Will be set only
                                   for shard split operations.
    :param cmd: Indicates the type of re-sharding operation.
    """
    #Fetch the Range sharding specification.
    range_sharding_spec, source_shard,  shard_mapping,  shard_mapping_defn = \
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

    if shard_mapping.type_name == "HASH":
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
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND)

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
    _events.trigger_within_procedure(
                                     PRUNE_SHARDS,
                                     new_shard_1.shard_id,
                                     new_shard_2.shard_id
                                     )

@_events.on_event(PRUNE_SHARDS)
def _prune_shard_tables_after_split(shard_id_1, shard_id_2):
    """Prune the two shards generated after a split.

    :param shard_id_1: The first shard id after the split.
    :param shard_id_2: The second shard id after the split.
    """
    #TODO: Start the threads that do the delete. For now the deletes are
    #TODO: done as part of the same thread. These will be started as
    #TODO: separate threads later.

    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    range_sharding_spec, source_shard,  shard_mapping,  _ = \
        _verify_and_fetch_shard(shard_id_1)

    #TODO: Use a dictionary that maps the type to the prune method.
    if shard_mapping.type_name == "RANGE":
        RangeShardingSpecification.prune_shard_id(shard_id_1)
        RangeShardingSpecification.prune_shard_id(shard_id_2)
    elif  shard_mapping.type_name == "HASH":
        HashShardingSpecification.prune_shard_id(shard_id_1)
        HashShardingSpecification.prune_shard_id(shard_id_2)

def _setup_shard_switch_move(shard_id,  source_group_id,  destination_group_id):
    """Setup the moved shard to map to the new group.

    :param shard_id: The shard ID of the shard that needs to be moved.
    :param source_group_id: The group_id of the source shard.
    :param destn_group_id: The ID of the group to which the shard needs to
                                           be moved.
    :param split_value: Indicates the value at which the range for the
                                   particular shard will be split. Will be set only
                                   for shard split operations.
    :param cmd: Indicates the type of re-sharding operation.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    range_sharding_spec, source_shard,  shard_mapping,  shard_mapping_defn = \
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
        raise _errors.ShardingError(SHARD_GROUP_NOT_FOUND)

    master = MySQLServer.fetch(source_group.master)
    if master is None:
        raise _errors.ShardingError(SHARD_GROUP_MASTER_NOT_FOUND)

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
                server.status == "RUNNING":
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

    :param shard_id: The ID for the shard whose specification needs to be fetched.

    :return: The sharding specification class representing the shard ID.

    :raises: ShardingError if the shard ID is not found.
    """
    #TODO: Change implementation to accept a flag that allows the method
    #TODO: to fetch only what is required.

    #Note:
    #Here the underlying sharding specification might be a RANGE
    #or a HASH. The type of sharding specification is obtained from the
    #shard mapping.
    range_sharding_spec = RangeShardingSpecification.fetch(shard_id)
    if range_sharding_spec is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id,  ))

    #Fetch the shard mapping and use it to find the type of sharding
    #scheme.
    shard_mapping = ShardMapping.fetch_by_id(
                        range_sharding_spec.shard_mapping_id
                    )
    if shard_mapping is None:
        raise _errors.ShardingError(
                    SHARD_MAPPING_NOT_FOUND % (
                        range_sharding_spec.shard_mapping_id,
                    )
                )

    #Fetch the shard mapping definition
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
    if shard_mapping.type_name == "HASH":
        return HashShardingSpecification.fetch(shard_id),\
            shard,  shard_mapping, shard_mapping_defn
    else:
        return range_sharding_spec, shard, shard_mapping, shard_mapping_defn

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
    range_sharding_spec, shard, shard_mapping, shard_mapping_defn  = \
        _verify_and_fetch_shard(shard_id)

    #Setup replication between the shard group and the global group.
    _group_replication.setup_group_replication \
            (shard_mapping_defn[2],  shard.group_id)

def _stop_shard_group_replication(shard_id,  clear_ref):
    """Stop the replication between the master group and the shard group.

    :param shard_id:  The ID of the shard, whose group needs to
                                be atopped as a slave.
    :param clear_ref: Indicates whether removing the shard should result
                                in the shard group losing all its slave group references.
    """
    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    range_sharding_spec, shard, shard_mapping, shard_mapping_defn  = \
        _verify_and_fetch_shard(shard_id)

    #Stop the replication between the shard group and the global group. Also
    #based on the clear_ref flag decide if you want to clear the references
    #associated with the group.
    _group_replication.stop_group_slave(shard_mapping_defn[2],  shard.group_id,
                                                                clear_ref)

#TODO: Should _read_config_value be moved to utils ?
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

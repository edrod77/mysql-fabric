"""This module provides the necessary interfaces for working with the shards
in FABRIC.
"""

import logging

from mysql.hub.command import (
    ProcedureCommand,
)

import mysql.hub.errors as _errors
import mysql.hub.events as _events
import mysql.hub.group_replication as _group_replication
import mysql.hub.replication as _replication
import mysql.hub.sharding as _sharding
import mysql.hub.backup as _backup


from mysql.hub.server import Group,  MySQLServer
from mysql.hub.sharding import ShardMapping, RangeShardingSpecification, Shards

_LOGGER = logging.getLogger(__name__)

#Error messages
INVALID_SHARDING_TYPE = "Invalid Sharding Type %s"
TABLE_NAME_NOT_FOUND = "Table name %s not found"
CANNOT_REMOVE_SHARD_MAPPING = "Cannot remove mapping, while, " \
                                                "shards still exist"
INVALID_SHARD_STATE = "Invalid Shard State %s"
INVALID_SHARDING_RANGE = "Invalid sharding range"
SHARD_MAPPING_NOT_FOUND = "Shard Mapping with shard_mapping_id %s not found"
SHARD_MAPPING_DEFN_NOT_FOUND = "Shard Mapping Definition with shard_mapping_id %s not found"
SHARD_NOT_DISABLED = "Shard not disabled"
SHARD_NOT_ENABLED = "Shard not enabled"
INVALID_SHARDING_KEY = "Invalid Key %s"
SHARD_NOT_FOUND = "Shard %s not found"
SHARD_LOCATION_NOT_FOUND = "Shard location not found"
INVALID_SHARDING_HINT = "Unknown lookup hint"
SHARD_GROUP_NOT_FOUND = "Shard group not found"
SHARD_GROUP_MASTER_NOT_FOUND = "Shard group master not found"
SHARD_MOVE_DESTINATION_NOT_EMPTY = "Shard move destination already hosts a shard"
INVALID_SHARD_SPLIT_VALUE = "The chosen split value must be between the " \
                            "lower bound and upper bound of the shard"
CONFIG_NOT_FOUND = "Configuration option not found %s . %s"

DEFINE_SHARD_MAPPING = _events.Event("DEFINE_SHARD_MAPPING")
class DefineShardMapping(ProcedureCommand):
    """Define a shard mapping.
    """
    group_name = "sharding"
    command_name = "define"
    def execute(self, type_name, global_group_id, synchronous=True):
        """Define a shard mapping.

        :param type_name: The type of sharding scheme - RANGE, HASH, LIST etc
        :param global_group: Every shard mapping is associated with a
                            Global Group that stores the global updates
                            and the schema changes for this shard mapping
                            and dissipates these to the shards.
        """
        procedures = _events.trigger(DEFINE_SHARD_MAPPING, type_name,
                                        global_group_id)
        return self.wait_for_procedures(procedures, synchronous)

ADD_SHARD_MAPPING = _events.Event("ADD_SHARD_MAPPING")
class AddShardMapping(ProcedureCommand):
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

        procedures = _events.trigger(ADD_SHARD_MAPPING, shard_mapping_id,
                                     table_name,
                                     column_name)
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD_MAPPING = _events.Event("REMOVE_SHARD_MAPPING")
class RemoveShardMapping(ProcedureCommand):
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
        procedures = _events.trigger(REMOVE_SHARD_MAPPING, table_name)
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
        procedures = _events.trigger(LOOKUP_SHARD_MAPPING, table_name)
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
        procedures = _events.trigger(LIST_SHARD_MAPPINGS, sharding_type)
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
        procedures = _events.trigger(LIST_SHARD_MAPPING_DEFINITIONS)
        return self.wait_for_procedures(procedures, synchronous)

ADD_SHARD = _events.Event("ADD_SHARD")
class AddShard(ProcedureCommand):
    """Add a shard.
    """
    group_name = "sharding"
    command_name = "add_shard"
    def execute(self, shard_mapping_id, lower_bound, upper_bound, group_id,
                      state="DISABLED", synchronous=True):
        """Add the RANGE shard specification. This represents a single instance
        of a shard specification that maps a key RANGE to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given RANGE sharding defn
        :param upper_bound: The upper bound of the given RANGE sharding defn
        :param group_id: The group that contains the shard information.
        :param state: Indicates whether a given shard is ENABLED or DISABLED
        :param synchronous: Whether one should wait until the execution finishes
                            or not.

        :return: A dictionary representing the current Range specification.
        """
        procedures = _events.trigger(ADD_SHARD, shard_mapping_id, lower_bound,
                                     upper_bound, group_id, state)
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SHARD = \
        _events.Event("REMOVE_SHARD")
class RemoveShard(ProcedureCommand):
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

        procedures = _events.trigger(REMOVE_SHARD, shard_id)
        return self.wait_for_procedures(procedures, synchronous)

SHARD_ENABLE = \
        _events.Event("SHARD_ENABLE")
class EnableShard(ProcedureCommand):
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
        procedures = _events.trigger(SHARD_ENABLE, shard_id)
        return self.wait_for_procedures(procedures, synchronous)

SHARD_DISABLE = \
        _events.Event("SHARD_DISABLE")
class DisableShard(ProcedureCommand):
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

        procedures = _events.trigger(SHARD_DISABLE, shard_id)
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
        procedures = _events.trigger(LOOKUP_SHARD_SERVERS, table_name, key,
                                                        hint)
        return self.wait_for_procedures(procedures, synchronous)

PRUNE_SHARD_TABLES = _events.Event("PRUNE_SHARD_TABLES")
class PruneShardTables(ProcedureCommand):
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
        procedures = _events.trigger(PRUNE_SHARD_TABLES, table_name)
        return self.wait_for_procedures(procedures, synchronous)

BACKUP_SOURCE_SHARD =  _events.Event("BACKUP_SOURCE_SHARD")
RESTORE_SHARD_BACKUP = _events.Event("RESTORE_SHARD_BACKUP")
SETUP_MOVE_SYNC = _events.Event("SETUP_MOVE_SYNC")
SETUP_RESHARDING_SWITCH = _events.Event("SETUP_RESHARDING_SWITCH")
PRUNE_SHARDS = _events.Event("PRUNE_SHARDS")
class MoveShardServer(ProcedureCommand):
    """Move the shard represented by the shard_id to the destination group.
    """
    group_name = "sharding"
    command_name = "move"
    def execute(self,  shard_id,  destn_group_id,  synchronous=True):
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
        mysqlclient_binary =  _read_config_value(self.config, 'sharding',
                                                'mysqlclient_program')

        procedures = _events.trigger(BACKUP_SOURCE_SHARD,
                                           shard_id,
                                           destn_group_id, 
                                           mysqldump_binary,
                                           mysqlclient_binary,
                                           None, 
                                           "MOVE")
        return self.wait_for_procedures(procedures, synchronous)

class SplitShardServer(ProcedureCommand):
    """Move the shard represented by the shard_id to the destination group.
    """
    group_name = "sharding"
    command_name = "split"
    def execute(self,  shard_id,  destn_group_id,  split_value,
                          synchronous=True):
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

        procedures = _events.trigger(BACKUP_SOURCE_SHARD,
                                           shard_id,
                                           destn_group_id,
                                           mysqldump_binary,
                                           mysqlclient_binary,
                                           split_value, 
                                           "SPLIT")
        return self.wait_for_procedures(procedures, synchronous)

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
    if shard_mapping.type_name == "RANGE":
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
         raise _errors.ShardingError(INVALID_SHARDING_TYPE % (shard_mapping.type_name, ))

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
        #We return an empty shard mapping because if an Error is thrown it would
        #cause the executor to rollback which is an unnecessary action. It is enough
        #if we inform the user that the lookup returned nothing.
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
def _add_shard(shard_mapping_id, lower_bound, upper_bound, group_id, state):
    """Add the RANGE shard specification. This represents a single instance
    of a shard specification that maps a key RANGE to a server.

    :param shard_mapping_id: The unique identification for a shard mapping.
    :param lower_bound: The lower bound of the given RANGE sharding defn
    :param upper_bound: The upper bound of the given RANGE sharding defn
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

    #More checking for the ranges needed, but for now just check that
    #the upper_bound is greater than the lower_bound
    if int(lower_bound) >= int(upper_bound):
        raise _errors.ShardingError(INVALID_SHARDING_RANGE)

    shard_mapping = ShardMapping.fetch_shard_mapping_defn(shard_mapping_id)
    if shard_mapping is None:
        raise _errors.ShardingError(SHARD_MAPPING_NOT_FOUND % \
                                                    (shard_mapping_id,  ))

    shard = Shards.add(group_id)

    shard_id = shard.shard_id

    schema_type = shard_mapping[1]
    if schema_type == "RANGE":
        range_sharding_specification = RangeShardingSpecification.add(
                                            shard_mapping_id, lower_bound,
                                            upper_bound, shard_id, state)
        _LOGGER.debug("Added Shard (%s, %s, %s, %s, %s).",
                            shard_mapping_id, lower_bound,
                            upper_bound, shard_id, state)
    else:
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % (schema_type,  ))

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
#TODO: As we start supporting heterogenous sharding schemes, we need
#TODO: support for mapping a shard_id to a particular sharding type.
#TODO: Only if we know that a shard_id is RANGE can we actually
#TODO: query the RangeShardingTables. This information WILL NOT be
#TODO: supplied by the user. For now proceed assuming it is RANGE.
    range_sharding_specification, shard = _verify_and_fetch_shard(shard_id)
    if range_sharding_specification.state == "ENABLED":
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
        if range_sharding_specification.state == "DISABLED":
            raise _errors.ShardingError(SHARD_NOT_ENABLED)
         #shard cannot be None since there is a foreign key mapping on the
         #shard_id. But an exception will be thrown nevertheless. This
         #could point to a problem in the state store.
        shard = Shards.fetch(str(range_sharding_specification.shard_id))
        if shard is None:
            raise _errors.ShardingError(SHARD_NOT_FOUND % ("",  ))
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
#TODO: As we start supporting heterogenous sharding schemes, we need
#TODO: support for mapping a shard_id to a particular sharding type.
#TODO: Only if we know that a shard_id is RANGE can we actually
#TODO: update the RangeShardingTables. This information WILL NOT be
#TODO: supplied by the user. For now proceed assuming it is RANGE.
    """Enable the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True Placeholder return value
    :raises: ShardingError if the shard_id is not found.
    """
    range_sharding_spec, shard = _verify_and_fetch_shard(shard_id)
    #When you enable a shard, setup replication with the global server
    #of the shard mapping associated with this shard.
    _setup_shard_group_replication(shard_id)
    range_sharding_spec.enable()

@_events.on_event(SHARD_DISABLE)
def _disable_shard(shard_id):
#TODO: As we start supporting heterogenous sharding schemes, we need
#TODO: support for mapping a shard_id to a particular sharding type.
#TODO: Only if we know that a shard_id is RANGE can we actually
#TODO: update the RangeShardingTables. This information WILL NOT be
#TODO: supplied by the user. For now proceed assuming it is RANGE.
    """Disable the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param shard_id: The shard ID of the shard that needs to be removed.

    :return: True Placeholder return value
    :raises: ShardingError if the shard_id is not found.
    """
    range_sharding_spec, shard = _verify_and_fetch_shard(shard_id)
    #When you disable a shard, disable replication with the global server
    #of the shard mapping associated with the shard.
    _stop_shard_group_replication(shard_id,  False)
    range_sharding_spec.disable()

#TODO: Removed Go Fish Lookup. Provide Dump facility.

@_events.on_event(PRUNE_SHARD_TABLES)
def _prune_shard_tables(table_name):
    """Delete the data from the copied data directories based on the
    sharding configuration uploaded in the sharding tables of the state
    store. The basic logic consists of

    a) Querying the sharding scheme name corresponding to the sharding table
    b) Querying the sharding key range using the sharding scheme name.
    c) Deleting the sharding keys that fall outside the range for a given
        server.

    :param job: The Job object created for executing this event.
    """
    RangeShardingSpecification.delete_from_shard_db(table_name)

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
    #If it is a split ensure that the range is correct.

    #We will need to change this once we start supporting heterogenous
    #sharding schemes. It cannot checks RANGES alone.
    if cmd == "SPLIT":
        range_sharding_spec,  shard = _verify_and_fetch_shard(shard_id)

    #TODO: Enable comparison / check in database. This is not the
    #TODO: right way to check for a correct split value. What if the
    #TODO: range is a string ?
#    if split_value < range_sharding_spec.lower_bound or \
#       split_value > range_sharding_spec.upper_bound:
#           raise _errors.ShardingError(INVALID_SHARD_SPLIT_VALUE)

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

    #Get a slave server whose status is spare.
    move_source_server = None
    for server in source_group.servers():
        if source_group.master != server.uuid and \
            server.status == "SPARE":
            move_source_server = server

    #If there is no spare check if a running slave is available
    if move_source_server is None:
        for server in source_group.servers():
            if source_group.master != server.uuid and \
                server.status == "RUNNING":
                move_source_server = server

    #If there is no running slave just use the master
    if move_source_server is None:
        move_source_server = MySQLServer.fetch(source_group.master)

    #If there is no master throw an exception
    if move_source_server is None:
        raise _errors.ShardingError(SHARD_GROUP_MASTER_NOT_FOUND)

    #TODO: The backup method should generic based on the backup tool
    #TODO: used to do the backup. Change this code to support generic
    #TODO: backups.

    #Do the backup of the group hosting the source shard.
    backup_image = _backup.MySQLDump.backup(move_source_server, mysqldump_binary)

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
        _backup.MySQLDump.restore(destn_group_server, bk_img, mysqlclient_binary)

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
    if cmd == "MOVE":
        _setup_shard_switch_move(shard_id,  source_group_id,  destination_group_id)
    elif cmd == "SPLIT":
         _setup_shard_switch_split(shard_id,  source_group_id,  destination_group_id,
                                                       split_value, cmd)

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
    #Setup replication for the new group from the global server

    #Fetch the Range sharding specification. When we start implementing
    #heterogenous sharding schemes, we need to find out the type of
    #sharding scheme and we should use that to find out the sharding
    #implementation.
    range_sharding_spec, source_shard = _verify_and_fetch_shard(shard_id)

    #TODO: Should it delete the old shard and add two new shards or should
    #TODO: it retain the original shard as one of the splits?

    #TODO: Currently it just updates the range for the old shard and creates
    #TODO: a new split.

    #TODO: Should I disable, update the range and enable or can I directly enable?
    #TODO: Disable, disables lookups and stops the global replication, but is this
    #TODO: required?

    #Add the new shard
    new_shard = Shards.add(destination_group_id)

    #Add the new split range (split_value, upper_bound)
    new_range_sharding_spec = \
        RangeShardingSpecification.add(range_sharding_spec.shard_mapping_id, 
                                                             split_value, 
                                                             range_sharding_spec.upper_bound,
                                                             new_shard.shard_id, 
                                                             "ENABLED")
    #Disable the old shard id
    range_sharding_spec.disable()

    #Update the range for the old shard.
    RangeShardingSpecification.update_shard(shard_id,
                                                range_sharding_spec.lower_bound, 
                                                split_value)

    #Enable the old shard id
    range_sharding_spec.enable()

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

    #Trigger changing the mappings for the shard that was copied
    _events.trigger_within_procedure(
                                     PRUNE_SHARDS,
                                     new_shard.shard_id,
                                     shard_id
                                     )

@_events.on_event(PRUNE_SHARDS)
def _prune_shard_tables_after_split(shard_id_1, shard_id_2):
    """Prune the two shards generated after a split.

    :param shard_id_1: The first shard id after the split.
    :param shard_id_2: The second shard id after the split.
    """
    #TODO:
    #Start the threads that do the delete. For now the deletes are done as
    #part of the same thread. These will be started as separate threads later.
    RangeShardingSpecification.prune_shard_id(shard_id_1)
    RangeShardingSpecification.prune_shard_id(shard_id_2)
    
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
    range_sharding_spec, source_shard = _verify_and_fetch_shard(shard_id)
    #Fetch the shard mapping definition for the given range specification.
    #The shard mapping contains the information about the global group.
    shard_mapping_defn = ShardMapping.fetch_shard_mapping_defn \
                                           (range_sharding_spec.shard_mapping_id)
    if shard_mapping_defn is None:
        raise _errors.ShardingError(SHARD_MAPPING_DEFN_NOT_FOUND % \
                            (range_sharding_spec.shard_mapping_id, ))
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

def _verify_and_fetch_shard(shard_id):
#TODO: As we start supporting heterogenous sharding schemes, we need
#TODO: support for mapping a shard_id to a particular sharding type.
#TODO: Only if we know that a shard_id is RANGE can we actually
#TODO: access the RangeShardingTables. This information WILL NOT be
#TODO: supplied by the user. For now proceed assuming it is RANGE.
    """Find out if the shard_id exists and return the sharding specification for
    it. If it does not exist throw an exception.

    :param shard_id: The ID for the shard whose specification needs to be fetched.

    :return: The sharding specification class representing the shard ID.

    :raises: ShardingError if the shard ID is not found.
    """
    range_sharding_spec = RangeShardingSpecification.fetch(shard_id)
    if range_sharding_spec is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id,  ))
    shard = Shards.fetch(shard_id)
    if shard is None:
        raise _errors.ShardingError(SHARD_NOT_FOUND % (shard_id,  ))
    return range_sharding_spec, shard

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
    range_sharding_spec, shard = _verify_and_fetch_shard(shard_id)
    #Fetch the shard mapping definition for the given range specification.
    #The shard mapping contains the information about the global group.
    shard_mapping_defn = ShardMapping.fetch_shard_mapping_defn \
                                           (range_sharding_spec.shard_mapping_id)
    if shard_mapping_defn is None:
        raise _errors.ShardingError(SHARD_MAPPING_DEFN_NOT_FOUND % \
                            (range_sharding_spec.shard_mapping_id, ))
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
    range_sharding_spec, shard = _verify_and_fetch_shard(shard_id)
    #Fetch the shard mapping for the given range specification. From the
    #shard mapping the details of the global group can be obtained.
    shard_mapping_defn = ShardMapping.fetch_shard_mapping_defn \
                                               (range_sharding_spec.shard_mapping_id)
    if shard_mapping_defn is None:
        raise _errors.ShardingError(SHARD_MAPPING_DEFN_NOT_FOUND % \
                            (range_sharding_spec.shard_mapping_id, ))
    #Stop the replication between the shard group and the global group. Also
    #based on the clear_ref flag decide if you want to clear the references associated
    #with the group.
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

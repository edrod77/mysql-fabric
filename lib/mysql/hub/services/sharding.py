"""This module provides the necessary interfaces for working with the shards
in FABRIC.
"""

import logging

from mysql.hub.command import (
    ProcedureCommand,
)

import mysql.hub.errors as _errors
import mysql.hub.events as _events
import mysql.hub.sharding as _sharding

from mysql.hub.server import Group
from mysql.hub.sharding import ShardMapping, RangeShardingSpecification, Shards

_LOGGER = logging.getLogger("mysql.hub.services.sharding")

#Error messages
INVALID_SHARDING_TYPE = "Invalid Sharding Type %s"
TABLE_NAME_NOT_FOUND = "Table name %s not found"
CANNOT_REMOVE_SHARD_MAPPING = "Cannot remove mapping, while, shards still exist"
INVALID_SHARD_STATE = "Invalid Shard State %s"
INVALID_SHARDING_RANGE = "Invalid sharding range"
SHARD_MAPPING_NOT_FOUND = "Shard Mapping with shard_mapping_id %s not found"
SHARD_NOT_DISABLED = "Shard not disabled"
SHARD_NOT_ENABLED = "Shard not enabled"
INVALID_SHARDING_KEY = "Invalid Key %s"
SHARD_NOT_FOUND = "Shard %s not found"
SHARD_LOCATION_NOT_FOUND = "Shard location not found"

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
    def execute(self, table_name, key, synchronous=True):
        """Given a table name and a key return the server where the shard of
        this table can be found.

        :param table_name: The table whose sharding specification needs to be
                            looked up.
        :param key: The key value that needs to be looked up
        :param synchronous: Whether one should wait until the execution finishes
                        or not.

        :return: The Group UUID that contains the range in which the key belongs.
        """

        procedures = _events.trigger(LOOKUP_SHARD_SERVERS, table_name, key)
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

#TODO: Should the shard mapping be removed by shard_mapping_id or
#TODO: table_name? It makes more sense to remove it by the table_name
#TODO: since shard_mapping_id:table_name is a 1:N mapping.
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
        return True
    else:
        raise _errors.ShardingError(INVALID_SHARDING_TYPE % (schema_type,  ))

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
    range_sharding_specification = _verify_and_fetch_shard(shard_id)

    if range_sharding_specification.state == "ENABLED":
        raise _errors.ShardingError(SHARD_NOT_DISABLED)

    ret = range_sharding_specification.remove()

    _LOGGER.debug("Removed Shard (%d).", shard_id)

    return ret

@_events.on_event(LOOKUP_SHARD_SERVERS)
def _lookup(table_name, key):
    """Given a table name and a key return the servers of the Group where the
    shard of this table can be found

    :param table_name: The table whose sharding specification needs to be
                        looked up.
    :param key: The key value that needs to be looked up

    :return: The servers of the Group that contains the range in which the
            key belongs.
    """
    group = None
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping is None:
        raise _errors.ShardingError(TABLE_NAME_NOT_FOUND % (table_name,  ))
    if shard_mapping.type_name == "RANGE":
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
    range_sharding_spec = _verify_and_fetch_shard(shard_id)
    return range_sharding_spec.enable()

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
    range_sharding_spec = _verify_and_fetch_shard(shard_id)
    return range_sharding_spec.disable()

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

    :return: False If the delete fails
            True if the delete succeeds.
    """
    return RangeShardingSpecification.delete_from_shard_db(table_name)

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
    return range_sharding_spec

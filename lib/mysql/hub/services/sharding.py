"""This module provides the necessary interfaces for working with the shards
in FABRIC.
"""

import logging

import mysql.hub.events as _events
import mysql.hub.executor as _executor
import mysql.hub.sharding as _sharding

from mysql.hub.sharding import ShardMapping, RangeShardingSpecification

_LOGGER = logging.getLogger("mysql.hub.services.sharding")

ADD_SHARD_MAPPING = _events.Event("ADD_SHARD_MAPPING")
def add_shard_mapping(table_name, column_name, type_name,
                      sharding_specification, synchronous=True):
    """Add the shard specification mapping information for the given table.
    This method is exposed through the XML-RPC framework and creates a job
    and enqueues it in the executor.

    :param table_name: The name of the table being sharded.
    :param column_name: The column whose value is used in the sharding
                        scheme being applied
    :param type_name: The type of sharding being used, RANGE, HASH etc.
    :param sharding_specification: The name of the sharding specification
                                    the given table is being mapped to.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    """

    jobs = _events.trigger(ADD_SHARD_MAPPING, table_name, column_name,
                           type_name,
                           sharding_specification)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

REMOVE_SHARD_MAPPING = _events.Event("REMOVE_SHARD_MAPPING")
def remove_shard_mapping(table_name, synchronous=True):
    """Remove the shard mapping represented by the Shard Mapping object.
    This method is exposed through the XML-RPC framework and creates a job
    and enqueues it in the executor.

    :param table_name: The name of the table whose sharding specification is
                        being removed.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    """
    jobs = _events.trigger(REMOVE_SHARD_MAPPING, table_name)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

LOOKUP_SHARD_MAPPING = _events.Event("LOOKUP_SHARD_MAPPING")
def lookup_shard_mapping(table_name, synchronous=True):
    """Fetch the shard specification mapping for the given table

    :param table_name: The name of the table for which the sharding
                        specification is being queried.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.

    :returns The a dictionary that contains the shard mapping information for
                the given table.
    """
    jobs = _events.trigger(LOOKUP_SHARD_MAPPING, table_name)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

LIST_SHARD_MAPPINGS = _events.Event("LIST_SHARD_MAPPINGS")
def list(sharding_type, synchronous=True):
    """The method returns all the shard mappings (names) of a
    particular sharding_type. For example if the method is called
    with 'range' it returns all the sharding specifications that exist
    of type range.

    :param sharding_type: The sharding type for which the sharding
                          specification needs to be returned.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.

    :returns: A list of dictionaries of sharding specifications that are of the
              sharding type.
    """
    jobs = _events.trigger(LIST_SHARD_MAPPINGS, sharding_type)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

ADD_SHARD = _events.Event("ADD_SHARD")
def add_shard(schema_type, name, lower_bound, upper_bound, group_id,
                  synchronous=True):
    """Add the RANGE shard specification. This represents a single instance
    of a shard specification that maps a key RANGE to a server.

    :param schema_type: The type of the sharding scheme.
    :param name: The name of the sharding scheme to which
                                this definition belongs.
    :param lower_bound: The lower bound of the range sharding definition.
    :param upper_bound: The upper bound of the range sharding definition
    :param group_id: The unique identifier of the Group where the current
                    KEY range belongs.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.

    :return A dictionary representing the current Range specification.
    """

    jobs = _events.trigger(ADD_SHARD, schema_type, name, lower_bound,
                           upper_bound, group_id)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

REMOVE_SHARD = \
        _events.Event("REMOVE_SHARD")
def remove_shard(schema_type, name, key, synchronous=True):
    """Remove the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param schema_type: The type of the sharding scheme.
    :param name: The name of the sharding scheme to which
                            this definition belongs.
    :param key: The key value whose range needs to be removed.
    :param synchronous: Whether one should wait until the execution finishes
                    or not.
    """

    jobs = _events.trigger(REMOVE_SHARD, schema_type, name, key)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

SHARD_LOOKUP = \
        _events.Event("SHARD_LOOKUP")
def lookup(table_name, key, synchronous=True):
    """Given a table name and a key return the server where the shard of this
    table can be found

    :param table_name: The table whose sharding specification needs to be
                        looked up.
    :param key: The key value that needs to be looked up
    :param synchronous: Whether one should wait until the execution finishes
                    or not.

    :return The Group UUID that contains the range in which the key belongs.
    """

    jobs = _events.trigger(SHARD_LOOKUP, table_name, key)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

GO_FISH_LOOKUP = _events.Event("GO_FISH_LOOKUP")
def go_fish_lookup(table_name, synchronous=True):
    """Given table name return all the servers that contain the shards for
    this table.

    :param table_name: The table whose shards need to be found
    :param synchronous: Whether one should wait until the execution finishes
                    or not.

    :return The set of Group UUIDs that contain the shards of the table.
    """

    jobs = _events.trigger(GO_FISH_LOOKUP, table_name)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

PRUNE_SHARD_TABLES = _events.Event("PRUNE_SHARD_TABLES")
def prune_shard_tables(table_name, synchronous=True):
    """Given the table name prune the tables according to the defined
    sharding specification for the table.

    :param table_name: The table that needs to be sharded.
    :param synchronous: Whether one should wait until the execution finishes
                    or not.
    """
    jobs = _events.trigger(PRUNE_SHARD_TABLES, table_name)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

@_events.on_event(ADD_SHARD_MAPPING)
def _add_shard_mapping(job):
    """Add the shard specification mapping information for the given table.

    :param job: The Job object created for executing this event.

    :return True if the add succeeded
            False if the add failed
    """

    table_name, column_name, type_name, sharding_specification = job.args
    shard_mapping = ShardMapping.add(table_name, column_name,
                                     type_name, sharding_specification)
    if shard_mapping is not None:
        return True
    else:
        return False

@_events.on_event(REMOVE_SHARD_MAPPING)
def _remove_shard_mapping(job):
    """Remove the shard specification mapping information for the given table.

    :param job: The Job object created for executing this event.

    :return True if the remove succeeded
            False if the query failed
    """

    table_name = job.args[0]
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping is not None:
        return shard_mapping.remove()
    else:
        return False


@_events.on_event(LOOKUP_SHARD_MAPPING)
def _lookup_shard_mapping(job):
    """Fetch the shard specification mapping for the given table

    :param job: The Job object created for executing this event.

    :returns The a dictionary that contains the shard mapping information for
                the given table.
    """

    table_name = job.args[0]
    shard_mapping = ShardMapping.fetch(table_name)

    if shard_mapping is not None:
        return {"table_name":shard_mapping.table_name,
                "column_name":shard_mapping.column_name,
                "type_name":shard_mapping.type_name,
                "sharding_specification":shard_mapping.sharding_specification}
    else:
        return {"table_name":"",
                "column_name":"",
                "type_name":"",
                "sharding_specification":""}

@_events.on_event(LIST_SHARD_MAPPINGS)
def _list(job):
    """The method returns all the shard mappings (names) of a
    particular sharding_type. For example if the method is called
    with 'range' it returns all the sharding specifications that exist
    of type range.

    :param job: The Job object created for executing this event.

    :returns: A list of dictionaries of sharding specifications that are of the
              sharding type.
    """

    ret_shard_mappings = []
    sharding_type = job.args[0]
    shard_mappings = ShardMapping.list(sharding_type)
    for shard_mapping in shard_mappings:
        ret_shard_mappings.append({"table_name":shard_mapping.table_name,
                                   "column_name":shard_mapping.column_name,
                                   "type_name":shard_mapping.type_name,
                                   "sharding_specification":
                                    shard_mapping.sharding_specification})
    return ret_shard_mappings

@_events.on_event(ADD_SHARD)
def _add_shard(job):
    """Add the RANGE shard specification. This represents a single instance
    of a shard specification that maps a key RANGE to a server.

    :param job: The Job object created for executing this event.

    :return A dictionary representing the current Range specification.
    """

    schema_type = job.args[0]
    if schema_type == "RANGE":
        schema_type, name, lower_bound, upper_bound, group_id = job.args
        range_sharding_specification = RangeShardingSpecification.add(
            name, lower_bound, upper_bound, group_id)
        if range_sharding_specification is not None:
            return True
        else:
            return False

@_events.on_event(REMOVE_SHARD)
def _remove_shard(job):
    """Remove the RANGE specification mapping represented by the current
    RANGE shard specification object.

    :param job: The Job object created for executing this event.

    :return True if the remove succeeded
            False if the query failed
    """

    schema_type = job.args[0]
    if schema_type == "RANGE":
        schema_type, name, key = job.args
        range_sharding_specification = \
            RangeShardingSpecification.lookup(key, name)
        if range_sharding_specification is not None:
            return range_sharding_specification.remove()
        else:
            return False

@_events.on_event(SHARD_LOOKUP)
def _lookup(job):
    """Given a table name and a key return the server where the shard of this
    table can be found

    :param job: The Job object created for executing this event.

    :return The Group UUID that contains the range in which the key belongs.
    """

    table_name, key = job.args
    group_id = _sharding.lookup(table_name, key)
    if group_id is not None:
        return group_id
    else:
        return ""

@_events.on_event(GO_FISH_LOOKUP)
def _go_fish_lookup(job):
    """Given table name return all the servers that contain the shards for
    this table.

    :param job: The Job object created for executing this event.

    :return The set of Group UUIDs that contain the shards of the table.
    """

    table_name = job.args[0]
    return _sharding.go_fish_lookup(table_name)

@_events.on_event(PRUNE_SHARD_TABLES)
def _prune_shard_tables(job):
    """Delete the data from the copied data directories based on the
    sharding configuration uploaded in the sharding tables of the state
    store. The basic logic consists of

    a) Querying the sharding scheme name corresponding to the sharding table
    b) Querying the sharding key range using the sharding scheme name.
    c) Deleting the sharding keys that fall outside the range for a given
        server.

    :param job: The Job object created for executing this event.

    :return False If the delete fails
            True if the delete succeeds.
    """

    table_name = job.args[0]
    return RangeShardingSpecification.delete_from_shard_db(table_name)

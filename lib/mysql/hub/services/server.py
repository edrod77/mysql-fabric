"""This module provides the necessary interfaces for performing administrative
tasks on groups and servers, specifically MySQL Servers.

It should be possible to add, update and remove a group. One cannot, however,
remove a group if there are associated servers. It should also be possible to
add a server to a group and remove a server from a group.

Search functions are also provided so that one may look up groups and servers.
Given a server's uri, one may also find out the server's uuid if the server is
alive and kicking.

Functions are scheduled to be asynchronously executed and return a schedule's
description, i.e. a job's description. If users are not interestered in the
result produced by a function, they must set the synchronous parameter to false.
It is set to true by default which means that the call is blocked until the
execution finishes.

The scheduling is made through the executor which enqueues functions, namely
jobs, and then serializes their execution within a Fabric Server. Any job object
encapsulates a function to be executed, its parameters, its execution's status
and its result. Due to its asynchronous nature, a job accesses a snapshot
produced by previously executed functions which are atomically processed so that
Fabric is never left in an inconsistent state after a failure.

Functions always return results in the following format::

  str(job.uuid), job.status, job.result

It is worth noticing that this module only provides functions for performing
basic administrative tasks, provisioning and high-availability functions are
provided elsewhere.
"""
import logging
import uuid as _uuid

import mysql.hub.events as _events
import mysql.hub.server as _server
import mysql.hub.errors as _errors
import mysql.hub.executor as _executor
import mysql.hub.failure_detector as _detector

# TODO: Fix this.
# Due to services/__init__.py:
# There is something wrong sometimes __name__ is server.
# See line lib/mysql/hub/services/__init__.py", line 25.
# Is this expected?
_LOGGER = logging.getLogger("mysql.hub.services.server")

def lookup_fabrics():
    """Return a list with all the available Fabric Servers.

    :return: List with existing Fabric Servers.
    :rtype: ["host:port", ...]
    """
    import mysql.hub.services as _services
    service = _services.ServiceManager()
    return [service.address]

LOOKUP_GROUPS = _events.Event()
def lookup_groups(synchronous=True):
    """Return a list with existing groups.

    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: A list with existing groups.
    :rtype: [[group], ....].
    """
    jobs = _events.trigger(LOOKUP_GROUPS)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

LOOKUP_GROUP = _events.Event()
def lookup_group(group_id, synchronous=True):
    """Look up a group identified by an id.

    :param group_id: Group's id.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: Return group's information.
    :rtype: {"group_id" : group_id, "description": description}.

    If the group does not exist, the :class:`mysql.hub.errors.GroupError`
    exception is thrown. Otherwise, the group's information is returned.
    """
    jobs = _events.trigger(LOOKUP_GROUP, group_id)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

CREATE_GROUP = _events.Event()
def create_group(group_id, description, synchronous=True):
    """Create a group.

    :param group_id: Group's id.
    :param description: Group's description.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: Tuple with job's uuid and status.
    """
    jobs = _events.trigger(CREATE_GROUP, group_id, description)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

UPDATE_GROUP = _events.Event()
def update_group(group_id, description, synchronous=True):
    """Update a group.

    :param group_id: Group's id.
    :param description: Group's description.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: Tuple with job's uuid and status.
    """
    jobs = _events.trigger(UPDATE_GROUP, group_id, description)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

REMOVE_GROUP = _events.Event()
def remove_group(group_id, force=False, synchronous=True):
    """Remove a group.

    :param group_id: Group's id.
    :param force: If the group is not empty, remove it serves.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: Tuple with job's uuid and status.
    """
    jobs = _events.trigger(REMOVE_GROUP, group_id, force)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

LOOKUP_SERVERS = _events.Event()
def lookup_servers(group_id, synchronous=True):
    """Return list of existing servers in a group.

    :param group_id: Group's id.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: List of existing servers.

    If the group does not exist, the :class:`mysqly.hub.errors.GroupError`
    exception is thrown. The list of servers returned has the following
    format::

      [uuid, ...]
    """
    jobs = _events.trigger(LOOKUP_SERVERS, group_id)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

LOOKUP_UUID = _events.Event()
def lookup_uuid(uri, user, passwd, synchronous=True):
    """Retrieve server's uuid.

    :param uri: Server's uri.
    :param user: Server's user.
    :param passwd: Server's passwd.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: uuid.
    """
    jobs = _events.trigger(LOOKUP_UUID, uri, user, passwd)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

LOOKUP_SERVER = _events.Event()
def lookup_server(group_id, uuid, synchronous=True):
    """Retrieve information on a server.

    :param group_id: Group's id.
    :param uuid: Server's uuid.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: List of existing servers.
    :rtype: {"uuid" : uuid, "uri": uri, "user": user, "passwd": passwd}.
    """
    jobs = _events.trigger(LOOKUP_SERVER, group_id, uuid)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

CREATE_SERVER = _events.Event()
def create_server(group_id, uri, user, passwd, synchronous=True):
    """Create a server and add it into a group.

    :param group_id: Group's id.
    :param uri: Server's uri.
    :param user: Server's user.
    :param passwd: Server's passwd.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: Tuple with job's uuid and status.
    """
    jobs = _events.trigger(CREATE_SERVER, group_id, uri, user, passwd)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

REMOVE_SERVER = _events.Event()
def remove_server(group_id, uuid, synchronous=True):
    """Remove a server from a group.

    :param uuid: Server's uuid.
    :param group_id: Group's id.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: Tuple with job's uuid and status.
    """
    jobs = _events.trigger(REMOVE_SERVER, group_id, uuid)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

@_events.on_event(LOOKUP_GROUPS)
def _lookup_groups(job):
    """Return a list of existing groups.
    """
    _LOGGER.debug("Looking up groups in job %s.", job)
    ret = _server.Group.groups()
    return ret

@_events.on_event(LOOKUP_GROUP)
def _lookup_group(job):
    """Look up a group identified by an id.
    """
    group_id = job.args[0]
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    return {
        "group_id" : group.group_id, "description": group.description}

@_events.on_event(CREATE_GROUP)
def _create_group(job):
    """Create group.
    """
    group_id, description = job.args
    group = _server.Group.add(group_id, description)
    _LOGGER.debug("Added group (%s).", str(group))
    _detector.FailureDetector.register_group(group_id)

@_events.on_event(UPDATE_GROUP)
def _update_group(job):
    """Update a group."""
    group_id, description = job.args
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    group.description = description
    _LOGGER.debug("Updated group (%s).", str(group))

@_events.on_event(REMOVE_GROUP)
def _remove_group(job):
    """Remove a group."""
    group_id, force = job.args
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    servers = group.servers()
    if servers and force:
        for row in servers:
            _do_remove_server(group, _uuid.UUID(row[0]))
    elif servers:
        raise _errors.GroupError("Group (%s) is not empty." % (group_id, ))
    group.remove()
    _LOGGER.debug("Removed group (%s).", str(group))
    _detector.FailureDetector.unregister_group(group_id)

@_events.on_event(LOOKUP_SERVERS)
def _lookup_servers(job):
    """Return list of existing servers in a group.
    """
    group_id = job.args[0]
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    ret = []
    for row in group.servers():
        server = _server.MySQLServer.fetch(_uuid.UUID(row[0]))
        ret.append([str(server.uuid), server.uri,
                   group.master == server.uuid])
    return ret

@_events.on_event(LOOKUP_UUID)
def _lookup_uuid(job):
    """Retrieve server's uuid.
    """
    uri, user, passwd = job.args
    return _server.MySQLServer.discover_uuid(uri=uri, user=user,
                                             passwd=passwd)
@_events.on_event(LOOKUP_SERVER)
def _lookup_server(job):
    """Retrieve information on a server.
    """
    group_id, uuid = job.args
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    if not group.contains_server(uuid):
        raise _errors.GroupError("Group (%s) does not contain server (%s)." \
                                 % (group_id, uuid))
    server = _server.MySQLServer.fetch(uuid)
    return {"uuid": str(server.uuid), "uri": server.uri,
           "user": server.user, "passwd": server.passwd}

@_events.on_event(CREATE_SERVER)
def _create_server(job):
    """Create a server and add it to a group.
    """
    group_id, uri, user, passwd = job.args
    uuid = _server.MySQLServer.discover_uuid(uri=uri, user=user,
                                             passwd=passwd)
    uuid = _uuid.UUID(uuid)
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    if group.contains_server(uuid):
        raise _errors.ServerError("Server (%s) already exists in group (%s)." \
                                  % (str(uuid), group_id))
    server = _server.MySQLServer.add(uuid, uri, user, passwd)
    group.add_server(server)
    _LOGGER.debug("Added server (%s) to group (%s).", str(server), str(group))

@_events.on_event(REMOVE_SERVER)
def _remove_server(job):
    """Remove a server from a group but check some requirements first."""
    group_id, uuid = job.args
    uuid = _uuid.UUID(uuid)
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    if not group.contains_server(uuid):
        raise _errors.GroupError("Group (%s) does not contain server (%s)." \
                                 % (group_id, uuid))
    if group.master == uuid:
        raise _errors.ServerError("Cannot remove server (%s), which is master "
                                  "in group (%s). Please, demote it first."
                                  % (uuid, group_id))
    _do_remove_server(group, uuid)

def _do_remove_server(group, uuid):
    """Remove a server from a group."""
    server = _server.MySQLServer.fetch(uuid)
    group.remove_server(server)
    server.remove()
    _LOGGER.debug("Removed server (%s) from group (%s).", str(server),
                  str(group))
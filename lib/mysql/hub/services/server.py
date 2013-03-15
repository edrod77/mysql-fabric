"""This module provides the necessary interfaces for performing administrative
tasks on groups and servers, specifically MySQL Servers.

It is possible to add, update and remove a group. One cannot, however, remove
a group if there are associated servers. It is possible to add a server to a
group and remove a server from a group. Search functions are also provided so
that one may look up groups and servers. Given a server's address, one may
also find out the server's uuid if the server is alive and kicking.

When a group is created though, it is inactive which means that the failure
detector will not check if its servers are alive. To start up the failure
detector, one needs to call :meth:`ativate_group`. A server may have one of
the following status:

- RUNNING - This is the regular status of a server and means that a server
  is alive and kicking.
- OFFLINE - It should be used before shutting down a server otherwise the
  failure detector will trigger notifications. If users, however, want to
  bring a master offline they must firstly run a switchover.
- SPARE - This is used to make a slave not automatically eligible to become
  a master in a switchover or failover operation. Nor are users redirected
  to such a server to execute read-only transactions.
- FAULTY - This is set by the failover routine and indicates that a server
  is not reachable.

Find in what follows the possible state transitions:

.. graphviz::

   digraph state_transition {
    rankdir=LR;
    size="8,5"

    node [shape = circle]; Master;
    node [shape = circle]; Slave;
    node [shape = circle]; Spare;
    node [shape = circle]; Faulty;
    node [shape = circle]; Offline;

    Master   -> Slave [ label = "demote, switchover" ];
    Slave    -> Master [ label = "promote, switchover, failover" ];
    Slave    -> Spare [ label = "set_status spare" ];
    Spare    -> Master [ label = "promote, swichover, failover" ];
    Master   -> Faulty [ label = "failover" ];
    Slave    -> Faulty [ label = "failover" ];
    Slave    -> Offline [ label = "set_status offline" ];
    Faulty   -> Slave [ label = "set_status running" ];
    Offline  -> Slave [ label = "set_status running" ];
    Spare    -> Slave [ label = "set_status running" ];
  }

It is worth noticing that this module only provides functions for performing
basic administrative tasks, provisioning and high-availability functions are
provided elsewhere.
"""
import logging
import uuid as _uuid

import mysql.hub.events as _events
import mysql.hub.server as _server
import mysql.hub.errors as _errors
import mysql.hub.failure_detector as _detector

from mysql.hub.command import (
    ProcedureCommand,
    )

_LOGGER = logging.getLogger("mysql.hub.services.server")

LOOKUP_GROUPS = _events.Event()
class GroupLookups(ProcedureCommand):
    """Return a list with existing groups.
    """
    group_name = "group"
    command_name = "lookup_groups"

    def execute(self, synchronous=True):
        """Return a list of existing groups.

        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: A list with existing groups.
        :rtype: [[group], ....].
        """
        procedures = _events.trigger(LOOKUP_GROUPS)
        return self.wait_for_procedures(procedures, synchronous)

LOOKUP_GROUP = _events.Event()
class GroupLookup(ProcedureCommand):
    """Return information on a group.
    """
    group_name = "group"
    command_name = "lookup_group"

    def execute(self, group_id, synchronous=True):
        """Look up a group identified by an id.

        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Return group's information.
        :rtype: {"group_id" : group_id, "description": description}.

        If the group does not exist, the :class:`mysql.hub.errors.GroupError`
        exception is thrown. Otherwise, the group's information is returned.
        """
        procedures = _events.trigger(LOOKUP_GROUP, group_id)
        return self.wait_for_procedures(procedures, synchronous)

CREATE_GROUP = _events.Event()
class GroupCreate(ProcedureCommand):
    """Create a group.
    """
    group_name = "group"
    command_name = "create"

    def execute(self, group_id, description=None, synchronous=True):
        """Create a group.

        :param group_id: Group's id.
        :param description: Group's description.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(CREATE_GROUP, group_id, description)
        return self.wait_for_procedures(procedures, synchronous)

UPDATE_GROUP = _events.Event()
class GroupDescription(ProcedureCommand):
    """Update group's description.
    """
    group_name = "group"
    command_name = "description"

    def execute(self, group_id, description=None, synchronous=True):
        """Update group's description.

        :param group_id: Group's id.
        :param description: Group's description.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(UPDATE_GROUP, group_id, description)
        return self.wait_for_procedures(procedures, synchronous)

DESTROY_GROUP = _events.Event()
class DestroyGroup(ProcedureCommand):
    """Remove a group.
    """
    group_name = "group"
    command_name = "destroy"

    def execute(self, group_id, force=False, synchronous=True):
        """Remove a group.

        :param group_id: Group's id.
        :param force: If the group is not empty, remove it serves.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(DESTROY_GROUP, group_id, force)
        return self.wait_for_procedures(procedures, synchronous)

LOOKUP_SERVERS = _events.Event()
class ServerLookups(ProcedureCommand):
    """Return a list of existing servers in a group.
    """
    group_name = "group"
    command_name = "lookup_servers"

    def execute(self, group_id, status=None, synchronous=True):
        """Return a list of existing servers in a group.

        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: List of existing servers.

        If the group does not exist, the :class:`mysqly.hub.errors.GroupError`
        exception is thrown. The list of servers returned has the following
        format::

          [[uuid, address, is_master, status], ...]
        """
        procedures = _events.trigger(LOOKUP_SERVERS, group_id, status)
        return self.wait_for_procedures(procedures, synchronous)

LOOKUP_UUID = _events.Event()
class ServerUuid(ProcedureCommand):
    """Return server's uuid.
    """
    group_name = "server"
    command_name = "lookup_uuid"

    def execute(self, address, user, passwd, synchronous=True):
        """Return server's uuid.

        :param address: Server's address.
        :param user: Server's user.
        :param passwd: Server's passwd.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: uuid.
        """
        procedures = _events.trigger(LOOKUP_UUID, address, user, passwd)
        return self.wait_for_procedures(procedures, synchronous)

LOOKUP_SERVER = _events.Event()
class ServerLookup(ProcedureCommand):
    """Return information on a server.
    """
    group_name = "group"
    command_name = "lookup_server"

    def execute(self, group_id, uuid, synchronous=True):
        """Return information on a server.

        :param group_id: Group's id.
        :param uuid: Server's uuid.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: List of existing servers.
        :rtype: {"uuid" : uuid, "address": address, "user": user, "passwd": passwd}.
        """
        procedures = _events.trigger(LOOKUP_SERVER, group_id, uuid)
        return self.wait_for_procedures(procedures, synchronous)

ADD_SERVER = _events.Event()
class ServerAdd(ProcedureCommand):
    """Add a server into group.
    """
    group_name = "group"
    command_name = "add"

    def execute(self, group_id, address, user, passwd, synchronous=True):
        """Add a server into a group.

        :param group_id: Group's id.
        :param address: Server's address.
        :param user: Server's user.
        :param passwd: Server's passwd.
        :param synchronous: Whether one should wait until the execution finishes
                            or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(
            ADD_SERVER, group_id, address, user, passwd
            )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SERVER = _events.Event()
class ServerRemove(ProcedureCommand):
    """Remove a server from a group.
    """
    group_name = "group"
    command_name = "remove"

    def execute(self, group_id, uuid, synchronous=True):
        """Remove a server from a group.

        :param uuid: Server's uuid.
        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(REMOVE_SERVER, group_id, uuid)
        return self.wait_for_procedures(procedures, synchronous)

ACTIVATE_GROUP = _events.Event()
class ActivateGroup(ProcedureCommand):
    """Activate a group.

    This means that it will be monitored and faulty servers will be detected.
    """
    group_name = "group"
    command_name = "activate"

    def execute(self, group_id, synchronous=True):
        """Activate a group.

        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(ACTIVATE_GROUP, group_id)
        return self.wait_for_procedures(procedures, synchronous)

DEACTIVATE_GROUP = _events.Event()
class DeactivateGroup(ProcedureCommand):
    """Deactivate a group.

    This means that it will not be monitored and faulty servers will not be
    detected. By default groups are inactive right after being created.
    """
    group_name = "group"
    command_name = "deactivate"

    def execute(self, group_id, synchronous=True):
        """Deactivate group.

        :param group_id: Group's id.
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(DEACTIVATE_GROUP, group_id)
        return self.wait_for_procedures(procedures, synchronous)

SET_SERVER_STATUS = _events.Event()
class SetServerStatus(ProcedureCommand):
    """Set a server's status.

    Any server added into a group has the RUNNING status which means that
    it is aliving and kicking. Otherwise, it could not be inserted into
    the group.

    A server may have its status automatically changed to FAULTY, if the
    failure detector is not able to reach it.

    Users can also manually change the server's status. Usually, a user
    may change a slave's status to SPARE to avoid write and read access
    and guarantee that it is not choosen when a failover or swithover
    routine is executed.

    If a slave needs to be taken offline, its status must be changed to
    OFFLINE before switching it off thus avoiding that the failure
    detector complains about not reaching the server.
    """
    group_name = "server"
    command_name = "set_status"

    def execute(self, uuid, status, synchronous=True):
        """Set a server's status.
        """
        procedures = _events.trigger(SET_SERVER_STATUS, uuid, status)
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(LOOKUP_GROUPS)
def _lookup_groups():
    """Return a list of existing groups.
    """
    return _server.Group.groups()

@_events.on_event(LOOKUP_GROUP)
def _lookup_group(group_id):
    """Return information on a group.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    return {"group_id" : group.group_id, "description": group.description}

@_events.on_event(CREATE_GROUP)
def _create_group(group_id, description):
    """Create a group.
    """
    group = _server.Group(group_id=group_id, description=description,
                          status=_server.Group.INACTIVE)
    _server.Group.add(group)
    _LOGGER.debug("Added group (%s).", str(group))

@_events.on_event(ACTIVATE_GROUP)
def _activate_group(group_id):
    """Activate a group.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    group.status = _server.Group.ACTIVE
    _detector.FailureDetector.register_group(group_id)
    _LOGGER.debug("Group (%s) is active.", str(group))

@_events.on_event(DEACTIVATE_GROUP)
def _deactivate_group(group_id):
    """Deactivate a group.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    group.status = _server.Group.INACTIVE
    _detector.FailureDetector.unregister_group(group_id)
    _LOGGER.debug("Group (%s) is active.", str(group))

@_events.on_event(UPDATE_GROUP)
def _update_group_description(group_id, description):
    """Update a group description.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    group.description = description
    _LOGGER.debug("Updated group (%s).", str(group))

@_events.on_event(DESTROY_GROUP)
def _destroy_group(group_id, force):
    """Destroy a group.
    """
    group = _server.Group.fetch(group_id)
    servers_uuid = []
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    servers = group.servers()
    if servers and force:
        for server in servers:
            servers_uuid.append(server.uuid)
            _do_remove_server(group, server)
    elif servers:
        raise _errors.GroupError("Group (%s) is not empty." % (group_id, ))
    _server.Group.remove(group)
    cnx_pool = _server.ConnectionPool()
    for uuid in servers_uuid:
        cnx_pool.purge_connections(uuid)
    _LOGGER.debug("Removed group (%s).", str(group))
    _detector.FailureDetector.unregister_group(group_id)

@_events.on_event(LOOKUP_SERVERS)
def _lookup_servers(group_id, status):
    """Return list of existing servers in a group.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    if status is not None and status not in _server.MySQLServer.SERVER_STATUS:
        raise ServerError(
            "Unknown server status (%s). Possible status are (%s)." %
            (status,  _server.MySQLServer.SERVER_STATUS)
            )
    elif status is None:
        status = _server.MySQLServer.SERVER_STATUS
    else:
        status = [status]

    ret = []
    for server in group.servers():
        if server.status in status:
            ret.append(
                [str(server.uuid), server.address,
                (group.master == server.uuid), server.status]
                )
    return ret

@_events.on_event(LOOKUP_UUID)
def _lookup_uuid(address, user, passwd):
    """Return server's uuid.
    """
    return _server.MySQLServer.discover_uuid(address=address, user=user,
                                             passwd=passwd)

@_events.on_event(LOOKUP_SERVER)
def _lookup_server(group_id, uuid):
    """Return information on a server.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    if not group.contains_server(uuid):
        raise _errors.GroupError("Group (%s) does not contain server (%s)." \
                                 % (group_id, uuid))
    server = _server.MySQLServer.fetch(uuid)
    return {"uuid": str(server.uuid), "address": server.address,
            "user": server.user, "passwd": server.passwd}

@_events.on_event(ADD_SERVER)
def _add_server(group_id, address, user, passwd):
    """Add a server into a group.
    """
    uuid = _server.MySQLServer.discover_uuid(address=address, user=user,
                                             passwd=passwd)
    uuid = _uuid.UUID(uuid)
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    if group.contains_server(uuid):
        raise _errors.ServerError("Server (%s) already exists in group (%s)." \
                                  % (str(uuid), group_id))

    server = _server.MySQLServer(uuid=uuid, address=address, user=user,
                                 passwd=passwd)
    _server.MySQLServer.add(server)
    server.connect()

    if not server.check_version_compat((5,6,8)):
        raise _errors.ServerError(
            "Server (%s) has an outdated version (%s). 5.6.8 or greater "
            "is required." % (uuid, server.version)
            )
    if not server.has_root_privileges():
        _LOGGER.warning(
            "User (%s) needs root privileges on Server (%s, %s)."
            % (user, address, uuid)
            )
        server.disconnect()

    group.add_server(server)
    _LOGGER.debug("Added server (%s) to group (%s).", str(server), str(group))

@_events.on_event(REMOVE_SERVER)
def _remove_server(group_id, uuid):
    """Remove a server from a group.
    """
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
    server = _server.MySQLServer.fetch(uuid)

    _do_remove_server(group, server)
    _server.ConnectionPool().purge_connections(uuid)

@_events.on_event(SET_SERVER_STATUS)
def _set_server_status(uuid, status):
    """Set a server's status.
    """
    uuid = _uuid.UUID(uuid)

    server = _server.MySQLServer.fetch(uuid)
    if not server:
        raise _errors.ServerError(
            "Server (%s) does not exist." % (uuid, )
            )

    status = str(status).upper()

    if status == _server.MySQLServer.SPARE:
        _set_server_spare(server)
    elif status == _server.MySQLServer.RUNNING:
        _set_server_running(server)
    elif status == _server.MySQLServer.OFFLINE:
        _set_server_offline(server)
    else:
        raise _errors.ServerError("Trying to set invalid status (%s) "
            "for server (%s)." % (server.status, uuid)
            )

def _set_server_spare(server):
    """Put the server in spare mode.
    """
    forbidden_status = [_server.MySQLServer.FAULTY,
                        _server.MySQLServer.OFFLINE]

    if server.status in forbidden_status:
        raise _errors.ServerError(
            "Cannot put server (%s) whose status is (%s) in "
            "spare mode." % (server.uuid, server.status)
            )

    group = _server.Group.group_from_server(server.uuid)
    if group.master == server.uuid:
        raise _errors.ServerError(
            "Server (%s) is master in group (%s) and cannot be put in "
            "spare mode." % (server.uuid, group.group_id)
            )

    server.status = _server.MySQLServer.SPARE

def _set_server_running(server):
    """Put the server in running mode.
    """
    server.connect()

    if server.is_alive():
        server.status = _server.MySQLServer.RUNNING
    else:
        raise _errors.ServerError(
            "Cannot connect to server (%s)." % (server.uuid, )
            )

def _set_server_offline(server):
    """Put the server in offline mode.
    """
    group = _server.Group.group_from_server(server.uuid)
    if group.master == server.uuid:
        raise _errors.ServerError(
            "Server (%s) is master in group (%s) and cannot be put in "
            "off-line mode." % (str(uuid), group.group_id)
            )
    server.status = _server.MySQLServer.OFFLINE
    _server.ConnectionPool().purge_connections(server.uuid)

def _do_remove_server(group, server):
    """Remove a server from a group.
    """
    group.remove_server(server)
    _server.MySQLServer.remove(server)
    _LOGGER.debug("Removed server (%s) from group (%s).", str(server),
                  str(group))

#
# Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
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

"""This module provides the necessary interfaces for performing administrative
tasks on groups and servers, specifically MySQL Servers.

It is possible to add, update and remove a group. One cannot, however, remove
a group if there are associated servers. It is possible to add a server to a
group and remove a server from a group. Search functions are also provided so
that one may look up groups and servers. Given a server's address, one may
also find out the server's uuid if the server is alive and kicking.

When a group is created though, it is inactive which means that the failure
detector will not check if its servers are alive. To start up the failure
detector, one needs to explicitly activate it per group. A server may have
one of the following statuses:

- PRIMARY - This is set when the server may accept both reads and writes
  operations.
- SECONDARY - This is set when the server may accept only read operations.
- SPARE - This is set when users want to have server that is kept in sync
  but does not accept neither reads or writes operations.
- FAULTY - This is set by the failure detector and indicates that a server
  is not reachable.

Find in what follows the possible state transitions:

.. graphviz::

   digraph state_transition {
    rankdir=LR;
    size="8,5"

    node [shape = circle]; Primary;
    node [shape = circle]; Secondary;
    node [shape = circle]; Spare;
    node [shape = circle]; Faulty;

    Primary   -> Secondary [ label = "demote" ];
    Primary   -> Faulty [ label = "failure" ];
    Secondary -> Primary [ label = "promote" ];
    Secondary -> Spare [ label = "set_status" ];
    Secondary -> Faulty [ label = "failure" ];
    Spare     -> Primary [ label = "promote" ];
    Spare     -> Secondary [ label = "set_status" ];
    Spare     -> Faulty [ label = "failure" ];
    Faulty    -> Spare [ label = "set_status" ];
  }

It is worth noticing that this module only provides functions for performing
basic administrative tasks, provisioning and high-availability functions are
provided elsewhere.
"""
import logging
import uuid as _uuid

import mysql.fabric.services.utils as _utils

from mysql.fabric import (
    events as _events,
    server as _server,
    errors as _errors,
    failure_detector as _detector,
    group_replication as _group_replication,
)

from mysql.fabric.command import (
    ProcedureGroup,
    Command,
)

_LOGGER = logging.getLogger(__name__)

class GroupLookups(Command):
    """Return information on existing group(s).
    """
    group_name = "group"
    command_name = "lookup_groups"

    def execute(self, group_id=None):
        """Return information on existing group(s).

        :param group_id: None if one wants to list the existing groups or
                         group's id if one wants information on a group.
        :return: List with existing groups or detailed information on group.
        :rtype: [[group], ....] or {group_id : ..., description : ...}.
        """
        return Command.generate_output_pattern(
            _lookup_groups, group_id)

CREATE_GROUP = _events.Event()
class GroupCreate(ProcedureGroup):
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
        procedures = _events.trigger(
            CREATE_GROUP, self.get_lockable_objects(), group_id, description
        )
        return self.wait_for_procedures(procedures, synchronous)

UPDATE_GROUP = _events.Event()
class GroupDescription(ProcedureGroup):
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
        procedures = _events.trigger(
            UPDATE_GROUP, self.get_lockable_objects(), group_id, description
        )
        return self.wait_for_procedures(procedures, synchronous)

DESTROY_GROUP = _events.Event()
class DestroyGroup(ProcedureGroup):
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
        procedures = _events.trigger(
            DESTROY_GROUP, self.get_lockable_objects(), group_id, force
        )
        return self.wait_for_procedures(procedures, synchronous)

class ServerLookups(Command):
    """Return information on existing server(s) in a group.
    """
    group_name = "group"
    command_name = "lookup_servers"

    def execute(self, group_id, uuid=None, status=None, mode=None):
        """Return information on existing server(s) in a group.

        :param group_id: Group's id.
        :param uuid: None if one wants to list the existing servers
                     in a group or server's id if one wants information
                     on a server in a group.
        :param status: Server's mode one is searching for.
        :return: List with existing severs in a group or detailed information
                 on a server in a group.
        :rtype: [server_uuid, ....] or  {"uuid" : uuid, "address": address,
                "user": user, "passwd": passwd}

        If the group does not exist, the
        :class:`~mysqly.fabric.errors.GroupError` exception is thrown. The
        information returned has the following format::

          [[uuid, address, is_master, status], ...]
        """
        return Command.generate_output_pattern(_lookup_servers,
                                               group_id, uuid, status, mode)

class ServerUuid(Command):
    """Return server's uuid.
    """
    group_name = "server"
    command_name = "lookup_uuid"

    def execute(self, address, user, passwd):
        """Return server's uuid.

        :param address: Server's address.
        :param user: Server's user.
        :param passwd: Server's passwd.

        :return: uuid.
        """
        return Command.generate_output_pattern(_lookup_uuid,
                                               address, user, passwd)

ADD_SERVER = _events.Event()
class ServerAdd(ProcedureGroup):
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
        :param synchronous: Whether one should wait until the execution
                            finishes or not.
        :return: Tuple with job's uuid and status.
        """
        procedures = _events.trigger(ADD_SERVER, self.get_lockable_objects(),
            group_id, address, user, passwd
        )
        return self.wait_for_procedures(procedures, synchronous)

REMOVE_SERVER = _events.Event()
class ServerRemove(ProcedureGroup):
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
        procedures = _events.trigger(REMOVE_SERVER, self.get_lockable_objects(),
            group_id, uuid
        )
        return self.wait_for_procedures(procedures, synchronous)

ACTIVATE_GROUP = _events.Event()
class ActivateGroup(ProcedureGroup):
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
        procedures = _events.trigger(
            ACTIVATE_GROUP, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

DEACTIVATE_GROUP = _events.Event()
class DeactivateGroup(ProcedureGroup):
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
        procedures = _events.trigger(
            DEACTIVATE_GROUP, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

class DumpServers(Command):
    """Return information about all servers. The servers might belong to any
    group that matches any of the provided patterns, or all servers if no
    patterns are provided.
    """
    group_name = "dump"
    command_name = "servers"

    def execute(self, version=None, patterns=""):
        """Return information about all servers.

        :param version: The connectors version of the data.
        :param patterns: group pattern.
        """
        return _server.MySQLServer.dump_servers(version, patterns)

SET_SERVER_STATUS = _events.Event()
class SetServerStatus(ProcedureGroup):
    """Set a server's status.

    Any server added into a group has to be alive and kicking and is added
    as Secondary. A server will have its status automatically changed to
    FAULTY, if the failure detector is not able to reach it.

    Users can also manually change the server's status. Usually, a user
    may change a slave's mode to SPARE to avoid write and read access
    and guarantee that it is not choosen when a failover or swithover
    routine is executed.
    """
    group_name = "server"
    command_name = "set_status"

    def execute(self, uuid, status, synchronous=True):
        """Set a server's status.
        """
        procedures = _events.trigger(
            SET_SERVER_STATUS, self.get_lockable_objects(), uuid, status
        )
        return self.wait_for_procedures(procedures, synchronous)

SET_SERVER_WEIGHT = _events.Event()
class SetServerWeight(ProcedureGroup):
    """Set a server's weight which determines the likelihood of a server
    being choseen by a connector to process transactions or by the high
    availability service to replace a failed master.

    From the connector's perspective, a server whose weight is 2.0 will
    receive 2 times more more requests than a server whose weight is 1.0.
    """
    group_name = "server"
    command_name = "set_weight"

    def execute(self, uuid, weight, synchronous=True):
        """Set a server's weight.
        """
        procedures = _events.trigger(
            SET_SERVER_WEIGHT, self.get_lockable_objects(), uuid, weight
        )
        return self.wait_for_procedures(procedures, synchronous)

SET_SERVER_MODE = _events.Event()
class SetServerMode(ProcedureGroup):
    """Set a server's mode which determines whether it can process
    read-only, read-write or both transaction types.
    """
    group_name = "server"
    command_name = "set_mode"

    def execute(self, uuid, mode, synchronous=True):
        """Set a server's mode.
        """
        procedures = _events.trigger(
            SET_SERVER_MODE, self.get_lockable_objects(), uuid, mode
        )
        return self.wait_for_procedures(procedures, synchronous)

def _lookup_groups(group_id=None):
    """Return a list of existing groups or fetch information on a group
    identified by group_id.
    """
    if group_id is None:
        return _server.Group.groups()

    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    return {"group_id" : group.group_id,
            "description": group.description if group.description else ""}

@_events.on_event(CREATE_GROUP)
def _create_group(group_id, description):
    """Create a group.
    """
    group = _server.Group.fetch(group_id)
    if group:
        raise _errors.GroupError("Group (%s) already exists." % (group_id, ))

    group = _server.Group(group_id=group_id, description=description,
                          status=_server.Group.INACTIVE)
    _server.Group.add(group)
    _LOGGER.debug("Added group (%s).", group)

@_events.on_event(ACTIVATE_GROUP)
def _activate_group(group_id):
    """Activate a group.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    group.status = _server.Group.ACTIVE

    _detector.FailureDetector.register_group(group_id)
    _LOGGER.debug("Group (%s) is active.", group)

@_events.on_event(DEACTIVATE_GROUP)
def _deactivate_group(group_id):
    """Deactivate a group.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))
    group.status = _server.Group.INACTIVE
    _detector.FailureDetector.unregister_group(group_id)
    #Since the Group is being deactivated, stop all the slaves
    #associated with this group. Although the slave groups are
    #being stopped do not remove the references to the slave
    #groups. When the group is activated again the slaves need
    #to be restarted again.
    _group_replication.stop_group_slaves(group_id, True)
    _LOGGER.debug("Group (%s) is active.", str(group))

@_events.on_event(UPDATE_GROUP)
def _update_group_description(group_id, description):
    """Update a group description.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id))
    group.description = description
    _LOGGER.debug("Updated group (%s).", group)

@_events.on_event(DESTROY_GROUP)
def _destroy_group(group_id, force):
    """Destroy a group.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    #Since the group is being destroyed stop all the slaves associated
    #with this group would have been removed. If the group had been
    #a slave to another group, this would also have been stopped by the
    #demote or the deactivate command. But we need to clear the ref
    #to the other groups that is part of this group object.
    #Remove the master group ID.
    group.remove_master_group_id()
    #Remove the slave group IDs.
    group.remove_slave_group_ids()

    servers_uuid = []
    servers = group.servers()
    if servers and force:
        for server in servers:
            servers_uuid.append(server.uuid)
            _server.MySQLServer.remove(server)
    elif servers:
        raise _errors.GroupError("Group (%s) is not empty." % (group_id, ))

    cnx_pool = _server.ConnectionPool()
    for uuid in servers_uuid:
        cnx_pool.purge_connections(uuid)

    _detector.FailureDetector.unregister_group(group_id)
    _server.Group.remove(group)
    _LOGGER.debug("Removed group (%s).", group)

def _lookup_servers(group_id, uuid=None, status=None, mode=None):
    """Return existing servers in a group or information on a server.
    """
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    status = _retrieve_server_status(status) if status is not None else None
    if status is None:
        status = _server.MySQLServer.SERVER_STATUS
    else:
        status = [status]

    mode = _retrieve_server_mode(mode) if mode is not None else None
    if mode is None:
        mode = _server.MySQLServer.SERVER_MODE
    else:
        mode = [mode]

    if uuid is None:
        ret = []
        for server in group.servers():
            if server.status in status and server.mode in mode:
                ret.append(
                    [str(server.uuid), server.address,
                    (group.master == server.uuid), server.status]
                    )
        return ret

    server = _server.MySQLServer.fetch(uuid)

    if group_id != server.group_id:
        raise _errors.GroupError("Group (%s) does not contain server (%s)."
                                 % (group_id, uuid))

    return {"uuid": str(server.uuid), "address": server.address,
            "user": server.user, "passwd": server.passwd}

def _lookup_uuid(address, user, passwd):
    """Return server's uuid.
    """
    return _server.MySQLServer.discover_uuid(address=address, user=user,
                                             passwd=passwd)

@_events.on_event(ADD_SERVER)
def _add_server(group_id, address, user, passwd):
    """Add a server into a group.
    """
    uuid = _server.MySQLServer.discover_uuid(address=address, user=user,
                                             passwd=passwd)
    uuid = _uuid.UUID(uuid)
    group = _server.Group.fetch(group_id)

    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    server = _server.MySQLServer.fetch(uuid)
    if server:
        raise _errors.ServerError("Server (%s) already exists." % (uuid, ))

    server = _server.MySQLServer(uuid=uuid, address=address, user=user,
                                 passwd=passwd)
    _server.MySQLServer.add(server)
    server.connect()

    # Check if the server fulfils the necessary requirements to become
    # a member.
    _check_requirements(server)

    # Add server as a member in the group.
    server.group_id = group_id

    # Configure the server as a slave if there is a master.
    _configure_as_slave(group, server)

    _LOGGER.debug("Added server (%s) to group (%s).", server, group)

@_events.on_event(REMOVE_SERVER)
def _remove_server(group_id, uuid):
    """Remove a server from a group.
    """
    try:
        uuid = _uuid.UUID(uuid)
    except ValueError:
        raise _errors.ServerError("Malformed UUID (%s)." % (uuid, ))

    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    if group.master == uuid:
        raise _errors.ServerError("Cannot remove server (%s), which is master "
                                  "in group (%s). Please, demote it first."
                                  % (uuid, group_id))

    server = _server.MySQLServer.fetch(uuid)

    if server and group_id != server.group_id:
        raise _errors.GroupError("Group (%s) does not contain server (%s)."
                                 % (group_id, uuid))
    elif not server:
        raise _errors.ServerError("Server (%s) does not exist." % (uuid, ))

    _server.MySQLServer.remove(server)
    _server.ConnectionPool().purge_connections(uuid)

@_events.on_event(SET_SERVER_STATUS)
def _set_server_status(uuid, status):
    """Set a server's status.
    """
    status = _retrieve_server_status(status)
    server = _retrieve_server(uuid)

    if status == _server.MySQLServer.PRIMARY:
        _set_server_status_primary(server)
    elif status == _server.MySQLServer.SECONDARY:
        _set_server_status_secondary(server)
    elif status == _server.MySQLServer.SPARE:
        _set_server_status_spare(server)
    elif status == _server.MySQLServer.FAULTY:
        _set_server_status_faulty(server)

def _retrieve_server_status(status):
    """Check whether the server's status is valid or not and
    if an integer was provided retrieve the correspondent
    string.
    """
    valid = False
    try:
        idx = int(status)
        try:
            status = _server.MySQLServer.get_status(idx)
            valid = True
        except IndexError:
            pass
    except ValueError:
        try:
            status = str(status).upper()
            _server.MySQLServer.get_status_idx(status)
            valid = True
        except ValueError:
            pass

    if not valid:
        raise _errors.ServerError("Trying to set an invalid status (%s) "
            "for server (%s)." % (status, uuid)
            )

    return status

def _set_server_status_primary(server):
    """Set server's status to primary.
    """
    raise _errors.ServerError(
        "If you want to put a server (%s) to primary, please, use the "
        "promote interface." % (server.uuid, )
    )

def _set_server_status_faulty(server):
    """Set server's status to faulty.
    """
    group = _server.Group.fetch(server.group_id)
    if group.status == _server.Group.ACTIVE:
        raise _errors.ServerError(
            "Group (%s) has the failure detector activate so that "
            "one cannot manually set a server (%s) as faulty."
            % (group.group_id, server.uuid)
        )

    if server.status ==  _server.MySQLServer.FAULTY:
        raise _errors.ServerError(
            "Server (%s) was already set to faulty." % (server.uuid, )
        )

    server.status = _server.MySQLServer.FAULTY
    _server.ConnectionPool().purge_connections(server.uuid)

    if group.master == server.uuid:
        _LOGGER.info("Master (%s) in group (%s) has "
                     "been lost.", server.uuid, group.group_id)
        _events.trigger_within_procedure("FAIL_OVER", group.group_id)

    _events.trigger_within_procedure(
        "SERVER_LOST", group.group_id, server.uuid
    )

def _set_server_status_secondary(server):
    """Set server's status to secondary.
    """
    allowed_status = (_server.MySQLServer.SPARE)
    status = _server.MySQLServer.SECONDARY
    mode = _server.MySQLServer.READ_ONLY
    return _do_set_status(server, allowed_status, status, mode)

def _set_server_status_spare(server):
    """Set server's status to spare.
    """
    allowed_status = (
        _server.MySQLServer.SECONDARY, _server.MySQLServer.FAULTY
    )
    status = _server.MySQLServer.SPARE
    mode = _server.MySQLServer.OFFLINE
    return _do_set_status(server, allowed_status, status, mode)

def _do_set_status(server, allowed_status, status, mode):
    """Set server's status.
    """
    server.connect()
    alive = server.is_alive()
    allowed_transition = server.status in allowed_status

    if alive and allowed_transition:
        if server.status == _server.MySQLServer.FAULTY:
            _check_requirements(server)
            group = _server.Group.fetch(server.group_id)
            _configure_as_slave(group, server)
        server.status = status
        server.mode = mode
    elif not alive:
        raise _errors.ServerError(
            "Cannot connect to server (%s)." % (server.uuid, )
            )
    elif server.status not in allowed_status:
        raise _errors.ServerError(
            "Cannot put server (%s) whose status is (%s) in "
            "(%s) status." % (server.uuid, server.status, status)
            )

@_events.on_event(SET_SERVER_WEIGHT)
def _set_server_weight(uuid, weight):
    """Set server's weight.
    """
    server = _retrieve_server(uuid)
    weight = float(weight)
    if weight <= 0.0:
        raise _errors.ServerError(
            "Cannot set the server's weight (%s) to a value lower "
            "than or equal to 0.0" % (weight, )
        )
    server.weight = weight

@_events.on_event(SET_SERVER_MODE)
def _set_server_mode(uuid, mode):
    """Set server's mode.
    """
    mode = _retrieve_server_mode(mode)
    server = _retrieve_server(uuid)

    if server.status == _server.MySQLServer.PRIMARY:
        _set_server_mode_primary(server, mode)
    elif server.status == _server.MySQLServer.SECONDARY:
        _set_server_mode_secondary(server, mode)
    elif server.status == _server.MySQLServer.SPARE:
        _set_server_mode_spare(server, mode)
    elif server.status == _server.MySQLServer.FAULTY:
        _set_server_mode_faulty(server, mode)
    else:
        raise _errors.ServerError("Trying to set an invalid mode (%s) "
            "for server (%s)." % (mode, uuid)
            )

def _retrieve_server_mode(mode):
    """Check whether the server's mode is valid or not and
    if an integer was provided retrieve the correspondent
    string.
    """
    valid = False
    try:
        idx = int(mode)
        try:
            mode = _server.MySQLServer.get_mode(idx)
            valid = True
        except IndexError:
            pass
    except ValueError:
        try:
            mode = str(mode).upper()
            _server.MySQLServer.get_mode_idx(mode)
            valid = True
        except ValueError:
            pass

    if not valid:
        raise _errors.ServerError("Trying to set an invalid mode (%s) "
            "for server (%s)." % (mode, uuid)
            )

    return mode

def _set_server_mode_primary(server, mode):
    """Set server's mode when it is a primary.
    """
    allowed_mode = \
        (_server.MySQLServer.WRITE_ONLY, _server.MySQLServer.READ_WRITE)
    _do_set_server_mode(server, mode, allowed_mode)

def _set_server_mode_secondary(server, mode):
    """Set server's mode when it is a secondary.
    """
    allowed_mode = \
        (_server.MySQLServer.OFFLINE, _server.MySQLServer.READ_ONLY)
    _do_set_server_mode(server, mode, allowed_mode)

def _set_server_mode_spare(server, mode):
    """Set server's mode when it is a spare.
    """
    allowed_mode = \
        (_server.MySQLServer.OFFLINE, _server.MySQLServer.READ_ONLY)
    _do_set_server_mode(server, mode, allowed_mode)

def _set_server_mode_faulty(server, mode):
    """Set server's mode when it is a faulty.
    """
    allowed_mode = ()
    _do_set_server_mode(server, mode, allowed_mode)

def _do_set_server_mode(server, mode, allowed_mode):
    """Set server's mode.
    """
    if mode not in allowed_mode:
        raise _errors.ServerError(
            "Cannot set mode to (%s) when the server's (%s) status is (%s)."
            % (mode, server.uuid, server.status)
            )
    server.mode = mode

def _retrieve_server(uuid):
    """Return a server object from a UUID.
    """
    try:
        uuid = _uuid.UUID(uuid)
    except ValueError:
        raise _errors.ServerError("Malformed UUID (%s)." % (uuid, ))

    server = _server.MySQLServer.fetch(uuid)
    if not server:
        raise _errors.ServerError(
            "Server (%s) does not exist." % (uuid, )
            )

    if not server.group_id:
        raise _errors.GroupError(
            "Server (%s) does not belong to a group." % (uuid, )
            )
    return server

def _check_requirements(server):
    """Check if the server fulfils some requirements.
    """
    if not server.check_version_compat((5, 6, 8)):
        raise _errors.ServerError(
            "Server (%s) has an outdated version (%s). 5.6.8 or greater "
            "is required." % (server.uuid, server.version)
            )

    if not server.has_root_privileges():
        _LOGGER.warning(
            "User (%s) needs root privileges on Server (%s, %s).",
            server.user, server.address, server.uuid
            )

    if not server.gtid_enabled or not server.binlog_enabled:
        raise _errors.ServerError(
            "Server (%s) does not have the binary log or gtid enabled."
            % (server.uuid, )
            )

def _configure_as_slave(group, server):
    """Configure the server as a slave.
    """
    try:
        if group.master:
            master = _server.MySQLServer.fetch(group.master)
            master.connect()
            server.read_only = True
            _utils.switch_master(server, master)
    except _errors.DatabaseError as error:
        _LOGGER.debug(
            "Error configuring slave (%s)...", server.uuid, exc_info=error
        )
        raise _errors.ServerError(
            "Error trying to configure Server (%s) as slave."
            % (server.uuid, )
        )

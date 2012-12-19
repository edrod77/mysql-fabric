"""This module provides the necessary interfaces for performing administrative
tasks on replication.
"""
import re
import logging
import uuid as _uuid

import mysql.hub.events as _events
import mysql.hub.server as _server
import mysql.hub.replication as _replication
import mysql.hub.errors as _errors
import mysql.hub.server_utils as _server_utils
import mysql.hub.executor as _executor
import mysql.hub.failure_detector as _detector

_LOGGER = logging.getLogger("mysql.hub.services.replication")

# Discover the replication topology.
DISCOVER_TOPOLOGY = _events.Event()
# Import the replication topology outputed in the previous step.
IMPORT_TOPOLOGY = _events.Event()
def import_topology(pattern_group_id, group_description, uri, user=None,
                    passwd=None, synchronous=True):
    """Try to figure out the replication topology and import it into the
    state store.

    The function tries to find out which servers are connected to a given
    server and, for each server found, it repeats the process recursively.
    It assumes that it can connect to all MySQL Instances using the same
    user and password and that slaves report their host and port through
    *--report-host* and *--report-port*.

    After discovering the replication topology (discover_topology), the
    information is stored into the state store (import_topology).

    :param pattern_group_id: Pattern group's id which is used to generate the
                             groups ids that will be used to create the groups
                             where servers will be imported into.
    :param description: Group's description where servers will be imported
                        into. If more than one group is created, they will
                        share the same description.
    :param uri: Server's uri.
    :param user: Server's user.
    :param passwd: Server's passwd.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    :return: A dictionary with information on the discovered topology.

    In what follows, one will find a figure that depicts the sequence of events
    that happen during the import operation. To ease the presentation some
    names are abbreivated:

    .. seqdiag::

      diagram {
        activation = none;
        === Schedule "discover_topology" ===
        import_topology --> executor [ label = "schedule(discover)" ];
        import_topology <-- executor;
        === Execute "discover_topology" and schedule "import_topology" ===
        executor -> discover [ label = "execute(discover)" ];
        discover --> executor [ label = "schedule(import)" ];
        discover <-- executor;
        executor <- discover;
        === Execute "import_topology" ===
        executor -> import [ label = "execute(import)" ];
        executor <- import;
      }
    """
    jobs = _events.trigger(DISCOVER_TOPOLOGY, pattern_group_id,
                           group_description, uri, user, passwd)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

# Find a slave that is not failing to keep with the master's pace.
FIND_CANDIDATE_SWITCH = _events.Event()
# Check if the candidate is properly configured to become a master.
CHECK_CANDIDATE_SWITCH = _events.Event()
# Block any write access to the master.
BLOCK_WRITE_SWITCH = _events.Event()
# Wait until all slaves synchronize with the master.
WAIT_CANDIDATES_SWITCH = _events.Event()
# Enable the new master by making slaves point to it.
CHANGE_TO_CANDIDATE = _events.Event()
def switch_over(group_id, slave_uuid=None, synchronous=True):
    """Do a switch over.

    If slave_uuid is not provided, the best candidate to become the new master
    is found. Any candidate must have the binary log enabled, should have
    logged the updates executed through the SQL Thread and both candidate and
    master must belong to the same group (i.e. `group_id`). The smaller the lag
    between slave and the master the better. So the candidate which satisfies
    the requirements and has the smaller lag is chosen to become the new master.

    After choosing a candidate (find_candidate and check_candidate), any write
    access to the current master (block_write) is disabled and the slaves are
    synchronized with it (wait_candidates). Failures during the synchronization
    that do not involve the candidate slave are ignored.

    After that, slaves are stopped and configured to point to the new master
    and the database updated setting the new master (changing_to_candidate).

    :param uuid: Group's id.
    :param slave_uuid: Candidate's uuid.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.

    In what follows, one will find a figure that depicts the sequence of events
    that happen during the switch over operation. The figure is split in two
    pieces and names are abreviated in order to ease presentation:

    .. seqdiag::

      diagram {
        activation = none;
        === Schedule "find_candidate_switch" ===
        switch_over --> executor [ label = "schedule(find_candidate)" ];
        switch_over <-- executor;
        find_candidate --> executor [ label = "schedule(check_candidate)" ];
        find_candidate <-- executor;
        executor <- find_candidate;
        === Execute "check_candidate_switch" and schedule "block_write_switch" ===
        executor -> check_candidate [ label = "execute(check_candidate)" ];
        check_candidate --> executor [ label = "schedule(block_write)" ];
        check_candidate <-- executor;
        executor <- check_candidate;
        === Execute "block_write_switch" and schedule "wait_candidates_switch" ===
        executor -> block_write [ label = "execute(block_write)" ];
        block_write --> executor [ label = "schedule(wait_candidates)" ];
        block_write <-- executor;
        executor <- block_write;
        === Continue in the next diagram ===
      }

    .. seqdiag::

      diagram {
        activation = none;
        edge_length = 350;
        === Continuation from previous diagram ===
        === Execute "wait_candidates_catch" and schedule "change_to_candidate" ===
        executor -> wait_candidates [ label = "execute(wait_candidates)" ];
        wait_candidates --> executor [ label = "schedule(change_to_candidate)" ];
        wait_candidates <-- executor;
        executor <- wait_candidates;
        === Execute "change_to_candidate" ===
        executor -> change_to_candidate [ label = "execute(change_to_candidate)" ];
        executor <- change_to_candidate;
      }
    """
    jobs = None
    if not slave_uuid:
        jobs = _events.trigger(FIND_CANDIDATE_SWITCH, group_id)
    else:
        jobs = _events.trigger(CHECK_CANDIDATE_SWITCH, group_id, slave_uuid)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

# Find a slave that was not failing to keep with the master's pace.
FIND_CANDIDATE_FAIL = _events.Event("FAIL_OVER")
# Check if the candidate is properly configured to become a master.
CHECK_CANDIDATE_FAIL = _events.Event()
def fail_over(group_id, slave_uuid=None, synchronous=True):
    """Do a fail over.

    If slave_uuid is not provided, the best candidate to become the new master
    is found. Any candidate must have the binary log enabled and should have
    logged the updates executed through the SQL Thread. If there is a
    registered master, it must not be accessible and both candidate and master
    must belong to the same group (i.e. `group_id`). The smaller the lag
    between slave and the master the better. So the candidate which satisfies
    the requirements and has the smaller lag is chosen to become the new
    master.

    After choosing a candidate (find_candidate and check_candidate), one makes
    the slaves point to the new master and updates the database setting the new
    master (change_to_candidate).

    :param uuid: Group's id.
    :param slave_uuid: Candidate's uuid.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.

    In what follows, one will find a figure that depicts the sequence of event
    that happen during the fail over operation. To ease the presentation some
    names are abbreivated:

    .. seqdiag::

      diagram {
        activation = none;
        === Schedule "find_candidate_fail" ===
        fail_over --> executor [ label = "schedule(find_candidate)" ];
        fail_over <-- executor;
        executor -> find_candidate [ label = "execute(find_candidate)" ];
        find_candidate --> executor [ label = "schedule(check_candidate)" ];
        find_candidate <-- executor;
        executor <- find_candidate;
        === Execute "check_candidate_fail" and schedule "change_to_candidate" ===
        executor -> check_candidate [ label = "execute(check_candidate)" ];
        check_candidate --> executor [ label = "schedule(change_to_candidate)" ];
        check_candidate <-- executor;
        executor <- check_candidate;
        === Execute "change_to_candidate" ===
        executor -> change_to_candidate [ label = "execute(change_to_candidate)" ];
        change_to_candidate <-- executor;
      }
    """
    jobs = None
    if not slave_uuid:
        jobs = _events.trigger(FIND_CANDIDATE_FAIL, group_id)
    else:
        jobs = _events.trigger(CHECK_CANDIDATE_FAIL, group_id, slave_uuid)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

def promote_master(group_id, slave_uuid=None, synchronous=True):
    """Promote a master if there isn't any. See :meth:`fail_over`.

    :param uuid: Group's id.
    :param uuid: Candidate's uuid.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    """
    return fail_over(group_id, slave_uuid, synchronous)

# Block any write access to the master.
BLOCK_WRITE_DEMOTE = _events.Event()
# Wait until all slaves synchronize with the master.
WAIT_CANDIDATES_DEMOTE = _events.Event()
# Stop replication and make slaves point to nowhere.
RESET_CANDIDATES_DEMOTE = _events.Event()
def demote_master(group_id, synchronous=True):
    """Demote the current master if there is any.

    In this case, the group must have a valid and operational master. Any write
    access to the master is blocked, slaves are synchronized with the master,
    stopped and their replication configuration reset. Note that no slave is
    promoted as master.

    :param uuid: Group's id.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.

    In what follows, one will find a figure that depicts the sequence of event
    that happen during the demote operation. To ease the presentation some
    names are abbreivated:

    .. seqdiag::

      diagram {
        activation = none;
        === Schedule "block_write_demote" ===
        demote_master --> executor [ label = "schedule(block_write)" ];
        demote_master <-- executor;
        === Execute "block_write_demote" and schedule "wait_candidates_demote" ===
        executor -> block_write [ label = "execute(block_write)" ];
        block_write --> executor [ label = "schedule(wait_candidates)" ];
        block_write <-- executor;
        executor <- block_write;
        === Execute "wait_candidates_demote" and schedule "reset_candidates_demote" ===
        executor -> wait_candidates [ label = "execute(wait_candidates)" ];
        wait_candidates --> executor [ label = "schedule(reset_candidates)" ];
        wait_candidates <-- executor;
        executor <- wait_candidates;
        === Execute "reset_candidates_demote" ===
        executor -> reset_candidates [ label = "execute(reset_candidates)" ];
        executor <- reset_candidates;
      }
    """
    jobs = _events.trigger(BLOCK_WRITE_DEMOTE, group_id)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

# Check which servers are up or down within a group.
CHECK_GROUP_AVAILABILITY = _events.Event()
def check_group_availability(group_id, synchronous=True):
    """Check if any server within a group has failed.

    :param uuid: Group's id.
    :param synchronous: Whether one should wait until the execution finishes
                        or not.
    """
    jobs = _events.trigger(CHECK_GROUP_AVAILABILITY, group_id)
    assert(len(jobs) == 1)
    return _executor.process_jobs(jobs, synchronous)

@_events.on_event(DISCOVER_TOPOLOGY)
def _discover_topology(job):
    """Discover topology and right after schedule a job to import it.
    """
    pattern_group_id, group_description, uri, user, passwd = job.args
    user = user or "root"
    passwd = passwd or ""
    topology = _do_discover_topology(uri, user, passwd)
    jobs = _events.trigger(IMPORT_TOPOLOGY, pattern_group_id,
                           group_description, topology, user, passwd)
    job.jobs = jobs
    return topology

def _do_discover_topology(uri, user, passwd, discovered_servers=None):
    """Discover topology.

    :param uri: Server's uri.
    :param user: Servers' user
    :param passwd: Servers' passwd.
    :param discovered_servers: List of servers already verified to avoid
                               cycles.
    """
    discovered_mapping = {}
    discovered_servers = discovered_servers or set()

    # Check server's uuid. If the server is not found, an exception is
    # raised.
    str_uuid = _server.MySQLServer.discover_uuid(uri=uri, user=user,
                                                 passwd=passwd)
    if str_uuid in discovered_servers:
        return discovered_mapping

    # Create a server object and connect to it.
    uuid = _uuid.UUID(str_uuid)
    server = _server.MySQLServer(uuid, uri, user, passwd)
    server.connect()

    # Store the server in the discovered set and create a map.
    discovered_mapping[str_uuid] = {"uri" : uri, "slaves": []}
    discovered_servers.add(str_uuid)
    _LOGGER.debug("Found server (%s, %s).", uri, str_uuid)

    # Check if the server has slaves and call _do_discover_topology
    # for each slave.
    _LOGGER.debug("Checking slaves for server (%s, %s).", uri, str_uuid)
    slaves = _replication.get_master_slaves(server)
    for slave in slaves:
        # If the slave does not report its host and port, the master
        # reports an empty value and zero, respectively. In these cases,
        # we skip the slave.
        if slave.Host and slave.Port:
            slave_uri = _server_utils.combine_host_port(
                slave.Host, slave.Port, _server_utils.MYSQL_DEFAULT_PORT)
            # The master may sometimes report stale information. So we
            # check it before trying to use it. Note that if the server
            # does not exist, this will raise an exception and the discover
            # will abort without importing anything.
            slave_str_uuid = _server.MySQLServer.discover_uuid(
                uri=slave_uri, user=user, passwd=passwd)
            slave = _server.MySQLServer(_uuid.UUID(slave_str_uuid), slave_uri,
                                        user, passwd)
            slave.connect()
            if str_uuid == _replication.slave_has_master(slave):
                _LOGGER.debug("Found slave (%s).", slave_uri)
                slave_discovery = _do_discover_topology(slave_uri,
                    user, passwd, discovered_servers)
                if slave_discovery:
                    discovered_mapping[str_uuid]["slaves"].\
                        append(slave_discovery)
    return discovered_mapping

@_events.on_event(IMPORT_TOPOLOGY)
def _import_topology(job):
    """Import topology.
    """
    pattern_group_id, group_description, topology, user, passwd = job.args
    groups = _do_import_topology(pattern_group_id, group_description,
                                 topology, user, passwd)
    for group in groups:
        _detector.FailureDetector.register_group(group)

def _do_import_topology(pattern_group_id, group_description,
                        topology, user, passwd):
    """Import topology.
    """
    master_uuid = topology.keys()[0]
    slaves = topology[master_uuid]["slaves"]
    groups = set()

    # Define group's id from pattern_group_id.
    check = re.compile('\w+-\d+')
    matched = check.match(pattern_group_id)
    if not matched or matched.end() != len(pattern_group_id):
        raise _errors.GroupError("Group pattern's id (%s) is not valid." % \
                                 (pattern_group_id, ))
    base_group_id, number_group_id = pattern_group_id.split("-", 1)
    number_group_id = int(number_group_id) + 1
    group_id = base_group_id + "-" + str(number_group_id)

    # Create group.
    group = _server.Group.add(group_id, group_description)
    _LOGGER.debug("Added group (%s).", str(group))
    groups.add(group_id)

    # Create master of the group.
    master_uri = topology[master_uuid]["uri"]
    server = _server.MySQLServer.add(_uuid.UUID(master_uuid),
                                     master_uri, user, passwd)

    # Added created master to the group.
    group.add_server(server)
    group.master = server.uuid
    _LOGGER.debug("Added server (%s) as master to group (%s).", str(server),
                  str(group))

    # Process slaves.
    for slave in slaves:
        slave_uuid = slave.keys()[0]
        new_slaves = slave[slave_uuid]["slaves"]
        if not new_slaves:
            slave_uri = slave[slave_uuid]["uri"]
            server = _server.MySQLServer.add(_uuid.UUID(slave_uuid),
                                             slave_uri, user, passwd)
        else:
            groups.union(_do_import_topology(group_id, group_description,
                                             slave, user, passwd))
            server = _server.MySQLServer.fetch(_uuid.UUID(slave_uuid))
        group.add_server(server)
        _LOGGER.debug("Added server (%s) as slave to group (%s).", str(server),
                      str(group))
    return groups

@_events.on_event(FIND_CANDIDATE_SWITCH)
def _find_candidate_switch(job):
    """Find the best slave to replace the current master.
    """
    group_id = job.args[0]

    slave_uuid = _do_find_candidate(group_id)

    jobs = _events.trigger(CHECK_CANDIDATE_SWITCH, group_id, slave_uuid)
    job.jobs = jobs

def _do_find_candidate(group_id):
    """Find out the best candidate in a group that may be used to replace a
    master.

    It chooses the slave that has processed more transactions and may become a
    master, i.e. has the binary log enabled.

    :param group_id: Group's id from where a candidate will be chosen.
    :return: Return the uuid of the best candidate to become a master in the
             group.
    """
    # TODO: CHECK FILTERS COMPATIBILITY, CHECK ITS ROLE (SLAVE and SPARE).
    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    master_uuid = None
    if group.master:
        master_uuid = str(group.master)

    chosen_uuid = None
    chosen_gtid_status = None
    for row in group.servers():
        candidate_uuid = row[0]
        if master_uuid != candidate_uuid:
            try:
                candidate = \
                    _server.MySQLServer.fetch(_uuid.UUID(candidate_uuid))
                candidate.connect()
                gtid_status = candidate.get_gtid_status()
                health = _replication.check_master_health(candidate)
                has_valid_master = (master_uuid is None or \
                    _replication.slave_has_master(candidate) == master_uuid)
                can_become_master = False
                if chosen_gtid_status:
                    n_trans = _replication.get_slave_num_gtid_behind(candidate,
                        chosen_gtid_status)
                    if not n_trans and health[0][0] and has_valid_master:
                        chosen_gtid_status = gtid_status
                        chosen_uuid = candidate_uuid
                        can_become_master = True
                elif health[0][0] and has_valid_master:
                    chosen_gtid_status = gtid_status
                    chosen_uuid = candidate_uuid
                    can_become_master = True
                if not can_become_master:
                    _LOGGER.debug("Candidate (%s) cannot become a master due "
                        "to the following reasons: health (%s), valid master "
                        "(%s).", candidate_uuid, health, has_valid_master)
            except _errors.DatabaseError as error:
                _LOGGER.exception(error)

    if not chosen_uuid:
        raise _errors.GroupError("There is no valid candidate in group "
                                 "(%s)." % (group_id, ))
    return chosen_uuid

@_events.on_event(CHECK_CANDIDATE_SWITCH)
def _check_candidate_switch(job):
    """Check if the candidate has all the prerequisites to become the new
    master.
    """
    # TODO: CHECK FILTERS COMPATIBILITY, CHECK ITS ROLE (SLAVE and SPARE).
    group_id, slave_uuid = job.args

    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    if not group.contains_server(slave_uuid):
        raise _errors.GroupError("Group (%s) does not contain server (%s)." \
                                 % (group_id, slave_uuid))

    slave = _server.MySQLServer.fetch(_uuid.UUID(slave_uuid))
    slave.connect()

    health = _replication.check_master_health(slave)
    if not health[0][0]:
        raise _errors.ServerError("Server (%s) is not a valid candidate slave "
                                  "due to the following reason: (%s).", health)

    health = _replication.check_slave_running_health(slave)
    if not health[0][0]:
        raise _errors.ServerError("Server (%s) is not a valid candidate slave "
                                  "due to the following reason: (%s).", health)
    master_uuid = _replication.slave_has_master(slave)

    if not group.contains_server(master_uuid):
        raise _errors.GroupError("Group (%s) does not contain server (%s)." \
                                 % (group_id, master_uuid))

    if not group.master or str(group.master) != master_uuid:
        raise _errors.GroupError("Group (%s) does not contain correct " \
                                 "master (%s)." % (group_id, master_uuid))

    jobs = _events.trigger(BLOCK_WRITE_SWITCH, group_id, master_uuid,
                           slave_uuid)
    job.jobs = jobs

@_events.on_event(BLOCK_WRITE_SWITCH)
def _block_write_switch(job):
    """Block and disable write access to the current master.
    """
    group_id, master_uuid, slave_uuid = job.args

    _do_block_write_master(group_id, master_uuid)

    jobs = _events.trigger(WAIT_CANDIDATES_SWITCH, group_id, master_uuid,
                           slave_uuid)
    job.jobs = jobs

def _do_block_write_master(group_id, master_uuid):
    """Block and disable write access to the current master.
    """
    group = _server.Group.fetch(group_id)

    # Temporarily unset the master in this group.
    group.master = None

    # TODO: IN THE FUTURUE, KILL CONNECTIONS AND MAKE THIS FASTER.
    server = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    server.connect()
    server.read_only = True

@_events.on_event(WAIT_CANDIDATES_SWITCH)
def _wait_candidates_switch(job):
    """Synchronize candidate with master and also all the other slaves.

    In the future, we may improve this and skip the synchronization with
    other slaves.
    """
    group_id, master_uuid, slave_uuid = job.args

    master = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    master.connect()
    slave = _server.MySQLServer.fetch(_uuid.UUID(slave_uuid))
    slave.connect()

    _synchronize(slave, master)

    _do_wait_candidates_catch(group_id, master, [slave_uuid])

    jobs = _events.trigger(CHANGE_TO_CANDIDATE, group_id, slave_uuid)
    job.jobs = jobs

def _do_wait_candidates_catch(group_id, master, skip_servers=None):
    """Synchronize slaves with master.
    """
    skip_servers = skip_servers or []
    skip_servers.append(str(master.uuid))

    group = _server.Group.fetch(group_id)
    for row in group.servers():
        server_uuid = row[0]
        if server_uuid not in skip_servers:
            try:
                server = _server.MySQLServer.fetch(_uuid.UUID(server_uuid))
                server.connect()
                used_master_uuid = _replication.slave_has_master(server)
                if  str(master.uuid) == used_master_uuid:
                    _synchronize(server, master)
                else:
                    _LOGGER.debug("Slave (%s) has a different master "
                        "from group (%s).", str(server.uuid), group_id)
            except _errors.DatabaseError as error:
                _LOGGER.exception(error)

    # At the end, we notify that a server was demoted.
    _events.trigger("SERVER_DEMOTED", group_id, str(master.uuid))

@_events.on_event(CHANGE_TO_CANDIDATE)
def _change_to_candidate(job):
    """Switch to candidate slave.
    """
    group_id, master_uuid = job.args

    master = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    master.connect()
    master.read_only = False

    group = _server.Group.fetch(group_id)
    group.master = master.uuid

    for row in group.servers():
        server_uuid = row[0]
        if server_uuid != master_uuid:
            try:
                server = _server.MySQLServer.fetch(_uuid.UUID(server_uuid))
                server.connect()
                _replication.stop_slave(server, wait=True)
                _replication.switch_master(server, master, master.user,
                                           master.passwd)
                _replication.start_slave(server, wait=True)
            except _errors.DatabaseError as error:
                _LOGGER.exception(error)

    # At the end, we notify that a server was promoted.
    _events.trigger("SERVER_PROMOTED", group_id, master_uuid)

@_events.on_event(FIND_CANDIDATE_FAIL)
def _find_candidate_fail(job):
    """Find the best candidate to replace the failed master.
    """
    group_id = job.args[0]

    slave_uuid = _do_find_candidate(group_id)

    jobs = _events.trigger(CHECK_CANDIDATE_FAIL, group_id, slave_uuid)
    job.jobs = jobs

@_events.on_event(CHECK_CANDIDATE_FAIL)
def _check_candidate_fail(job):
    """Check if the candidate has all the prerequisites to become the new
    master.
    """
    # TODO: CHECK FILTERS COMPATIBILITY, CHECK ITS ROLE (SLAVE and SPARE).
    group_id, slave_uuid = job.args

    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    if not group.contains_server(slave_uuid):
        raise _errors.GroupError("Group (%s) does not contain server (%s)." \
                                 % (group_id, slave_uuid))

    slave = _server.MySQLServer.fetch(_uuid.UUID(slave_uuid))
    slave.connect()

    health = _replication.check_master_health(slave)
    if not health[0][0]:
        raise _errors.ServerError("Server (%s) is not a valid candidate slave "
                                  "due to the following reason: (%s).", health)

    master_uuid = _replication.slave_has_master(slave)
    if master_uuid and not group.contains_server(_uuid.UUID(master_uuid)):
        raise _errors.GroupError("Group (%s) does not contain the master "
                                 "(%s) reported by server (%s)." \
                                 % (group_id, master_uuid, slave_uuid))
    if group.master:
        try:
            server = _server.MySQLServer.fetch(group.master)
            server.connect()
            if server.is_alive():
                _LOGGER.debug("Failover is not possible because "
                    "the master in group (%s) is still alive.", group_id)
        except _errors.DatabaseError as error:
            _LOGGER.debug(error)

        if str(group.master) != master_uuid:
            raise _errors.GroupError("Group (%s) does not contain correct " \
                                     "master (%s)." % (group_id, master_uuid))

    jobs = _events.trigger(CHANGE_TO_CANDIDATE, group_id, slave_uuid)
    job.jobs = jobs

@_events.on_event(BLOCK_WRITE_DEMOTE)
def _block_write_demote(job):
    """Block and disable write access to the current master.
    """
    group_id = job.args[0]

    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    if not group.master:
        raise _errors.GroupError("Group (%s) does not have a master." % \
                                 (group_id, ))

    master_uuid = str(group.master)
    _do_block_write_master(group_id, master_uuid)

    jobs = _events.trigger(WAIT_CANDIDATES_DEMOTE, group_id, master_uuid)
    job.jobs = jobs

@_events.on_event(WAIT_CANDIDATES_DEMOTE)
def _wait_candidates_demote(job):
    """Synchronize slaves with master.
    """
    group_id, master_uuid = job.args

    master = _server.MySQLServer.fetch(_uuid.UUID(master_uuid))
    master.connect()

    _do_wait_candidates_catch(group_id, master)

@_events.on_event(CHECK_GROUP_AVAILABILITY)
def _check_group_availability(job):
    """Check which servers in a group are up and down.
    """
    group_id = job.args[0]
    availability = {}

    group = _server.Group.fetch(group_id)
    if not group:
        raise _errors.GroupError("Group (%s) does not exist." % (group_id, ))

    for row in group.servers():
        candidate_uuid = _uuid.UUID(row[0])
        alive = False
        try:
            server = _server.MySQLServer.fetch(candidate_uuid)
            server.connect()
            alive = server.is_alive()
        except _errors.DatabaseError as error:
            _LOGGER.exception(error)
        availability[str(candidate_uuid)] = \
            (alive, group.master == candidate_uuid)
    return availability

def _synchronize(slave, master):
    """Synchronize a slave with a master and after that stop the slave.
    """
    synced = False
    _replication.start_slave(slave, wait=True)
    master_gtids = master.get_gtid_status()
    while _replication.is_slave_thread_running(slave) and not synced:
        try:
            _replication.wait_for_slave_gtid(slave, master_gtids, timeout=3)
            synced = True
        except _errors.TimeoutError as error:
            _LOGGER.exception(error)
    if not _replication.is_slave_thread_running(slave):
        health = _replication.check_slave_running_health(slave)
        raise _errors.DatabaseError("Slave's thread(s) stopped due to (%s).",
                                    health)
    _replication.stop_slave(slave, wait=True)

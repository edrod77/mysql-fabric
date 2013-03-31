"""This module contains abstractions of MySQL replication features.
"""
import time
import logging
import uuid as _uuid

import mysql.hub.errors as _errors
import mysql.hub.server_utils as _server_utils
import mysql.hub.server as _server

_LOGGER = logging.getLogger(__name__)

_RPL_USER_QUERY = (
    "SELECT user, host, password != '' as has_password "
    "FROM mysql.user "
    "WHERE repl_slave_priv = 'Y'"
)

_MASTER_POS_WAIT = "SELECT MASTER_POS_WAIT(%s, %s, %s)"

_GTID_WAIT = "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(%s, %s)"

IO_THREAD = "IO_THREAD"

SQL_THREAD = "SQL_THREAD"

@_server.server_logging
def get_master_status(server, options=None):
    """Return the master status. In order to ease the navigation through
    the result set, a named tuple is always returned. Look up the `SHOW
    MASTER STATUS` command in the MySQL Manual for further details.

    :param options: Define how the result is formatted and retrieved.
                    See :meth:`mysql.hub.server.MySQLServer.exec_stmt`.
    """
    if options is None:
        options = {}
    options["columns"] = True
    options["raw"] = False
    return server.exec_stmt("SHOW MASTER STATUS", options)

@_server.server_logging
def reset_master(server):
    """Reset the master. Look up the `RESET MASTER` command in the
    MySQL Manual for further details.
    """
    server.exec_stmt("RESET MASTER")

@_server.server_logging
def get_master_rpl_users(server, options=None):
    """Return users that have the `REPLICATION SLAVE PRIVILEGE`. In order
    to ease the navigation through the result set, a named tuple is always
    returned.

    :param options: Define how the result is formatted and retrieved.
                    See :meth:`mysql.hub.server.MySQLServer.exec_stmt`.
    :return:  List of users that have the `REPLICATION SLAVE PRIVILEGE`.
    :rtype: user, host, password = (0, 1).
    """
    if options is None:
        options = {}
    options["columns"] = True
    options["raw"] = False
    return server.exec_stmt(_RPL_USER_QUERY, options)

@_server.server_logging
def get_master_slaves(server, options=None):
    """Return the slaves registered for this master. In order to ease the
    navigation through the result set, a named tuple is always returned.
    Please, look up the `SHOW SLAVE HOSTS` in the MySQL Manual for further
    details.

    :param options: Define how the result is formatted and retrieved.
                    See :meth:`mysql.hub.server.MySQLServer.exec_stmt`.
    """
    if options is None:
        options = {}
    options["columns"] = True
    options["raw"] = False
    return server.exec_stmt("SHOW SLAVE HOSTS", options)

@_server.server_logging
def check_master_health(server):
    """Check replication health on the master.

    This method checks if the master is set up correctly to operate in a
    replication environment. It returns a tuple with a bool to indicate
    if health is Ok (True), and a list that contains errors encountered
    during the checks, if there is any. Basically, it checks if the master
    is alive and kicking, the binary log is enabled, the gtid is enabled,
    the server is able to log the updates through the SQL Thread and finally
    if there is a user that has the `REPLICATION SLAVE PRIVILEGE`. One may
    find in what follows the set of possible returned values::

      ret = check_master_health(server)
      assert ret == [(False, ["Cannot connect to server"])],
        ("This is what happens when the master is not running.")

      ret = check_master_health(server)
      assert ret == [(False, ["No binlog on master"])],
        ("This is what happens when the binary log is not enabled.")

      ret = check_master_health(server)
      assert ret == [(False, ["There are no users with replication "
        privileges."])],
        ("This is what happens when there are no users with the appropriate "
         "privileges.")

    In other words, one needs to check whether ret[0][0] is True or False.
    If it is True, the server is set up correctly to operate as a master.
    Otherwise, there is some problem and ret[0][1] may be used to get
    detailed information on the problem.
    """
    errors = []
    rpl_ok = True

    # TODO: Check if this is the bet way of returning data on health.
    # Quoting Mats: I still find it strange to return a list of a single
    # tuple consisting of a boolean and a list where the boolean indicate
    # if the list has length zero or not.
    if not server.is_alive():
        return [(False, ["Cannot connect to server"])]

    # Check for binlogging
    if not server.binlog_enabled:
        errors.append("No binlog on master.")
        rpl_ok = False

    if not server.gtid_enabled:
        errors.append("No gtid on master.")
        rpl_ok = False

    if not server.get_variable("LOG_SLAVE_UPDATES"):
        errors.append("log_slave_updates is not set.")
        rpl_ok = False

    # TODO: FILTERS?

    # See if there is at least one user with rpl privileges
    if not get_master_rpl_users(server):
        errors.append("There are no users with replication privileges.")
        rpl_ok = False

    return [(rpl_ok, errors)]

@_server.server_logging
def get_slave_status(server, options=None):
    """Return the slave status. In order to ease the navigation through
    the result set, a named tuple is always returned. Look up the `SHOW
    SLAVE STATUS` command in the MySQL Manual for further details.

    :param options: Define how the result is formatted and retrieved.
                    See :meth:`mysql.hub.server.MySQLServer.exec_stmt`.
    """
    if options is None:
        options = {}
    options["columns"] = True
    options["raw"] = False
    return server.exec_stmt("SHOW SLAVE STATUS", options)

@_server.server_logging
def is_slave_thread_running(server, threads=None):
    """Check to see if slave's threads are running smoothly.
    """
    return _check_condition(server, threads, True)

@_server.server_logging
def slave_has_master(server):
    """Return the master's uuid to which the slave is connected to.
    If the slave is not connected to any master, what happens if the
    IO Thread is stopped, None is returned.

    :return: Master's uuid or None.
    :rtype: String.
    """
    ret = get_slave_status(server)
    if ret:
        try:
            _uuid.UUID(ret[0].Master_UUID) # TODO: In the future, return UUID.
            return ret[0].Master_UUID
        except ValueError:
            pass
    return None

@_server.server_logging
def get_num_gtid(gtids, server_uuid=None):
    """Return the number of transactions represented in gtids.

    By default this function considers any server in gtids. So if one wants
    to count transactions from a specific server, the parameter server_uuid
    must be defined.

    :param gtids: Set of transactions.
    :param server_uuid: Which server one should consider where None means
                        all.
    """
    sid = None
    difference = 0
    for gtid in gtids.split(","):
        # Exctract the server_uuid and the trx_ids.
        trx_ids = None
        if gtid.find(":") != -1:
            sid, trx_ids = gtid.split(":")
        else:
            if not sid:
                raise _errors.ProgrammingError("Malformed GTID (%s)." % gtid)
            trx_ids = gtid

        # Ignore differences if server_uuid is passed and does
        # not match.
        if server_uuid and str(server_uuid).upper() != sid.upper():
            continue

        # Check the difference.
        difference += 1
        if trx_ids.find("-") != -1:
            lgno, rgno = trx_ids.split("-")
            difference += int(rgno) - int(lgno)
    return difference

# TODO: master_uuid should be a list and not a single value.
#       This can be useful to determine the set of values one
#       is insterested in.
def get_slave_num_gtid_behind(server, master_gtids, master_uuid=None):
    """Get the number of transactions behind the master.

    :param master_gtids: GTID information retrieved from the master.
        See :meth:`mysql.hub.server.MySQLServer.get_gtid_status`.
    :param master_uuid: Master which is used as the basis for comparison.
    :return: Number of transactions behind master.
    """
    gtids = None
    master_gtids = master_gtids[0].GTID_EXECUTED
    slave_gtids = server.get_gtid_status()[0].GTID_EXECUTED

    # The subtract function does not accept empty strings.
    if master_gtids == "" and slave_gtids != "":
        raise _errors.InvalidGtidError(
            "It is not possible to check the lag when the "
            "master's GTID is empty."
            )
    elif master_gtids == "" and slave_gtids == "":
        return 0
    elif slave_gtids == "":
        gtids = master_gtids
    else:
        assert (master_gtids != "" and slave_gtids != "")
        gtids = server.exec_stmt("SELECT GTID_SUBTRACT(%s,%s)",
                                 {"params": (master_gtids, slave_gtids)})[0][0]
        if gtids == "":
            return 0
    return get_num_gtid(gtids, master_uuid)

#TODO: In the WAIT FUNCTION, we need to verify if any problem is
#      reported so that we don't wait forever.
@_server.server_logging
def start_slave(server, threads=None, wait=False, timeout=None):
    """Start the slave. Look up the `START SLAVE` command in the MySQL
    Manual for further details.

    :param threads: Determine which threads shall be started.
    :param wait: Determine whether one shall wait until the thread(s)
                 start(s) or not.
    :type wait: Bool
    :param timeout: Time in seconds after which one gives up waiting for
                    thread(s) to start.

    The parameter `threads` determine which threads shall be started. If
    None is passed as parameter, both the `SQL_THREAD` and the `IO_THREAD`
    are started.
    """
    threads = threads or ()
    server.exec_stmt("START SLAVE " + ", ".join(threads))
    if wait:
        wait_for_slave_thread(server, timeout=timeout, wait_for_running=True,
                              threads=threads)

@_server.server_logging
def stop_slave(server, threads=None, wait=False, timeout=None):
    """Stop the slave. Look up the `STOP SLAVE` command in the MySQL
    Manual for further details.

    :param threads: Determine which threads shall be stopped.
    :param wait: Determine whether one shall wait until the thread(s)
                 stop(s) or not.
    :type wait: Bool
    :param timeout: Time in seconds after which one gives up waiting for
                    thread(s) to stop.

    The parameter `threads` determine which threads shall be stopped. If
    None is passed as parameter, both the `SQL_THREAD` and the `IO_THREAD`
    are stopped.
    """
    threads = threads or ()
    server.exec_stmt("STOP SLAVE " + ", ".join(threads))
    if wait:
        wait_for_slave_thread(server, timeout=timeout, wait_for_running=False,
                              threads=threads)

@_server.server_logging
def reset_slave(server, clean=False):
    """Reset the slave. Look up the `RESET SLAVE` command in the MySQL
    Manual for further details.

    :param clean: Do not save master information such as host, user, etc.
    """
    param = "ALL" if clean else ""
    server.exec_stmt("RESET SLAVE %s" % (param, ))

@_server.server_logging
def wait_for_slave_thread(server, timeout=None, wait_for_running=True,
                          threads=None):
    """Wait until slave's threads stop or start.

    If timeout is None, one waits indefinitely until the condition is
    achieved. If the timeout period expires prior to achieving the
    condition the exception TimeoutError is raised.

    :param timeout: Number of seconds one waits until the condition is
                    achieved. If it is None, one waits indefinitely.
    :param wait_for_running: If one should check if threads are
                             running or stopped.
    :type check_if_running: bool
    :param threads: Which threads should be checked.
    :type threads: `SQL_THREAD` or `IO_THREAD`.
    """
    while (timeout is None or timeout > 0) and \
           not _check_condition(server, threads, wait_for_running):
        time.sleep(1)
        timeout = timeout - 1 if timeout is not None else None
    if not _check_condition(server, threads, wait_for_running):
        _LOGGER.debug("Error waiting for slave thread to "
                      "either start or stop.")
        _LOGGER.debug("Slave's status (%s) after error.",
                      get_slave_status(server))
        raise _errors.TimeoutError("Error waiting for slave thread to "
                                   "either start or stop.")

@_server.server_logging
def wait_for_slave(server, binlog_file, binlog_pos, timeout=3):
    """Wait for the slave to read the master's binlog up to a specified
    position.

    This methods call the MySQL function `SELECT MASTER_POS_WAIT`. If
    the timeout period expires prior to achieving the condition the
    :class:`mysql.hub.errors.TimeoutError` exception is raised.

    :param binlog_file: Master's binlog file.
    :param binlog_pos: Master's binlog file position.
    :param timeout: Maximum number of seconds to wait for the condition to
                    be achieved.
    :return: True if slave has read to the file and pos, and
             False if slave is behind.
    """
    # Wait for slave to read the master log file
    res = server.exec_stmt(_MASTER_POS_WAIT,
        {"params": (binlog_file, binlog_pos, timeout), "raw" : False })

    if res is None or res[0] is None or res[0][0] is None:
        return False
    elif res[0][0] > -1:
        return True
    else:
        assert(res[0][0] == -1)
        _LOGGER.debug("Error waiting for slave to catch up. "\
                      "Binary log (%s, %s).", binlog_file, binlog_pos)
        _LOGGER.debug("Slave's status (%s) after error.",
                      get_slave_status(server))
        raise _errors.TimeoutError("Error waiting for slave to catch up. "\
                                   "Binary log (%s, %s)." %
                                   (binlog_file, binlog_pos))

@_server.server_logging
def wait_for_slave_gtid(server, master_gtids, timeout=3):
    """Wait for the slave to read the master's GTIDs.

    The function `SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS` is called until the
    slave catches up. If the timeout period expires prior to achieving
    the condition the :class:`mysql.hub.errors.TimeoutError` exception is
    raised.

    :param master_gtids: Result of running `get_gtid_status`.
    :param timeout: Timeout for waiting for slave to catch up.
    :return: True if slave has read all GTIDs or False if slave is
             behind.
    """
    # Check servers for GTID support
    if not server.gtid_enabled:
        raise _errors.ProgrammingError("Global Transaction IDs are not "\
                                       "supported.")
    gtid = master_gtids[0].GTID_EXECUTED
    _LOGGER.debug("Slave (%s).",
        _server_utils.split_host_port(server.address,
                                      _server_utils.MYSQL_DEFAULT_PORT))
    _LOGGER.debug("Query (%s).", _GTID_WAIT % (gtid.strip(','), timeout))
    res = server.exec_stmt(_GTID_WAIT,
        {"params": (gtid.strip(','), timeout), "raw" : False })
    _LOGGER.debug("Return code (%s).", res)
    if res is None or res[0] is None or res[0][0] is None:
        return False
    elif res[0][0] > -1:
        return True
    else:
        assert(res[0][0] == -1)
        _LOGGER.debug("Error waiting for slave to catch up. "\
                      "GTID (%s).", master_gtids[0].GTID_EXECUTED)
        _LOGGER.debug("Slave's status (%s) after error.",
                      get_slave_status(server))
        raise _errors.TimeoutError("Error waiting for slave to catch up. "\
                                   "GTID (%s)." %
                                   (master_gtids[0].GTID_EXECUTED, ))

@_server.server_logging
def switch_master(slave, master, master_user, master_passwd=None,
                  from_beginning=True, master_log_file=None,
                  master_log_pos=None):
    """Switch slave to a new master by executing the `CHANGE MASTER` command.
    Look up the command in the MySQL Manual for further details.

    This method forms the `CHANGE MASTER` command based on the current
    settings of the slave along with the parameters provided and execute
    it. No prerequisites are checked.

    :param master: Master class instance.
    :param master_user: Replication user.
    :param master_passwd: Replication user password.
    :param from_beginning: If True, start from beginning of logged events.
    :param master_log_file: Master's log file (not needed for GTID).
    :param master_log_pos: master's log file position (not needed for GTID).
    """
    commands = []
    params = []
    master_host, master_port = _server_utils.split_host_port(master.address,
        _server_utils.MYSQL_DEFAULT_PORT)

    commands.append("MASTER_HOST = %s")
    params.append(master_host)
    commands.append("MASTER_PORT = %s")
    params.append(int(master_port))
    if master_user:
        commands.append("MASTER_USER = %s")
        params.append(master_user)
    if master_passwd:
        commands.append("MASTER_PASSWORD = %s")
        params.append(master_passwd)
    if slave.gtid_enabled:
        commands.append("MASTER_AUTO_POSITION = 1")
    elif not from_beginning:
        commands.append("READ_MASTER_LOG_FILE = %s")
        params.append(master_log_file)
        if master_log_pos >= 0:
            commands.append("MASTER_LOG_POS = %s" % master_log_pos)
            params.append(master_log_pos)

    slave.exec_stmt("CHANGE MASTER TO " + ", ".join(commands),
                    {"params": tuple(params)})

@_server.server_logging
def check_slave_running_health(server):
    """Check replication health of the slave.

    This method checks if the slave is setup correctly to operate in a
    replication environment. It returns a tuple with a bool to indicate
    if health is Ok (True), and a list that contains errors encountered
    during the checks, if there is any.

    Specifically, it checks if `SQL_THREAD` and `IO_THREAD` are running.

    One needs to check whether ret[0][0] is True or False. If it is True,
    the slave is health. Otherwise, there is some problem and ret[0][1]
    may be used to get detailed information on the problem.

    :param server: Slave.
    :return: Return a list reporting on the slave's health.
    :rtype: [(health, ["error", "error"])]
    """
    errors = []
    rpl_ok = True

    if not server.is_alive():
        return [(False, ["Cannot connect to server"])]

    ret = get_slave_status(server)

    if not ret:
        return [(False, ["Not connected or not running."])]

    # Check slave status for errors, threads activity
    if ret[0].Slave_IO_Running.upper() != "YES":
        errors.append("IO thread is not running.")
        rpl_ok = False
    if ret[0].Slave_SQL_Running.upper() != "YES":
        errors.append("SQL thread is not running.")
        rpl_ok = False
    if ret[0].Last_IO_Errno > 0:
        errors.append(ret[0].Last_IO_Error)
        rpl_ok = False
    if ret[0].Last_SQL_Errno > 0:
        errors.append(ret[0].Last_SQL_Error)
        rpl_ok = False

    return [(rpl_ok, errors)]

# TODO: We need to check if this function that encapsulates others
#       are really necessary.
@_server.server_logging
def check_slave_delay_health(server, master_uuid, master_status,
                             master_gtid_status, max_delay_behind,
                             max_pos_behind, max_gtid_behind):
    """Check replication health of the slave.

    This method checks if the slave is setup correctly to operate in a
    replication environment. It returns a tuple with a bool to indicate
    if health is Ok (True), and a list that contains errors encountered
    during the checks, if there is any.

    Specifically, it checks if the slave's delay is within determined
    boundaries.

    One needs to check whether ret[0][0] is True or False. If it is True,
    the slave is health. Otherwise, there is some problem and ret[0][1]
    may be used to get detailed information on the problem.

    :param server: Slave.
    :param master_uuid: Master's uuid.
    :param master_status: Result of running `get_master_status`.
    :param master_gtid_status: Result of running `get_gtid_status`.
    :param max_delay_behind: Maximum acceptable delay.
    :param mas_pos_behind: Maximum acceptable difference in positions.
    :param mas_gtid_behind: Maximum acceptable difference number of
                            transactions.
    :return: Return a list reporting on the slave's health.
    :rtype: [(health, ["error", "error"])]
    """
    errors = []
    rpl_ok = True

    if not server.is_alive():
        return [(False, ["Cannot connect to server"])]

    ret = get_slave_status(server)

    if not ret:
        return [(False, ["Not connected or not running."])]

    # Check slave delay with threshhold of SBM, and master's log pos
    s_delay = ret[0].SQL_Delay
    delay = s_delay if s_delay is not None else 0
    if delay > max_delay_behind:
        errors.append("Slave delay is %s seconds behind master." %
                      delay)
        if ret[0].SQL_Remaining_Delay:
            errors.append(ret[0].SQL_Remaining_Delay)
        rpl_ok = False

    # Check master position
    # TODO: Improve by creating a function to calculate the diff.
    # Similar to the get_slave_num_gtid_behind.
    if not server.gtid_enabled:
        if ret[0].Master_Log_File != master_status[0].File:
            errors.append("Wrong master log file.")
            rpl_ok = False
        elif (ret[0].Read_Master_Log_Pos + max_pos_behind) \
             < master_status[0].Position:
            errors.append("Slave's master position exceeds maximum.")
            rpl_ok = False

    # Check GTID trans behind.
    elif server.gtid_enabled:
        num_gtids_behind = get_slave_num_gtid_behind(server,
                                                     master_gtid_status,
                                                     master_uuid)
        if num_gtids_behind > max_gtid_behind:
            errors.append("Slave has %s transactions behind master." %
                          num_gtids_behind)
            rpl_ok = False

    return [(rpl_ok, errors)]

# TODO: We need to check if this function that encapsulates others
#       are really necessary.
def check_slave_health(server, master_uuid, master_status,
                       master_gtid_status, max_delay_behind,
                       max_pos_behind, max_gtid_behind):
    """Check replication health of the slave.
    See :meth:`check_slave_running_health` and
    :meth:`check_slave_delay_health`.
    """
    errors = []
    rpl_ok = True

    # TODO: FILTERS?
    ret_running = check_slave_running_health(server)
    ret_delay = check_slave_delay_health(server, master_uuid, master_status,
                                         master_gtid_status, max_delay_behind,
                                         max_pos_behind, max_gtid_behind)

    rpl_ok = ret_running[0][0] and ret_delay[0][0]
    errors.extend(ret_running[0][1])
    errors.extend(ret_delay[0][1])

    return [(rpl_ok, errors)]

def _check_condition(server, threads, check_if_running):
    """Check if slave's threads are either running or stopped.

    :param threads: Which threads should be checked.
    :type threads: `SQL_THREAD` or `IO_THREAD`.
    :param check_if_running: If one should check whether threads are
                             running or stopped.
    :type check_if_running: Bool
    """
    if threads is None:
        threads = (SQL_THREAD, IO_THREAD)
    assert(isinstance(threads, tuple))

    io_status = not check_if_running
    sql_status = not check_if_running

    ret = get_slave_status(server)
    io_status = sql_status = not check_if_running
    check_stmt = "YES" if check_if_running else "NO"
    if ret:
        io_status = ret[0].Slave_IO_Running.upper() == check_stmt
        sql_status = ret[0].Slave_SQL_Running.upper() == check_stmt

    achieved = True
    if SQL_THREAD in threads:
        achieved = achieved and sql_status

    if IO_THREAD in threads:
        achieved = achieved and io_status

    return achieved

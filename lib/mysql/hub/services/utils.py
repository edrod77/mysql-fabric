""" This module contains functions that are called from the services interface
and change MySQL state. Notice though that after a failure the system does no
undo the changes made through the execution of these functions.
"""
import mysql.hub.replication as _replication

def switch_master(slave, master):
    """Make slave point to master.

    :param slave: Slave.
    :param master: Master.
    """
    _replication.stop_slave(slave, wait=True)
    _replication.switch_master(slave, master, master.user, master.passwd)
    _replication.start_slave(slave, wait=True)


def set_read_only(server, read_only):
    """Set server to read only mode.

    :param read_only: Either read-only or not.
    """
    server.read_only = read_only


def stop_master_as_slave(master):
    """Stop a master as a slave.

    :param master: Master.
    """
    _replication.stop_slave(master, wait=True)
    _replication.reset_slave(master, clean=True)


def synchronize(slave, master):
    """Synchronize a slave with a master and after that stop the slave.

    :param slave: Slave.
    :param master: Master.
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
        health = _replication.check_slave_running_health(slave) # TODO: IMPROVE HEALTH INFORMATION
        raise _errors.DatabaseError("Slave's thread(s) stopped due to (%s).",
                                    health)
    _replication.stop_slave(slave, wait=True)

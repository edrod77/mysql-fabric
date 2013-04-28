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


def reset_slave(slave):
    """Stop slave and reset it.

    :param slave: slave.
    """
    _replication.stop_slave(slave, wait=True)
    _replication.reset_slave(slave, clean=True)


def synchronize(slave, master):
    """Synchronize a slave with a master and after that stop the slave.


    :param slave: Slave.
    :param master: Master.
    """
    master_gtids = master.get_gtid_status()
    _replication.wait_for_slave_gtid(slave, master_gtids, timeout=0)


def stop_slave(slave):
    _replication.stop_slave(slave, wait=True)

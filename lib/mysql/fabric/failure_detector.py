"""This modules contais a simple failure detector which is used by Fabric
to monitor the availability of servers within groups.

If a master cannot be accessed through the method
:meth:`~mysql.fabric.server.MySQLServer.is_alive`, one must consider
that it has failed and proceed with the election of a new master if
there is any candidate slave that can become one. In particular, the
failure detector does not choose any new master but only triggers some
events (:const:`~mysql.fabric.events.SERVER_LOST` and
:const:`~mysql.fabric.events.FAIL_OVER`) and registered listener(s) will
take the necessary and appropriate actions.

Similar to a master, if a slave has failed, an event
(:const:`~mysql.fabric.events.SERVER_LOST`) is triggered and registered
listener(s) will take the necessary and appropriate actions.

See :meth:`~mysql.fabric.server.MySQLServer.is_alive`.
See :class:`~mysql.fabric.services.highavailability.CheckHealth`.
See :class:`~mysql.fabric.services.highavailability.FailOver`.
See :const:`~mysql.fabric.events.SERVER_LOST`.
"""
import threading
import time
import logging

import mysql.fabric.errors as _errors
import mysql.fabric.persistence as _persistence

from mysql.fabric.events import (
    trigger,
    )

_LOGGER = logging.getLogger(__name__)

class FailureDetector(object):
    """Responsible for periodically checking if a set of servers within a
    group is alive.

    It does so by connecting to these servers and executing a query (i.e.
    :meth:`mysql.fabric.server.MySQLServer.is_alive`.
    """
    LOCK = threading.Condition()
    GROUPS = {}
    # By default, every second the failure detector checks if the servers
    # within a group are alive.
    CHECK_PERIOD = 1

    @staticmethod
    def register_groups():
        """Upon startup initializes a failure detector for each group.
        """
        from mysql.fabric.server import Group
        _LOGGER.info("Starting failure detector.")
        for row in Group.groups_by_status(Group.ACTIVE):
            FailureDetector.register_group(row[0])

    @staticmethod
    def register_group(group_id):
        """Start a failure detector for a group.

        :param group_id: Group's id.
        """
        _LOGGER.info("Monitoring group (%s).", group_id)
        with FailureDetector.LOCK:
            if group_id not in FailureDetector.GROUPS:
                detector = FailureDetector(group_id)
                detector.start()
                FailureDetector.GROUPS[group_id] = detector

    @staticmethod
    def unregister_group(group_id):
        """Stop a failure detector for a group.

        :param group_id: Group's id.
        """
        _LOGGER.info("Stop monitoring group (%s).", group_id)
        with FailureDetector.LOCK:
            if group_id in FailureDetector.GROUPS:
                detector = FailureDetector.GROUPS[group_id]
                detector.shutdown()
                del FailureDetector.GROUPS[group_id]

    @staticmethod
    def unregister_groups():
        """Upon shutdown stop all failure detectors that are running.
        """
        _LOGGER.info("Stopping failure detector.")
        with FailureDetector.LOCK:
            for detector in FailureDetector.GROUPS.values():
                detector.shutdown()
            FailureDetector.GROUPS = {}

    # TODO: USE A CONFIGURATION FILE TO DEFINE THE PERIOD.
    #       MAYBE WE SHOULD ALSO INHERIT FROM Threading.
    def __init__(self, group_id, sleep=None):
        """Constructor for FailureDetector.
        """
        self.__group_id = group_id
        self.__thread = None
        self.__check = False
        self.__sleep = sleep or FailureDetector.CHECK_PERIOD

    def start(self):
        """Start the failure detector.
        """
        self.__check = True
        self.__thread = threading.Thread(target=self._run,
            name="FailureDetector(" + self.__group_id + ")")
        self.__thread.daemon = True
        self.__thread.start()

    def shutdown(self):
        """Stop the failure detector.
        """
        self.__check = False

    def _run(self):
        """Function that verifies servers' availabilities.
        """
        from mysql.fabric.server import (
            Group, MySQLServer
        )
        ignored_status = [MySQLServer.FAULTY, MySQLServer.OFFLINE]

        _persistence.init_thread()
        while self.__check:
            try:
                group = Group.fetch(self.__group_id)
                if group is not None:
                    for server in group.servers():
                        if server.status in ignored_status or \
                            server.is_alive():
                            continue
                        _LOGGER.info("Server (%s) in group (%s) has "
                            "been lost.", server.uuid, self.__group_id)
                        if group.master == server.uuid:
                            _LOGGER.info("Master (%s) in group (%s) has "
                                "been lost.", server.uuid, self.__group_id)
                            trigger(
                                "FAIL_OVER", set([self.__group_id]),
                                self.__group_id
                            )
                        else:
                            trigger(
                                "SERVER_LOST", set([self.__group_id]),
                                self.__group_id, server.uuid
                            )
                        server.status = MySQLServer.FAULTY
            except (_errors.ExecutorError, _errors.DatabaseError):
                pass
            except Exception as error:
                _LOGGER.exception(error)
            time.sleep(self.__sleep)
        _persistence.deinit_thread()

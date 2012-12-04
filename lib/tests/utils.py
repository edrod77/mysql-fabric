"""Module holding support utilities for tests.
"""
import uuid as _uuid

import mysql.hub.utils as _utils
import mysql.hub.server as _server
import mysql.hub.replication as _replication
import mysql.hub.executor as _executor

class SkipTests(type):
    """Metaclass which is used to skip test cases as follows::

      import unittest
      import tests.utils as _utils

      class TestCaseClass(unittest.TestCase):
        __metaclass__ = _utils.SkipTests
    """
    def __new__(cls, name, bases, dct):
        """Create a new instance for SkipTests.
        """
        for name, item in dct.items():
            if callable(item) and name.startswith("test"):
                dct[name] = None
        return type.__new__(cls, name, bases, dct)

class MySQLInstances(_utils.Singleton):
    """Contain a reference to the available set of MySQL Instances that can be
    used in a test case.
    """
    def __init__(self):
        self.__uris = []
        self.__instances = {}

    def add_uri(self, uri):
        assert(isinstance(uri, basestring))
        self.__uris.append(uri)

    def get_uri(self, number):
        return self.__uris[number]

    def get_instance(self, number):
        return self.__instances[number]

    def destroy_instances(self):
        for instance in self.__instances.values():
            _replication.stop_slave(instance, wait=True)
            _replication.reset_slave(instance, clean=True)
        self.__instances = {}

    def configure_instances(self, topology, user, passwd):
        persister = _executor.Executor().persister

        for number in topology.keys():
            master_uri = self.get_uri(number)

            master_uuid = _server.MySQLServer.discover_uuid(uri=master_uri,
                                                            user=user,
                                                            passwd=passwd)
            master = _server.MySQLServer(_uuid.UUID(master_uuid), master_uri,
                                         user, passwd)
            master.connect()
            _replication.stop_slave(master, wait=True)
            _replication.reset_master(master)
            _replication.reset_slave(master)
            master.read_only = False
            self.__instances[number] = master
            for slave_topology in topology[number]:
                slave = self.configure_instances(slave_topology, user, passwd)
                slave.read_only = True
                _replication.switch_master(slave, master, user, passwd)
                _replication.start_slave(slave, wait=True)
            return master

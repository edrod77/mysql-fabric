"""Module holding support utilities for tests.
"""
import sys
import threading
import time
import uuid as _uuid
import xmlrpclib

from mysql.hub import (
    config as _config,
    persistence as _persistence,
    replication as _replication,
    server as _server,
    utils as _utils,
    )

from mysql.hub.sharding import ShardMapping, RangeShardingSpecification

class MySQLInstances(_utils.Singleton):
    """Contain a reference to the available set of MySQL Instances that can be
    used in a test case.
    """
    def __init__(self):
        """Constructor for MySQLInstances.
       """
        self.__uris = []
        self.__instances = {}

    def add_uri(self, uri):
        """Add the address of a MySQL Instance that can be used in the test
        cases.

        :param uri: MySQL's address.
        """
        assert(isinstance(uri, basestring))
        self.__uris.append(uri)

    def get_number_uris(self):
        """Return the number of MySQL Instances' address registered.
        """
        return len(self.__uris)

    def get_uri(self, number):
        """Return the n-th address registerd.
        """
        assert(number < len(self.__uris))
        return self.__uris[number]

    def get_instance(self, number):
        """Return the n-th instance created through the
        :meth:`configure_instances` method.
     
        :return: Return a MySQLServer object.
        """
        assert(number < len(self.__uris))
        return self.__instances[number]

    def destroy_instances(self):
        """Destroy the MySQLServer objects created through the
        :meth:`configure_instances` method.
        """
        for instance in self.__instances.values():
            _replication.stop_slave(instance, wait=True)
            _replication.reset_slave(instance, clean=True)
        self.__instances = {}

    def configure_instances(self, topology, user, passwd):
        """Configure a replication topology using the MySQL Instances
        previously registerd.

        :param topology: Topology to be configured.
        :param user: MySQL Instances' user.
        :param passwd: MySQL Instances' password.

        This method can be used as follows::

          import tests.utils as _test_utils

          topology = {1 : [{2 : []}, {3 : []}]}
          instances = _test_utils.MySQLInstances()
          instances.configure_instances(topology, "root", "")

        Each instance in the topology is represented as a dictionary whose
        keys are references to addresses that will be retrieved through
        the :meth:`get_uri` method and values are a list of slaves.

        So after calling :meth:`configure_instances` method, one can get a
        reference to an object, MySQLServer, through the :meth:`get_instance`
        method.
        """
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

class ShardingUtils(object):
    @staticmethod
    def compare_shard_mapping(shard_mapping_1, shard_mapping_2):
        """Compare two sharding mappings with each other. Two sharding
        specifications are equal if they are defined on the same table, on
        the same column, are of the same type and use the same sharding
        specification.

        :param shard_mapping_1: shard mapping
        :param shard_mapping_2: shard mapping

        :return True if shard mappings are equal
                False if shard mappings are not equal
        """
        return isinstance(shard_mapping_1, ShardMapping) and \
                isinstance(shard_mapping_2, ShardMapping) and \
               shard_mapping_1.table_name == \
                        shard_mapping_2.table_name and \
                shard_mapping_1.column_name == \
                        shard_mapping_2.column_name and \
                shard_mapping_1.type_name == \
                            shard_mapping_2.type_name and \
                shard_mapping_1.sharding_specification == \
                            shard_mapping_2.sharding_specification
    @staticmethod
    def compare_range_specifications(range_specification_1,
                                         range_specification_2):
        """Compare two RANGE specification definitions. They are equal if they
        belong to the same sharding scheme, define the same upper and lower
        bound and map to the same server.

        :param range_specification_1: Range Sharding Specification
        :param range_specification_2: Range Sharding Specification

        :return True if Range Sharding Specifications are equal
                False if Range Sharding Specifications are not equal
        """
        return isinstance(range_specification_1, RangeShardingSpecification) and \
                isinstance(range_specification_2, RangeShardingSpecification) and \
                range_specification_1.name == \
                        range_specification_2.name and \
                range_specification_1.lower_bound == \
                        range_specification_2.lower_bound and \
                range_specification_1.upper_bound == \
                        range_specification_2.upper_bound and \
                range_specification_1.group_id == range_specification_2.group_id

def setup_xmlrpc():
    from __main__ import options, xmlrpc_next_port
    params = {
        'protocol.xmlrpc': {
            'address': 'localhost:%d' % (xmlrpc_next_port,),
            },
        'storage': {
            'address': options.host + ":" + str(options.port),
            'user': options.user,
            'password': options.password,
            'database': 'fabric',
            },
        }
    config = _config.Config(None, params, True)
    xmlrpc_next_port += 1

    # Set up the manager
    from mysql.hub.commands.start import start
    manager_thread = threading.Thread(target=start, args=(config, ),
                                      name="Services")
    manager_thread.start()

    attempts = 10
    while attempts > 0 and not manager_thread.is_alive():
        time.sleep(1)
        attempts -= 1

    # Set up the client
    url = "http://%s" % (config.get("protocol.xmlrpc", "address"),)
    proxy = xmlrpclib.ServerProxy(url)

    while True:
        try:
            proxy.ping()
            break
        except Exception as err:
            time.sleep(1)

    return (manager_thread, proxy)
    
def teardown_xmlrpc(manager, proxy):
    proxy.shutdown()
    manager.join()
    _persistence.deinit()

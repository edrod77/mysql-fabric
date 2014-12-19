#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
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

"""Module holding support utilities for tests.
"""

import glob
import logging
import os
import re
import unittest
import uuid
import xmlrpclib

from mysql.fabric import (
    replication as _replication,
    server as _server,
    utils as _utils,
    command,
)

from mysql.fabric.sharding import (
    ShardMapping,
    RangeShardingSpecification,
    HashShardingSpecification,
)

import mysql.connector

import mysql.fabric.protocols.xmlrpc as _xmlrpc

_LOGGER = logging.getLogger(__name__)

def _make_result(names, types, rows):
    rset = command.ResultSet(names=names, types=types)
    for row in rows:
        rset.append_row(row)
    return rset

def make_servers_result(rows):
    return _make_result(
        names=('server_uuid', 'group_id', 'host', 'port', 'mode', 'status', 'weight'),
        types=(str, str, str, int, int, int, float),
        rows=rows,
    )

def make_tables_result(rows):
    return _make_result(
        names=('schema_name', 'table_name', 'column_name', 'mapping_id', 'range_check'),
        types=(str, str, str, int, int),
        rows=rows,
    )

def make_mapping_result(rows):
    return _make_result(
        names=('mapping_id', 'type_name', 'global_group_id'),
        types=(int, str, str),
        rows=rows,
    )

def make_index_result(rows):
    return _make_result(
        names=('lower_bound', 'mapping_id', 'shard_id', 'group_id'),
        types=(str, int, int, str),
        rows=rows,
    )

def make_info_result(rows):
    return _make_result(
        names=('schema_name', 'table_name', 'column_name', 'lower_bound',
               'shard_id', 'type_name', 'group_id', 'global_group'),
        types=(str, str, str, str, int, str, str, str),
        rows=rows,
    )

def make_shard_mapping_list_result(rows):
    return _make_result(
        names=('mapping_id', 'type_name', 'table_name', 'global_group',
               'column_name'),
        types=(int, str, str, str, str),
        rows=rows,
    )

def make_servers_lookup_result(rows):
    return _make_result(
        names=('server_uuid', 'address', 'status', 'mode', 'weight'),
        types=(str, str, str, str, float),
        rows=rows,
    )

def configure_decoupled_master(group, master):
    """Configure master in a group by changing the group.master and
    mode and status properties without redirecting slaves to the
    specified master.

    :param group: Group object.
    :param master: Reference to the master.
    :type master: MySQLServer, UUID or None
    """
    for server in group.servers():
        server.mode = _server.MySQLServer.READ_ONLY
        server.status = _server.MySQLServer.SECONDARY
    group.master = None

    if master and isinstance(master, uuid.UUID):
        master = _server.MySQLServer.fetch(master)

    if master and isinstance(master, _server.MySQLServer):
        group.master = master.uuid
        master.mode = _server.MySQLServer.READ_WRITE
        master.status = _server.MySQLServer.PRIMARY
    elif not master:
        assert("Invalid instance")


class MySQLInstances(_utils.Singleton):
    """Contain a reference to the available set of MySQL Instances that can be
    used in a test case.
    """
    def __init__(self):
        """Constructor for MySQLInstances.
       """
        super(MySQLInstances, self).__init__()
        self.__addresses = []
        self.__instances = {}
        self.user = None
        self.passwd = None
        self.root_user = None
        self.root_passwd = None
        self.state_store_address = None

    def add_address(self, address):
        """Add the address of a MySQL Instance that can be used in the test
        cases.

        :param address: MySQL's address.
        """
        assert(isinstance(address, basestring))
        self.__addresses.append(address)

    def get_number_addresses(self):
        """Return the number of MySQL Instances' address registered.
        """
        return len(self.__addresses)

    def get_address(self, number):
        """Return the n-th address registerd.
        """
        assert(number < len(self.__addresses))
        return self.__addresses[number]

    def get_instance(self, number):
        """Return the n-th instance created through the
        :meth:`configure_instances` method.

        :return: Return a MySQLServer object.
        """
        assert(number < len(self.__addresses))
        return self.__instances[number]

    def destroy_instances(self):
        """Destroy the MySQLServer objects created through the
        :meth:`configure_instances` method.
        """
        cleanup_environment()

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
          user = instances.user
          passwd = instances.passwd
          instances.configure_instances(topology, user, passwd)

        Each instance in the topology is represented as a dictionary whose
        keys are references to addresses that will be retrieved through
        the :meth:`get_address` method and values are a list of slaves.

        So after calling :meth:`configure_instances` method, one can get a
        reference to an object, MySQLServer, through the :meth:`get_instance`
        method.
        """
        for number in topology.keys():
            master_address = self.get_address(number)

            master_uuid = _server.MySQLServer.discover_uuid(
                address=master_address
            )
            master = _server.MySQLServer(
                uuid.UUID(master_uuid), master_address, user, passwd)
            master.connect()
            master.read_only = False
            self.__instances[number] = master
            for slave_topology in topology[number]:
                slave = self.configure_instances(slave_topology, user, passwd)
                slave.read_only = True
                _replication.switch_master(slave, master, user, passwd)
                _replication.start_slave(slave, wait=True)
            return master

class ShardingUtils(object):
    """Utility class for sharding.
    """

    @staticmethod
    def compare_shard_mapping(shard_mapping_1, shard_mapping_2):
        """Compare two sharding mappings with each other. Two sharding
        specifications are equal if they have the same id, are defined
        on the same table, on the same column, are of the same type and
        use the same global group.

        :param shard_mapping_1: shard mapping
        :param shard_mapping_2: shard mapping

        :return True if shard mappings are equal
                False if shard mappings are not equal
        """
        return isinstance(shard_mapping_1, ShardMapping) and \
                isinstance(shard_mapping_2, ShardMapping) and \
               shard_mapping_1.shard_mapping_id == \
                        shard_mapping_2.shard_mapping_id and \
               shard_mapping_1.table_name == \
                        shard_mapping_2.table_name and \
                shard_mapping_1.column_name == \
                        shard_mapping_2.column_name and \
                shard_mapping_1.type_name == \
                            shard_mapping_2.type_name and \
                shard_mapping_1.global_group == \
                            shard_mapping_2.global_group

    @staticmethod
    def compare_range_specifications(range_specification_1,
                                     range_specification_2):
        """Compare two RANGE specification definitions. They are equal if they
        belong to the same shard mapping, define the same upper and lower
        bound, map to the same shard, and are in the same state.

        :param range_specification_1: Range Sharding Specification
        :param range_specification_2: Range Sharding Specification

        :return: If Range Sharding Specifications are equal, it returns True.
                 False if Range Sharding Specifications are not equal
        """
        return \
            isinstance(range_specification_1, RangeShardingSpecification) and \
            isinstance(range_specification_2, RangeShardingSpecification) and \
                range_specification_1.shard_mapping_id == \
                    range_specification_2.shard_mapping_id and \
                int(range_specification_1.lower_bound) == \
                    int(range_specification_2.lower_bound) and \
                range_specification_1.shard_id == \
                    range_specification_2.shard_id

    @staticmethod
    def compare_hash_specifications(hash_specification_1,
                                     hash_specification_2):
        """Compare two HASH specification definitions. They are equal if they
        belong to the same shard mapping, define the same upper and lower
        bound, map to the same shard, and are in the same state.

        :param hash_specification_1: Hash Sharding Specification
        :param hash_specification_2: Hash Sharding Specification

        :return: If Hash Sharding Specifications are equal, it returns True.
                 False if Hash Sharding Specifications are not equal
        """
        return \
            isinstance(hash_specification_1, HashShardingSpecification) and \
            isinstance(hash_specification_2, HashShardingSpecification) and \
                hash_specification_1.shard_mapping_id == \
                    hash_specification_2.shard_mapping_id and \
                hash_specification_1.lower_bound == \
                    hash_specification_2.lower_bound and \
                hash_specification_1.shard_id == \
                    hash_specification_2.shard_id

def cleanup_environment():
    """Clean up the existing environment
    """
    #Clean up information on instances.
    MySQLInstances().__instances = {}

    #Clean up information in the state store.
    uuid_server = _server.MySQLServer.discover_uuid(
        MySQLInstances().state_store_address, MySQLInstances().root_user,
        MySQLInstances().root_passwd
    )
    server = _server.MySQLServer(uuid.UUID(uuid_server),
        MySQLInstances().state_store_address, MySQLInstances().root_user,
        MySQLInstances().root_passwd
    )
    server.connect()

    server.set_foreign_key_checks(False)
    tables = server.exec_stmt(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE "
        "TABLE_SCHEMA = 'fabric' and TABLE_TYPE = 'BASE TABLE'"
    )
    for table in tables:
        server.exec_stmt("TRUNCATE fabric.%s" % (table[0], ))
    server.set_foreign_key_checks(True)

    #Remove all the databases from the running MySQL instances
    #other than the standard ones
    server_count = MySQLInstances().get_number_addresses()

    for i in range(0, server_count):
        uuid_server = _server.MySQLServer.discover_uuid(
            MySQLInstances().get_address(i)
        )
        server = _server.MySQLServer(
            uuid.UUID(uuid_server), MySQLInstances().get_address(i)
        )
        server.connect()
        _replication.stop_slave(server, wait=True)

        server.set_foreign_key_checks(False)
        databases = server.exec_stmt("SHOW DATABASES")
        for database in databases:
            if database[0] not in _server.MySQLServer.NO_USER_DATABASES:
                server.exec_stmt(
                    "DROP DATABASE IF EXISTS %s" % (database[0], )
                )
        server.set_foreign_key_checks(True)

        _replication.reset_master(server)
        _replication.reset_slave(server, clean=True)

    for __file in glob.glob(os.path.join(os.getcwd(), "*.sql")):
        os.remove(__file)

def setup_xmlrpc():
    """Configure XML-RPC.
    """
    from __main__ import xmlrpc_next_port

    # Set up the client
    proxy = xmlrpclib.ServerProxy(
        "http://localhost:{0}".format(xmlrpc_next_port),
        allow_none=True
    )
    return (None, proxy)

def setup_mysqlrpc():
    """Configure MySQL-RPC
    """
    from __main__ import mysqlrpc_next_port

    cnx = mysql.connector.connect(host='localhost', port=mysqlrpc_next_port)

    return cnx

class TestCase(unittest.TestCase):
    """Test case class that defines some convenience methods for MySQL
    Fabric test cases.

    """
    def check_xmlrpc_simple(self, packet, checks, has_error=False,
                            index=0, rowcount=None):
        """Perform assertion checks on a row of a result set.

        This will perform basic assertion checks on a command result
        returned by an XML-RPC server proxy. It will decode the result
        into a command result and pick a row from the first result set
        in the result (assuming there were no error) and do an
        equality comparison with the fields provided in the ``checks``
        parameter.

        :param packet: The Python data structure from the XML-RPC server.

        :param checks: Dictionary of values to check.

        :param has_error: True if errors are expected for this packet,
        False otherwise. Default to False.

        :param index: Index of row to check. Default to the first row
        of the result set, if there is any.

        :param rowcount: Number of rows expected in the result set, or
        None if no check should be done.

        :return: Return a dictionary of the actual contents of the
        row, or an empty dictionary in the event of an error.

        """

        result = _xmlrpc._decode(packet)

        self.assertEqual(bool(result.error), has_error, str(result))

        if not has_error:
            # Some commands are successful but have no result sets
            # anyway (e.g., set_logging_level).
            if len(result.results) == 0:
                return {}

            if rowcount is not None:
                self.assertEqual(result.results[0].rowcount, rowcount, str(result))

            if result.results[0].rowcount == 0:
                return {}

            # Check that there is enough rows in the first result set
            self.assertTrue(
                result.results[0].rowcount > index, str(result)
            )

            # Create a dictionary from this row.
            info = dict(
                zip([col.name for col in result.results[0].columns],
                result.results[0][index])
            )

            for key, value in checks.items():
                self.assertTrue(key in info, str(result))
                self.assertEqual(info[key], value, "[%s != %s]:\n%s" % (
                    info[key], value, str(result))
                )

            # For convenience, allowing the simple result to be used
            # by callers.
            return info
        return {}

    def check_xmlrpc_command_result(self, packet, is_syncronous=True,
                                    has_error=False, returns=None):
        """Check that a packet from a procedure execution is sane.

        This check that the first command result set, which contain
        result of execution, is sane.

        """

        check = re.compile('\w{8}(-\w{4}){3}-\w{12}')
        result = _xmlrpc._decode(packet)

        self.assertEqual(bool(result.error), has_error, str(result))

        # If the procedure did not have an error, first result set,
        # first row, first column contain UUID of procedure. Just
        # check that it looks like a UUID.
        if not has_error:
            self.assertNotEqual(check.match(result.results[0][0][0]), None)

        # If the call was synchronous and succeeded, check that there
        # is at least 2 result sets and that the second result set
        # contain more than zero jobs.
        if is_syncronous and not has_error:
            self.assertTrue(len(result.results) > 1, str(result))
            self.assertNotEqual(result.results[1].rowcount, 0,
                                "had %d result sets" % len(result.results))

        if not has_error and returns is not None:
            self.assertTrue(len(result.results) > 1, str(result))
            self.assertEqual(result.results[0].rowcount, 1, str(result))
            self.assertEqual(result.results[0][0][3], returns)

    def check_xmlrpc_result(self, packet, expected, index=0):
        """Compare the result set of a command result with an expected value.

        Order of rows in result sets are important and have to match.

        :param ResultSet expected: Expected result set.

        """

        result = _xmlrpc._decode(packet)

        self.assertFalse(result.error, "Error: '%s'" % result.error)
        self.assertTrue(len(result.results) > index, str(result))
        self.assertEqual(result.results[index].columns, expected.columns)

        for row, exp in zip(result.results[index], expected):
            self.assertEqual(row, exp)

    def check_xmlrpc_iter(self, packet, index=0, rowcount=None):
        """Iterate over a result set and do some basic integrity checking first.
        """

        result = _xmlrpc._decode(packet)
        self.assertTrue(len(result.results) > index, str(result))
        if rowcount is not None:
            self.assertEqual(
                result.results[index].rowcount,
                rowcount,
                str(result)
            )

        names = [ c.name for c in result.results[index].columns ]
        for row in result.results[index]:
            yield dict(zip(names, row))


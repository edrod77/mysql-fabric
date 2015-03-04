#
# Copyright (c) 2013,2015, Oracle and/or its affiliates. All rights reserved.
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

"""Unit tests for administrative on servers.
"""
import unittest
import uuid as _uuid
import tests.utils
import re
import sys

from mysql.fabric import (
    executor as _executor,
    server as _server,
    replication as _replication,
)

from tests.utils import (
    MySQLInstances,
    fetch_test_server,
)

import mysql.fabric.protocols.xmlrpc as _xmlrpc

import logging
_LOGGER = logging.getLogger(__name__)

class TestServerServices(tests.utils.TestCase):
    """Test server service interface.
    """
    uuid_cre = re.compile('\w{8}(-\w{4}){3}-\w{12}')

    def check_xmlrpc_command_result(self, packet, has_error,
                                    error_message=None, is_syncronous=True):
        "Check that a packet from a procedure execution is sane."

        result = _xmlrpc._decode(packet)

        if has_error:
            message = "No error but error expected"
        else:
            message = "Error: %s" % result.error
        self.assertEqual(bool(result.error), has_error, message)

        # If the procedure had an error, check the error message provided
        # it was requested to do so, i.e error_message is not None.
        if has_error and error_message:
            self.assertEqual(result.error, error_message)

        # If the procedure did not have an error, first result set,
        # first row, first column contain UUID of procedure. Just
        # check that it looks like a UUID.
        if not has_error:
            self.assertNotEqual(
                self.uuid_cre.match(result.results[0][0][0]),
                None,
                str(result)
            )

        # If the call was synchronous and succeeded, check that there
        # is at least 2 result sets and that the second result set
        # contain more than zero jobs.
        if is_syncronous and not has_error:
            self.assertTrue(len(result.results) > 1, str(result))
            self.assertNotEqual(result.results[1].rowcount, 0,
                                "had %d result sets" % len(result.results))

    def check_xmlrpc_get_uuid(self, packet, has_error):
        result = _xmlrpc._decode(packet)

        # If the procedure did not have an error, first result set,
        # first row, first column contain UUID of server. Just
        # check that it looks like a UUID.
        if not has_error:
            self.assertNotEqual(self.uuid_cre.match(result.results[0][0][0]), None)

        return result.results[0][0][0]

    def setUp(self):
        """Configure the existing environment
        """
        _LOGGER.critical("\n\nStart of test-fixture-SetUp(): %s\n" %
                         (self._testMethodName))
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _LOGGER.debug("End of test-fixture-SetUp(): %s\n" %
                      (self._testMethodName))

    def tearDown(self):
        """Clean up the existing environment
        """
        _LOGGER.debug("Start of test-fixture-tearDown(): %s" %
                      (self._testMethodName))
        tests.utils.cleanup_environment()
        _LOGGER.debug("End of test-fixture-tearDown(): %s" %
                      (self._testMethodName))

    def test_create_group_events(self):
        """Test creating a group by calling group.create().
        """
        # Look up groups.
        status = self.proxy.group.lookup_groups()
        result = _xmlrpc._decode(status)
        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].rowcount, 0)

        # Insert a new group.
        status = self.proxy.group.create("group", "Testing group...")
        self.check_xmlrpc_command_result(status, False)

        # Try to insert a group twice.
        status = self.proxy.group.create("group", "Testing group...")
        self.check_xmlrpc_command_result(status, True)

        # Look up groups.
        status = self.proxy.group.lookup_groups()
        self.check_xmlrpc_simple(status, {
            "group_id" : "group",
            "description" : "Testing group...",
            "failure_detector" : False,
        })

        # Look up a group.
        status = self.proxy.group.lookup_groups("group")
        self.check_xmlrpc_simple(status, {
            "group_id" : "group",
            "description" : "Testing group...",
            "failure_detector" : False,
        })

        # Try to look up a group that does not exist.
        status = self.proxy.group.lookup_groups("group_1")
        self.check_xmlrpc_simple(status, {}, has_error=True)

        # Update a group.
        status = self.proxy.group.description("group", "Test Test Test")
        self.check_xmlrpc_command_result(status, False)

        # Try to update group that does not exist.
        status = self.proxy.group.description("group_1", "Test Test Test")
        self.check_xmlrpc_command_result(status, True)

    def test_add_server_events(self):
        """Test adding a server by calling group.add().
        """
        # Insert a new server.
        address = tests.utils.MySQLInstances().get_address(0)
        self.proxy.group.create("group_1", "Testing group...")
        status = self.proxy.group.add("group_1", address)
        self.check_xmlrpc_command_result(status, False)

        # Try to insert a server twice.
        status = self.proxy.group.add("group_1", address)
        self.check_xmlrpc_command_result(status, True)

        # Try to insert a server into a non-existing group.
        status = self.proxy.group.add("group_2", address)
        self.check_xmlrpc_command_result(status, True)

        # Look up servers.
        status = self.proxy.group.lookup_servers("group_1")
        info = self.check_xmlrpc_simple(status, {
            'address': address,
            'status': _server.MySQLServer.SECONDARY,
            'mode': _server.MySQLServer.READ_ONLY,
            'weight': _server.MySQLServer.DEFAULT_WEIGHT,
        })
        
        # Try to look up servers in a group that does not exist.
        status = self.proxy.group.lookup_servers("group_x")
        self.check_xmlrpc_command_result(status, True)

        # Look up a server using UUID.
        status = self.proxy.group.lookup_servers("group_1", info['server_uuid'])
        self.check_xmlrpc_simple(status, {
            "server_uuid" : info['server_uuid'],
            "address" : address,
            "status" : _server.MySQLServer.SECONDARY,
            "mode" : _server.MySQLServer.READ_ONLY,
            "weight" : _server.MySQLServer.DEFAULT_WEIGHT,
        })

        # Look up a server using HOST:PORT.
        status = self.proxy.group.lookup_servers("group_1", address)
        self.check_xmlrpc_simple(status, {
            "server_uuid" : info['server_uuid'],
            "address" : address,
            "status" : _server.MySQLServer.SECONDARY,
            "mode" : _server.MySQLServer.READ_ONLY,
            "weight" : _server.MySQLServer.DEFAULT_WEIGHT
        })

        # Try to look up a server in a group that does not exist.
        status = self.proxy.group.lookup_servers("group_x", info['server_uuid'])
        self.check_xmlrpc_simple(status, {}, has_error=True)
        
        # Try to look up a server that does not exist.
        status = self.proxy.group.lookup_servers(
            "group_1",
            "cc75b12c-98d1-414c-96af-9e9d4b179678"
        )
        self.check_xmlrpc_simple(status, {}, has_error=True)

        # Try to look up a server that does not exist
        status = self.proxy.server.lookup_uuid("unknown:15000")
        self.check_xmlrpc_simple(status, {}, has_error=True)

    def test_destroy_group_events(self):
        """Test destroying a group by calling group.destroy().
        """
        # Prepare group and servers
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)

        # Remove a group.
        self.proxy.group.create("group", "Testing group...")
        status = self.proxy.group.destroy("group")
        self.check_xmlrpc_command_result(status, False)

        # Try to remove a group twice.
        status = self.proxy.group.destroy("group")
        self.check_xmlrpc_command_result(status, True,
            error_message="GroupError: Group (group) does not exist."
        )

        # Try to remove a group where there are servers.
        self.proxy.group.create("group_1", "Testing group...")
        self.proxy.group.add("group_1", address_1)
        status = self.proxy.group.destroy("group_1")
        self.check_xmlrpc_command_result(status, True,
            error_message=("GroupError: Cannot destroy a group (group_1) "
                "which has associated servers."
            )
        )
        self.proxy.group.remove("group_1", address_1)

        # Try to remove a group that is used by shards.
        self.proxy.group.create("group_global")
        self.proxy.group.add("group_global", address_1)
        self.proxy.group.promote("group_global")
        status = self.proxy.sharding.create_definition("RANGE", "group_global")
        shard_mapping_id = status[2]
        status = self.proxy.group.destroy("group_global")
        self.check_xmlrpc_command_result(status, True,
            error_message=("GroupError: Cannot destroy a group (group_global) "
                "which is used as a global group in a shard definition (1)."
            )
        )

        self.proxy.group.create("group")
        self.proxy.group.add("group", address_2)
        self.proxy.group.promote("group")
        self.proxy.sharding.add_table(shard_mapping_id, "db1.t1", "user")
        self.proxy.sharding.add_shard(shard_mapping_id, "group/0", "ENABLED", 0)
        status = self.proxy.group.destroy("group")
        self.check_xmlrpc_command_result(status, True,
            error_message=("GroupError: Cannot destroy a group (group) which "
                "is associated to a shard (1)."
            )
        )

    def test_remove_server_events(self):
        """Test removing a server by calling group.remove().
        """
        # Prepare group and servers
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(2)
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.create("group_1", "Testing group...")
        self.proxy.group.add("group_1", address_1)
        self.proxy.group.add("group_1", address_2)
        status_1 = self.proxy.server.lookup_uuid(address_1)
        info_1 = self.check_xmlrpc_simple(status_1, {})

        # Try to remove a server from a non-existing group.
        status = self.proxy.group.remove(
            "group_2", "bb75b12b-98d1-414c-96af-9e9d4b179678"
            )
        self.check_xmlrpc_command_result(status, True)

        # Try to remove a server with an invalid uuid.
        status = self.proxy.group.remove(
            "group_1", "bb-98d1-414c-96af-9"
            )
        self.check_xmlrpc_command_result(status, True)

        # Try to remove a server that does not exist.
        status = self.proxy.group.remove(
            "group_1",
            "bb75b12c-98d1-414c-96af-9e9d4b179678"
            )
        self.check_xmlrpc_command_result(status, True)

        # Try to remove a server that is master within the group.
        group = _server.Group.fetch("group_1")
        tests.utils.configure_decoupled_master(group,
                                               _uuid.UUID(info_1['uuid']))
        status = self.proxy.group.remove("group_1", info_1['uuid'])
        self.check_xmlrpc_command_result(status, True)

        # Remove a server using its UUID.
        group = _server.Group.fetch("group_1")
        tests.utils.configure_decoupled_master(group, None)
        status = self.proxy.group.remove("group_1", info_1['uuid'])
        self.check_xmlrpc_command_result(status, False)

        # Remove a server using its address.
        status = self.proxy.group.remove("group_1", address_2)
        self.check_xmlrpc_command_result(status, False)

    def test_group_status(self):
        """Test group's status by calling group.activate()/group.deactive().
        """
        # Prepare group and servers
        address = tests.utils.MySQLInstances().get_address(0)
        user = tests.utils.MySQLInstances().user
        passwd = tests.utils.MySQLInstances().passwd
        self.proxy.group.create("group", "Testing group...")
        self.proxy.group.add("group", address)
        status = self.proxy.server.lookup_uuid(address)
        self.check_xmlrpc_simple(status, {})

        # Try to activate a non-existing group.
        status = self.proxy.group.activate("group-1")
        self.check_xmlrpc_command_result(status, True)

        # Activate group.
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.INACTIVE)
        status = self.proxy.group.activate("group")
        self.check_xmlrpc_command_result(status, False)
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.ACTIVE)

        # Deactivate group.
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.ACTIVE)
        status = self.proxy.group.deactivate("group")
        self.check_xmlrpc_command_result(status, False)
        group = _server.Group.fetch("group")
        self.assertEqual(group.status, _server.Group.INACTIVE)

        # Try to deactivate a non-existing group.
        status = self.proxy.group.deactivate("group-1")
        self.check_xmlrpc_command_result(status, True)

    def test_server_status(self):
        """Test server's status by calling server.status().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        self.proxy.group.add("group", address_1)
        status_1 = self.proxy.server.lookup_uuid(address_1)
        info_1 = self.check_xmlrpc_simple(status_1, {})
        uuid_1 = info_1['uuid']
        error_uuid = 'foo'

        self.proxy.group.add("group", address_2)
        status = self.proxy.server.lookup_uuid(address_2)
        self.check_xmlrpc_simple(status, {})

        # Try to set the status when the server's id is invalid.
        status = self.proxy.server.set_status("INVALID", "spare")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the status when the server does not exist.
        status = self.proxy.server.set_status(error_uuid, "spare")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the status when the server does not belong to a group.
        server = _server.MySQLServer.fetch(uuid_1)
        server.group_id = None
        status = self.proxy.server.set_status(uuid_1, "spare")
        self.check_xmlrpc_command_result(status, True)
        server.group_id = "group"

        # Try to set a spare server when the server is a master.
        group = _server.Group.fetch("group")
        tests.utils.configure_decoupled_master(group, _uuid.UUID(uuid_1))
        status = self.proxy.server.set_status(uuid_1, "spare")
        self.check_xmlrpc_command_result(status, True)
        tests.utils.configure_decoupled_master(group, None)

        # Set a spare server when the server is faulty.
        # Note this is using HOST:PORT instead of using UUID.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.server.set_status(address_1, "spare")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)

        # Set a spare server when the server is secondary.
        server.status = _server.MySQLServer.SECONDARY
        status = self.proxy.server.set_status(uuid_1, "spare")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)

        # Try to set a status that does not exist.
        # Note though that this uses idx.
        status = self.proxy.server.set_status(uuid_1, 20)
        self.check_xmlrpc_command_result(status, True)

        # Set a spare server when the server is secondary.
        # Note though that this uses idx.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.server.set_status(uuid_1,
            _server.MySQLServer.get_status_idx("SPARE"))
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SPARE)

        # Try to set a secondary server that does not exist.
        status = self.proxy.server.set_status(error_uuid, "secondary")
        self.check_xmlrpc_command_result(status, True)

        # Try to set a secondary server when the server is a master.
        group = _server.Group.fetch("group")
        tests.utils.configure_decoupled_master(group, _uuid.UUID(uuid_1))
        status = self.proxy.server.set_status(uuid_1, "secondary")
        self.check_xmlrpc_command_result(status, True)
        tests.utils.configure_decoupled_master(group, None)

        # Try to set a secondary server when the server is faulty.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.FAULTY
        status = self.proxy.server.set_status(uuid_1, "secondary")
        self.check_xmlrpc_command_result(status, True)

        # Try to set a faulty server.
        status = self.proxy.server.set_status(uuid_1, "faulty")
        self.check_xmlrpc_command_result(status, True)

        # Try to set a primary server.
        status = self.proxy.server.set_status(uuid_1, "primary")
        self.check_xmlrpc_command_result(status, True)

        # Try to set an invalid status.
        status = self.proxy.server.set_status(uuid_1, "INVALID")
        self.check_xmlrpc_command_result(status, True)

    def test_server_weight(self):
        """Test server's weight by calling server.weight().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        self.proxy.group.add("group", address_1)
        status = self.proxy.server.lookup_uuid(address_1)
        info = self.check_xmlrpc_simple(status, {})
        uuid_1 = info['uuid']
        error_uuid = 'foo'

        # Try to set the weight when the server's id is invalid.
        status = self.proxy.server.set_weight("INVALID", "0.1")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the weight when the server does not exist.
        status = self.proxy.server.set_weight(error_uuid, "0.1")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the mode when the server does not belong to a group.
        server = _server.MySQLServer.fetch(uuid_1)
        server.group_id = None
        status = self.proxy.server.set_weight(uuid_1, "0.1")
        self.check_xmlrpc_command_result(status, True)
        server.group_id = "group"

        # Try to set the weight to zero.
        status = self.proxy.server.set_weight(uuid_1, "0.0")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the weight to a negative value.
        status = self.proxy.server.set_weight(uuid_1, "-1.0")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the weight to a string.
        status = self.proxy.server.set_weight(uuid_1, "error")
        self.check_xmlrpc_command_result(status, True)

        # Set the weight to 0.1.
        status = self.proxy.server.set_weight(uuid_1, 0.1)
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.weight, 0.1)

        # Set the weight to 1.0.
        # Note this is using HOST:PORT instead of using UUID.
        status = self.proxy.server.set_weight(address_1, 1.0)
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.weight, 1.0)

    def test_server_mode(self):
        """Test server's mode by calling server.mode().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        status = self.proxy.group.add("group", address_1)
        self.check_xmlrpc_command_result(status, False)
        status = self.proxy.server.lookup_uuid(address_1)
        info = self.check_xmlrpc_simple(status, {})
        uuid_1 = info['uuid']
        error_uuid = 'foobar'

        # Try to set the mode when the server's id is invalid.
        status = self.proxy.server.set_mode("INVALID", "read_write")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the mode when the server does not exist.
        status = self.proxy.server.set_mode(error_uuid, "read_write")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the mode when the server does not belong to a group.
        server = _server.MySQLServer.fetch(uuid_1)
        server.group_id = None
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.check_xmlrpc_command_result(status, True)
        server.group_id = "group"

        # Try to set the READ_WRITE mode when the server is a secondary.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.status, _server.MySQLServer.SECONDARY)
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the WRITE_ONLY mode when the server is a secondary.
        status = self.proxy.server.set_mode(uuid_1, "write_only")
        self.check_xmlrpc_command_result(status, True)

        # Set the OFFLINE mode when the server is a secondary.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)
        status = self.proxy.server.set_mode(uuid_1, "offline")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.OFFLINE)

        # Set the READ_ONLY mode when the server is a secondary.
        status = self.proxy.server.set_mode(uuid_1, "read_only")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)

        # Try to set the READ_WRITE mode when the server is a spare.
        server = _server.MySQLServer.fetch(uuid_1)
        server.status = _server.MySQLServer.SPARE
        self.assertEqual(server.status, _server.MySQLServer.SPARE)
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the WRITE_ONLY mode when the server is a spare.
        status = self.proxy.server.set_mode(uuid_1, "write_only")
        self.check_xmlrpc_command_result(status, True)

        # Set the OFFLINE mode when the server is a spare.
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)
        status = self.proxy.server.set_mode(uuid_1, "offline")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.OFFLINE)

        # Set the READ_ONLY mode when the server is a spare.
        status = self.proxy.server.set_mode(uuid_1, "read_only")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_ONLY)

        # Try to set the OFFLINE mode when the server is a primary.
        server = _server.MySQLServer.fetch(uuid_1)
        group = _server.Group.fetch(server.group_id)
        tests.utils.configure_decoupled_master(group, server)
        self.assertEqual(server.mode, _server.MySQLServer.READ_WRITE)
        self.assertEqual(server.status, _server.MySQLServer.PRIMARY)
        status = self.proxy.server.set_mode(uuid_1, "offline")
        self.check_xmlrpc_command_result(status, True)

        # Try to set the READ_ONLY mode when the server is a primary.
        status = self.proxy.server.set_mode(uuid_1, "read_only")
        self.check_xmlrpc_command_result(status, True)

        # Set the WRITE_ONLY mode when the server is a primary.
        status = self.proxy.server.set_mode(uuid_1, "write_only")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.WRITE_ONLY)

        # Set the READ_WRITE mode when the server is a primary.
        status = self.proxy.server.set_mode(uuid_1, "read_write")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_WRITE)

        # Trye to set an invalid mode using idx.
        status = self.proxy.server.set_mode(uuid_1, 20)
        self.check_xmlrpc_command_result(status, True)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_WRITE)

        # Set the WRITE_ONLY mode when the server is a primary.
        # Note this uses idx
        status = self.proxy.server.set_mode(uuid_1,
            _server.MySQLServer.get_mode_idx("WRITE_ONLY")
        )
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.WRITE_ONLY)

        # Set the READ_WRITE mode when the server is a primary.
        # Note this is using HOST:PORT instead of using UUID.
        status = self.proxy.server.set_mode(address_1, "read_write")
        self.check_xmlrpc_command_result(status, False)
        server = _server.MySQLServer.fetch(uuid_1)
        self.assertEqual(server.mode, _server.MySQLServer.READ_WRITE)

    def test_add_slave(self):
        """Test whether some servers are made slaves or not.
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_0 = tests.utils.MySQLInstances().get_address(0)
        address_1 = tests.utils.MySQLInstances().get_address(1)
        address_2 = tests.utils.MySQLInstances().get_address(2)
        status_uuid = self.proxy.server.lookup_uuid(address_0)
        uuid_0 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        uuid_1 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        uuid_2 = status_uuid[2]
        status = self.proxy.group.add("group", address_0)
        self.check_xmlrpc_command_result(status, False)

        status = self.proxy.group.promote("group")
        self.check_xmlrpc_command_result(status, False)

        # Add servers and check that they are made slaves.
        self.proxy.group.add("group", address_1)
        self.proxy.group.add("group", address_2)
        status =  self.proxy.group.lookup_servers("group")
        result = _xmlrpc._decode(status)
        for row in result.results[0]:
            info = dict(zip([ c.name for c in result.results[0].columns ], row))
            self.assertEqual(info['weight'], 1.0)
            if info['address'] == address_0:
                self.assertEqual(info['status'], _server.MySQLServer.PRIMARY)
                self.assertEqual(info['mode'], _server.MySQLServer.READ_WRITE)
            else:
                self.assertEqual(info['status'], _server.MySQLServer.SECONDARY)
                self.assertEqual(info['mode'], _server.MySQLServer.READ_ONLY)

    def test_server_permissions(self):
        """Verify the server user permissions.
        """

        #
        # Skip test in trial-mode. Users are the same a s admin user.
        # Admin won't be able to grant a privilege back to itself.
        #
        if (MySQLInstances().server_user == MySQLInstances().user):
            # The trailing comma prevents a newline.
            print "Skipping test_server_permissions in trial-mode --- ",
            return

        #
        # Prepare group and servers
        #
        self.proxy.group.create("group", "Testing group...")
        address_0 = tests.utils.MySQLInstances().get_address(0)
        address_1 = tests.utils.MySQLInstances().get_address(1)
        address_2 = tests.utils.MySQLInstances().get_address(2)
        status_uuid = self.proxy.server.lookup_uuid(address_0)
        uuid_0 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        uuid_1 = status_uuid[2]
        status_uuid = self.proxy.server.lookup_uuid(address_2)
        uuid_2 = status_uuid[2]
        status = self.proxy.group.add("group", address_0)
        self.check_xmlrpc_command_result(status, False)

        server_0 = fetch_test_server(address_0)
        server_0.connect()

        #
        # Change password of the the server user, remove server from
        # group, try to add server to group, which fails, because the
        # Fabric instance does still use the old password, change
        # password back, add server to group. The remove operation is
        # required, so that fabric purges all connections to this
        # server. Otherwise it would use existing connections, which do
        # not care about a changed password. Only the add server to
        # group command after a remove server from group cpmmand forces
        # Fabric to establish a new connection, which fails on the wrong
        # password.
        #
        server_0.exec_stmt("SET PASSWORD FOR '{user}'@'%' ="
                           " PASSWORD('foobar')".
                           format(user=MySQLInstances().server_user))
        status = self.proxy.group.remove("group", address_0)
        self.check_xmlrpc_command_result(status, False)
        status = self.proxy.group.add("group", address_0)
        self.check_xmlrpc_command_result(status, has_error=True)
        server_0.connect()
        server_0.exec_stmt("SET PASSWORD FOR '{user}'@'%' ="
                          " PASSWORD('{passwd}')".
                          format(user=MySQLInstances().server_user,
                                 passwd=MySQLInstances().server_passwd))
        status = self.proxy.group.add("group", address_0)
        self.check_xmlrpc_command_result(status, False)

        #
        # Revoke the REPLICATION SLAVE privilege from the server user,
        # try a promote, which fails, grant REPLICATION SLAVE back.
        #
        server_0.exec_stmt("REVOKE REPLICATION SLAVE ON *.* FROM '{user}'@'%'".
                           format(user=MySQLInstances().server_user))
        status = self.proxy.group.promote("group")
        self.check_xmlrpc_command_result(status, has_error=True)
        server_0.exec_stmt("GRANT REPLICATION SLAVE ON *.* TO '{user}'@'%'".
                           format(user=MySQLInstances().server_user))

        #
        # Do a successful promote.
        #
        status = self.proxy.group.promote("group")
        self.check_xmlrpc_command_result(status, False)

    def test_update_only(self):
        """Test the update_only parameter while adding a slave.
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)
        address_3 = tests.utils.MySQLInstances().get_address(2)
        user      = tests.utils.MySQLInstances().user
        passwd    = tests.utils.MySQLInstances().passwd

        status = self.proxy.server.lookup_uuid(address_1)
        uuid_1 = self.check_xmlrpc_get_uuid(status, False)
        server_1 = _server.MySQLServer(_uuid.UUID(uuid_1), address_1,
                                       user, passwd)
        server_1.connect()

        status = self.proxy.server.lookup_uuid(address_2)
        uuid_2 = self.check_xmlrpc_get_uuid(status, False)
        server_2 = _server.MySQLServer(_uuid.UUID(uuid_2), address_2,
                                       user, passwd)
        server_2.connect()

        status = self.proxy.server.lookup_uuid(address_3)
        uuid_3 = self.check_xmlrpc_get_uuid(status, False)
        server_3 = _server.MySQLServer(_uuid.UUID(uuid_3), address_3,
                                       user, passwd)
        server_3.connect()

        # Add a server and check that replication is not configured. Since
        # there is no master configured, it does not matter whether the
        # update_only parameter is set or not.
        self.proxy.group.add("group", address_1, 5, True)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.SECONDARY,
            'is_not_configured': True,
        })

        self.proxy.group.remove("group", uuid_1)
        self.proxy.group.add("group", address_1, 5, False)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
            "is_not_configured" : True,
        })

        # Try to make the previous server a master, i.e. --update-only = False.
        status = self.proxy.server.set_status(
            uuid_1, _server.MySQLServer.PRIMARY
        )
        self.check_xmlrpc_command_result(status, True)

        # Try to make the previous server a master, i.e. --update-only = True.
        status = self.proxy.server.set_status(
            uuid_1, _server.MySQLServer.PRIMARY, True
        )
        self.check_xmlrpc_command_result(status, True)
        self.proxy.group.promote("group", uuid_1)

        # Add a slave but notice that it is not properly configured, i.e.
        # --update-only = True.
        self.proxy.group.add("group", address_2, 5, True)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
            "is_not_configured" : True,
        }, rowcount=2, index=1)

        # Properly configure the previous slave.
        _replication.switch_master(slave=server_2, master=server_1,
            master_user=server_1.user, master_passwd=server_1.passwd
        )
        _replication.start_slave(server_2, wait=True)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
        }, rowcount=2, index=1)

        # Add a slave but notice that it is properly configured, i.e.
        # --update-only = False.
        self.proxy.group.add("group", address_3)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SECONDARY,
        }, index=1)

        # Stop replication, set slave's status to faulty and add it
        # back as a spare, --update-only = False. Note that it is
        # properly configured.
        _replication.stop_slave(server_3, wait=True)
        server_3.status = _server.MySQLServer.FAULTY
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            'status': _server.MySQLServer.FAULTY,
            "io_not_running": True,
            "sql_not_running": True,
        }, rowcount=3, index=2)
        status = self.proxy.server.set_status(
            uuid_3, _server.MySQLServer.SPARE
        )
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SPARE,
        }, rowcount=3, index=2)

        # Stop replication, set slave's status to faulty and add it
        # back as a spare, --update-only = True. Note that it is not
        # properly configured.
        _replication.stop_slave(server_3, wait=True)
        server_3.status = _server.MySQLServer.FAULTY
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.FAULTY,
            "io_not_running": True,
            "sql_not_running": True,
        }, rowcount=3, index=2)
        status = self.proxy.server.set_status(
            uuid_3, _server.MySQLServer.SPARE, True
        )
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SPARE,
            "io_not_running": True,
            "sql_not_running": True,
        }, rowcount=3, index=2)

        # Try to set slave's status to faulty, i.e. --update-only = False.
        status = self.proxy.server.set_status(
            uuid_3, _server.MySQLServer.FAULTY
        )
        self.check_xmlrpc_command_result(status, True)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SPARE,
            "io_not_running": True,
            "sql_not_running": True,
        }, rowcount=3, index=2)

        # Try to set slave's status to faulty, i.e. --update-only = True.
        status = self.proxy.server.set_status(
            uuid_3, _server.MySQLServer.FAULTY, True
        )
        self.check_xmlrpc_command_result(status, has_error=True)
        status = self.proxy.group.health("group")
        self.check_xmlrpc_simple(status, {
            "status": _server.MySQLServer.SPARE,
            "io_not_running": True,
            "sql_not_running": True,
        }, rowcount=3, index=2)

    def test_lookup_servers(self):
        """Test searching for servers by calling group.lookup_servers().
        """
        # Prepare group and servers
        self.proxy.group.create("group", "Testing group...")
        address_0 = tests.utils.MySQLInstances().get_address(0)
        address_1 = tests.utils.MySQLInstances().get_address(1)
        address_2 = tests.utils.MySQLInstances().get_address(2)
        status = self.proxy.group.add("group", address_0)
        self.check_xmlrpc_command_result(status, False)
        status = self.proxy.group.add("group", address_1)
        self.check_xmlrpc_command_result(status, False)
        status = self.proxy.group.add("group", address_2)
        self.check_xmlrpc_command_result(status, False)
        status_uuid = self.proxy.server.lookup_uuid(address_1)
        server_1 = _server.MySQLServer.fetch(status_uuid[2])

        # Fetch all servers in a group and check the number of server.
        status = self.proxy.group.lookup_servers("group")
        result = _xmlrpc._decode(status)
        self.assertNotEqual(len(result.results), 0, str(result));
        self.assertEqual(result.results[0].rowcount, 3, str(result))

        #
        # It it not possible to specify only some of the optional
        # parameters. For that reason, this part of the test is
        # skipped as it requires to set the status without setting
        # the uuid parameter.
        # Note this is not possible though as the method has the
        #following signature:
        # lookup_servers(self, group_id, uuid=None, status=None, ...)
        #
        return
        # Fetch all running servers in a group.
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.SECONDARY
            )
        self.assertEqual(len(server[2]), 3)

        # Fetch all offline servers in a group.
        server_1.status = _server.MySQLServer.FAULTY
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.FAULTY
            )
        self.assertEqual(len(server[2]), 1)

        # Fetch all running servers in a group.
        server =  self.proxy.group.lookup_servers(
            "group", _server.MySQLServer.SECONDARY
            )
        self.assertEqual(len(server[2]), 2)

        # Fetch all servers in a group.
        server =  self.proxy.group.lookup_servers("group")
        self.assertEqual(len(server[2]), 3)

        # Try to fetch servers with a non-existing status.
        server =  self.proxy.group.lookup_servers(
            "group", 10
            )
        self.assertEqual(server[0], False)
        self.assertNotEqual(server[1], "")
        self.assertEqual(server[2],  True)

    def test_dump_fabric(self):
        """Test searching for fabric instances by calling
        dump.fabric_nodes().
        """
        from __main__ import xmlrpc_next_port
        status = self.proxy.dump.fabric_nodes()
        self.check_xmlrpc_simple(status, {
            'host': "localhost",
            'port': int(xmlrpc_next_port),
            'host': "localhost",
            'port': int(xmlrpc_next_port) + 1,
        })

if __name__ == "__main__":
    unittest.main()

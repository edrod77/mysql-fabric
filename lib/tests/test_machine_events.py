#
# Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
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
"""Unit tests for managing machines.
"""
import unittest
import uuid as _uuid
import tests.utils

from mysql.fabric.provider import (
    Provider,
)

from mysql.fabric.machine import (
    Machine,
)

from mysql.fabric import (
    executor as _executor,
)

PROVIDER_ID = "provider"
PROVIDER_TYPE = "NULLPROVIDER"
USERNAME = "username"
PASSWORD = "password"
URL = "http://127.0.0.1:5000/v2.0"
TENANT = "tenant"
DEFAULT_IMAGE = "image"
DEFAULT_FLAVOR = "flavor"
IMAGE = ["name=image"]
FLAVOR = ["name=flavor"]

class TestMachineServices(tests.utils.TestCase):
    """Unit tests for managing machines.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        self.proxy.provider.register(
            PROVIDER_ID, USERNAME, PASSWORD, URL, TENANT, PROVIDER_TYPE
        )

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_machine(self):
        """Test creating/destroying a machine
        """
        # Try to create a machine with a wrong provider.
        status = self.proxy.server.create(
            "Doesn't exist", IMAGE, FLAVOR
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Create a machine.
        status = self.proxy.server.create(
            PROVIDER_ID, IMAGE, FLAVOR
        )
        self.check_xmlrpc_command_result(status)
        status = self.proxy.server.list(PROVIDER_ID)
        info = self.check_xmlrpc_simple(status, {})
        machine_uuid = info['uuid']
        av_zone = info['av_zone']
        machine = Machine.fetch(machine_uuid)
        self.assertEqual(str(machine.uuid), machine_uuid)
        self.assertEqual(machine.av_zone, av_zone)

        # Try to remove a machine  with a wrong provider
        status = self.proxy.server.destroy("Don't exist", str(machine.uuid))
        self.check_xmlrpc_command_result(status, has_error=True)

        # Remove a machine.
        status = self.proxy.server.destroy(PROVIDER_ID, machine_uuid)
        self.check_xmlrpc_command_result(status)

        # Try to remove a machine that does not exist.
        status = self.proxy.server.destroy(PROVIDER_ID, machine_uuid)
        self.check_xmlrpc_command_result(status, has_error=True)

    def test_parameters(self):
        """Check if parameters are pre-processed.
        """
        # Try to create a machine  without an image.
        status = self.proxy.server.create(PROVIDER_ID)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine without a flavor.
        status = self.proxy.server.create(PROVIDER_ID, IMAGE)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine with wrong image format.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size"], "flavor"
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine with wrong flavor format.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], "flavor"
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine with wrong machine_numbers.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], ["flavor=flavor"], -1
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine with wrong userdata.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "userdata"
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine with wrong scheduler_hints.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            "scheduler_hints"
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine with wrong meta.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            ["name=scheduler_hints"], ["meta"]
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a machine with reserved meta.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            ["name=scheduler_hints"], ["mysql-fabric=True"]
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Create a machine.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            ["name=scheduler_hints"], ["name=meta"]
        )
        self.check_xmlrpc_command_result(status)

        # TODO: Test other parameters that were included with database.

    def test_skip_store(self):
        """Test the skip_store parameter.
        """
        machine_uuid = "955429e1-2125-478a-869c-3b3ce5549c38"

        # Create a machine with the --skip_store=True.
        status = self.proxy.server.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, True
        )
        self.check_xmlrpc_command_result(status)
        
        # Check if there is any reference to the machine in the state store.
        status = self.proxy.server.list(PROVIDER_ID)
        self.check_xmlrpc_simple(status, {}, rowcount=0)

        # Try to destroy the machine. The operation fails because there is
        # no reference to the machine in the state store. Note that we can
        # provide any UUID as the NULLPROVIDER is being used.
        status = self.proxy.server.destroy(PROVIDER_ID, machine_uuid)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Destroy a machine with the --skip_store=True.
        status = self.proxy.server.destroy(
            PROVIDER_ID, machine_uuid, False, True
        )
        self.check_xmlrpc_command_result(status)

class TestSnapshotServices(tests.utils.TestCase):
    """Unit tests for managing snapshot machines.
    """
    def setUp(self):
        """Configure the existing environment.
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        self.proxy.provider.register(
            PROVIDER_ID, USERNAME, PASSWORD, URL, TENANT, PROVIDER_TYPE,
            DEFAULT_IMAGE, DEFAULT_FLAVOR
        )
        status = self.proxy.server.create(
            PROVIDER_ID, IMAGE, FLAVOR
        )
        self.check_xmlrpc_command_result(status)
        status = self.proxy.server.list(PROVIDER_ID)
        info = self.check_xmlrpc_simple(status, {})
        self.machine_uuid = info['uuid']

    def tearDown(self):
        """Clean up the existing environment.
        """
        tests.utils.cleanup_environment()

    def test_snapshot(self):
        """Test creating/destroying a snapshot machine.
        """
        # Try to create a snapshot with a wrong machine_uuid.
        status = self.proxy.snapshot.create(
            PROVIDER_ID, "Doesn't exist"
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to create a snapshot with a wrong provider.
        status = self.proxy.snapshot.create(
            "Doesn't exist", self.machine_uuid
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Create a snapshot.
        status = self.proxy.snapshot.create(
            PROVIDER_ID, self.machine_uuid
        )
        self.check_xmlrpc_command_result(status)

        # Try to destroy snapshots with a wrong provider.
        status = self.proxy.snapshot.destroy(
            "Doesn't exist", self.machine_uuid
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Destroy snapshots.
        status = self.proxy.snapshot.destroy(
            PROVIDER_ID, self.machine_uuid
        )
        self.check_xmlrpc_command_result(status)

    def test_skip_store(self):
        """Test the skip_store parameter.
        """
        machine_uuid = "955429e1-2125-478a-869c-3b3ce5549c38"

        # Try to create machine's snapshot. The operation fails because
        # there is no reference to the machine in the state store.
        status = self.proxy.snapshot.create(PROVIDER_ID, machine_uuid)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Create machine's snapshot with the --skip_store=True.
        status = self.proxy.snapshot.create(
            PROVIDER_ID, machine_uuid, True, False
        )
        self.check_xmlrpc_command_result(status)

        # Try to destroy machine's snapshot(s). The operation fails because
        # there is no reference to the machine in the state store.
        status = self.proxy.snapshot.destroy(PROVIDER_ID, machine_uuid)
        self.check_xmlrpc_command_result(status, has_error=True)

        # Destroy machine's snapshots with the --skip_store=True.
        status = self.proxy.snapshot.destroy(
            PROVIDER_ID, machine_uuid, True
        )
        self.check_xmlrpc_command_result(status)

if __name__ == "__main__":
    unittest.main()

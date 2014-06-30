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
PROVIDER_TYPE = "FAKE_PROVIDER"
USERNAME = "username"
PASSWORD = "password"
URL = "http://127.0.0.1:5000/v2.0"
TENANT = "tenant"
DEFAULT_IMAGE = "image"
DEFAULT_FLAVOR = "flavor"
IMAGE = ["name=image"]
FLAVOR = ["name=flavor"]

class TestMachineServices(unittest.TestCase):
    """Unit tests for managing machines.
    """
    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

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
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_machine(self):
        """Test creating/destroying a machine
        """
        # Try to create a machine  with a wrong provider.
        status = self.proxy.machine.create(
            "Doesn't exist", IMAGE, FLAVOR
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Create a machine.
        status = self.proxy.machine.create(
            PROVIDER_ID, IMAGE, FLAVOR
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_machine).")
        machine_uuid  = status[2][0]['uuid']
        av_zone  = status[2][0]['av_zone']
        machine = Machine.fetch(machine_uuid)
        self.assertEqual(str(machine.uuid), machine_uuid)
        self.assertEqual(machine.av_zone, av_zone)

        # Try to remove a machine  with a wrong provider
        status = self.proxy.machine.destroy("Don't exist", str(machine.uuid))
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_destroy_machine).")

        # Remove a machine.
        status = self.proxy.machine.destroy(PROVIDER_ID, machine_uuid)
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_destroy_machine).")

        # Try to remove a machine that does not exist.
        status = self.proxy.machine.destroy(PROVIDER_ID, machine_uuid)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_destroy_machine).")

    def test_parameters(self):
        """Check if parameters are pre-processed.
        """
        # Try to create a machine  without an image.
        status = self.proxy.machine.create(PROVIDER_ID)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine without a flavor.
        status = self.proxy.machine.create(PROVIDER_ID, IMAGE)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine with wrong image format.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size"], "flavor"
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine with wrong flavor format.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], "flavor"
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine with wrong machine_numbers.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], ["flavor=flavor"], -1
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine with wrong userdata.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "userdata"
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine with wrong scheduler_hints.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            "scheduler_hints"
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine with wrong meta.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            ["name=scheduler_hints"], ["meta"]
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Try to create a machine with reserved meta.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            ["name=scheduler_hints"], ["mysql-fabric=True"]
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_machine).")

        # Create a machine.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            "availability_zone", "key_name", "security_group",
            "private_network", "public_network", "setup.py", "swap",
            ["name=scheduler_hints"], ["name=meta"]
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_machine).")

    def test_skip_store(self):
        """Test the skip_store parameter.
        """
        # Create a machine with the --skip_store=True.
        status = self.proxy.machine.create(
            PROVIDER_ID, ["name=image", "size=20"], ["name=flavor"], 1,
            None, None, None, None, None, None, None, None, None, True
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_machine).")
        machine_uuid = status[2][0]['uuid']

        # Check if there is any reference to the machine in the state store.
        status = self.proxy.machine.list(PROVIDER_ID)
        self.assertEqual(status[2], [])

        # Try to destroy the machine. The operation fails because there is
        # no reference to the machine in the state store.
        status = self.proxy.machine.destroy(PROVIDER_ID, machine_uuid)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_destroy_machine).")

        # Destroy a machine with the --skip_store=True.
        status = self.proxy.machine.destroy(
            PROVIDER_ID, machine_uuid, False, True
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_destroy_machine).")


class TestSnapshotServices(unittest.TestCase):
    """Unit tests for managing snapshot machines.
    """
    def assertStatus(self, status, expect):
        items = (item['diagnosis'] for item in status[1] if item['diagnosis'])
        self.assertEqual(status[1][-1]["success"], expect, "\n".join(items))

    def setUp(self):
        """Configure the existing environment.
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        self.proxy.provider.register(
            PROVIDER_ID, USERNAME, PASSWORD, URL, TENANT, PROVIDER_TYPE,
            DEFAULT_IMAGE, DEFAULT_FLAVOR
        )
        status = self.proxy.machine.create(
            PROVIDER_ID, IMAGE, FLAVOR
        )
        self.machine_uuid = status[2][0]['uuid']

    def tearDown(self):
        """Clean up the existing environment.
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_snapshot(self):
        """Test creating/destroying a snapshot machine.
        """
        # Try to create a snapshot with a wrong machine_uuid.
        status = self.proxy.snapshot.create(
            PROVIDER_ID, "Doesn't exist"
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_snapshot).")

        # Try to create a snapshot with a wrong provider.
        status = self.proxy.snapshot.create(
            "Doesn't exist", self.machine_uuid
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_snapshot).")

        # Create a snapshot.
        status = self.proxy.snapshot.create(
            PROVIDER_ID, self.machine_uuid
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_snapshot).")

        # Try to destroy snapshots with a wrong provider.
        status = self.proxy.snapshot.destroy(
            "Doesn't exist", self.machine_uuid
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_destroy_snapshot).")

        # Try to destroy snapshots with a wrong provider.
        status = self.proxy.snapshot.create(
            "Doesn't exist", self.machine_uuid
        )
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_snapshot).")

        # Destroy snapshots.
        status = self.proxy.snapshot.destroy(
            PROVIDER_ID, self.machine_uuid
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_destroy_snapshot).")

    def test_skip_store(self):
        """Test the skip_store parameter.
        """
        machine_uuid = "955429e1-2125-478a-869c-3b3ce5549c38"

        # Try to create machine's snapshot. The operation fails because
        # there is no reference to the machine in the state store.
        status = self.proxy.snapshot.create(PROVIDER_ID, machine_uuid)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_snapshot).")

        # Create machine's snapshot with the --skip_store=True.
        status = self.proxy.snapshot.create(
            PROVIDER_ID, machine_uuid, True, False
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_snapshot).")

        # Try to destroy machine's snapshot(s). The operation fails because
        # there is no reference to the machine in the state store.
        status = self.proxy.snapshot.destroy(PROVIDER_ID, machine_uuid)
        self.assertStatus(status, _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_destroy_snapshot).")

        # Destroy machine's snapshots with the --skip_store=True.
        status = self.proxy.snapshot.destroy(
            PROVIDER_ID, machine_uuid, True
        )
        self.assertStatus(status, _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_destroy_snapshot).")

if __name__ == "__main__":
    unittest.main()

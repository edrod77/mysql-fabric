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
"""Unit tests for testing machine.
"""
import re
import unittest
import uuid as _uuid
import tests.utils
import mysql.fabric.errors as _errors
import mysql.fabric.server_utils as _server_utils

from mysql.fabric.provider import (
    Provider,
)

from mysql.fabric.machine import (
    Machine,
)

class TestMachine(unittest.TestCase):
    """Unit test for testing Machine.
    """
    def setUp(self):
        """Configure the existing environment.
        """
        self.provider_id = "provider"
        self.provider_type = "NULLPROVIDER"
        self.username = "username"
        self.password = "password"
        self.url = "http://127.0.0.1:5000/v2.0"
        self.tenant = "tenant"

        self.provider = Provider(
            provider_id=self.provider_id, provider_type=self.provider_type,
            username=self.username, password=self.password, url=self.url,
            tenant=self.tenant
        )
        Provider.add(self.provider)

        self.uuid_1 = _uuid.uuid4()
        self.uuid_2 = _uuid.uuid4()
        self.av_zone = "MySQL Fabric's Zone"

    def tearDown(self):
        """Clean up environment.
        """
        tests.utils.cleanup_environment()

    def test_properties(self):
        """Test setting/reading Machine's properties.
        """
        machine = Machine(
            uuid=self.uuid_1, provider_id=self.provider.provider_id,
            av_zone=self.av_zone
        )

        # Check property uuid.
        self.assertEqual(machine.uuid, self.uuid_1)

        # Check property provider_id.
        self.assertEqual(machine.provider_id, self.provider_id)

        # Check property av_zone.
        self.assertEqual(machine.av_zone, self.av_zone)

    def test_storage(self):
        """Test using Machine's storage.
        """
        src_machine = Machine(
            uuid=self.uuid_1, provider_id=self.provider.provider_id,
            av_zone=self.av_zone
        )
        Machine.add(src_machine)
        dst_machine = Machine.fetch(self.uuid_1)
        self.assertEqual(src_machine, dst_machine)
        self.assertNotEqual(id(src_machine), id(dst_machine))

        Machine.remove(src_machine)
        dst_machine = Machine.fetch(self.uuid_1)
        self.assertEqual(None, dst_machine)

    def test_machines(self):
        """Check machines created within a provider.
        """
        machine_1 = Machine(
            uuid=self.uuid_1, provider_id=self.provider.provider_id,
            av_zone=self.av_zone
        )
        Machine.add(machine_1)

        machine_2 = Machine(
            uuid=self.uuid_2, provider_id=self.provider.provider_id,
            av_zone=self.av_zone
        )
        Machine.add(machine_2)

        ret = [ isinstance(machine, Machine) for machine in \
            Machine.machines(self.provider.provider_id)
        ]
        self.assertEqual(len(ret), 2)
        self.assertTrue(all(ret))

if __name__ == "__main__":
    unittest.main()

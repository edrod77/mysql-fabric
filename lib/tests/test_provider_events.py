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

"""Unit tests for managing providers.
"""
import unittest
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

class TestProviderServices(tests.utils.TestCase):
    """Unit tests for managing providers.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_provider(self):
        """Test registering/unregistring a provider
        """
        # Look up providers.
        status = self.proxy.provider.list()
        self.check_xmlrpc_simple(status, {}, rowcount=0)

        # Register a new provider.
        status = self.proxy.provider.register(
            PROVIDER_ID, USERNAME, PASSWORD, URL, TENANT, PROVIDER_TYPE,
            DEFAULT_IMAGE, DEFAULT_FLAVOR
        )
        self.check_xmlrpc_command_result(status, returns=True)

        # Try to register a provider twice.
        status = self.proxy.provider.register(
            PROVIDER_ID, USERNAME, PASSWORD, URL, TENANT, PROVIDER_TYPE,
            DEFAULT_IMAGE, DEFAULT_FLAVOR
        )
        self.check_xmlrpc_command_result(status, has_error=True)

        # Look up providers.
        status = self.proxy.provider.list()
        self.check_xmlrpc_simple(status, {
            "provider_id" : PROVIDER_ID,
            "type" : PROVIDER_TYPE,
            "username" : USERNAME,
            "url" : URL,
            "tenant" : TENANT,
            "default_image" : DEFAULT_IMAGE,
            "default_flavor" : DEFAULT_FLAVOR}
        )

        # Look up a provider.
        status = self.proxy.provider.list(PROVIDER_ID)
        self.check_xmlrpc_simple(status, {
            "provider_id" : PROVIDER_ID,
            "type" : PROVIDER_TYPE,
            "username" : USERNAME,
            "url" : URL,
            "tenant" : TENANT,
            "default_image" : DEFAULT_IMAGE,
            "default_flavor" : DEFAULT_FLAVOR}
        )

        # Try to look up a provider that does not exist.
        status = self.proxy.provider.list("Doesn't exist")
        self.check_xmlrpc_simple(status, {}, has_error=True)

        # Try to unregister a provider that does not exist.
        status = self.proxy.provider.unregister("Doesn't exist")
        self.check_xmlrpc_command_result(status, has_error=True)

        # Try to unregister a provider where there are associated machines.
        status = self.proxy.machine.create(PROVIDER_ID, IMAGE, FLAVOR)
        self.check_xmlrpc_command_result(status)
        status = self.proxy.machine.list(PROVIDER_ID)
        info = self.check_xmlrpc_simple(status, {})
        machine_uuid = info['uuid']
        status = self.proxy.provider.unregister(PROVIDER_ID)
        self.check_xmlrpc_command_result(status, has_error=True)
        status = self.proxy.machine.destroy(PROVIDER_ID, machine_uuid)
        self.check_xmlrpc_command_result(status)

        # Unregister a provider.
        status = self.proxy.provider.unregister(PROVIDER_ID)
        self.check_xmlrpc_command_result(status)

        # Look up providers.
        status = self.proxy.provider.list()
        self.check_xmlrpc_simple(status, {}, rowcount=0)

if __name__ == "__main__":
    unittest.main()

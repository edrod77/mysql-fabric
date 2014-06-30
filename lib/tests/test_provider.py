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
"""Unit tests for testing Provider.
"""
import unittest
import tests.utils
import mysql.fabric.errors as _errors

from mysql.fabric.provider import (
    Provider,
)

class TestProvider(unittest.TestCase):
    """Unit test for testing Provider.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.provider_id = "provider"
        self.provider_type = "FAKE_PROVIDER"
        self.username = "username"
        self.password = "password"
        self.url = "http://127.0.0.1:5000/v2.0"
        self.tenant = "tenant"
        self.image = "default_image"
        self.flavor = "default_flavor"
        self.av_zone = "MySQL Fabric's Zone"

    def tearDown(self):
        """Clean up environment.
        """
        tests.utils.cleanup_environment()

    def test_properties(self):
        """Test setting/reading Provider's properties.
        """
        provider = Provider(
            provider_id=self.provider_id, provider_type=self.provider_type,
            username=self.username, password=self.password, url=self.url,
            tenant=self.tenant, default_image=self.image,
            default_flavor=self.flavor
        )

        # Check property provider_id.
        self.assertEqual(provider.provider_id, self.provider_id)

        # Check property provider_type.
        self.assertEqual(provider.provider_type, self.provider_type)

        # Check property username.
        self.assertEqual(provider.username, self.username)

        # Check property password.
        self.assertEqual(provider.password, self.password)

        # Check property url.
        self.assertEqual(provider.url, self.url)

        # Check property tenant.
        self.assertEqual(provider.tenant, self.tenant)

        # Check property default_image.
        self.assertEqual(provider.default_image, self.image)

        # Check property default_flavor.
        self.assertEqual(provider.default_flavor, self.flavor)

    def test_storage(self):
        """Test using Provider's storage.
        """
        src_provider = Provider(
            provider_id=self.provider_id, provider_type=self.provider_type,
            username=self.username, password=self.password, url=self.url,
            tenant=self.tenant, default_image=self.image,
            default_flavor=self.flavor
        )
        Provider.add(src_provider)
        dst_provider = Provider.fetch(self.provider_id)
        self.assertEqual(src_provider, dst_provider)
        self.assertNotEqual(id(src_provider), id(dst_provider))

        Provider.remove(src_provider)
        dst_provider = Provider.fetch(self.provider_id)
        self.assertEqual(None, dst_provider)

    def test_library(self):
        """Test whether libraries are installed.
        """
        provider = Provider(
            provider_id=self.provider_id, provider_type=self.provider_type,
            username=self.username, password=self.password, url=self.url,
            tenant=self.tenant, default_image=self.image,
            default_flavor=self.flavor
        )
        Provider.add(provider)
        self.assertNotEqual(None, provider.get_provider_manager())
        Provider.remove(provider)

        self.assertRaises(_errors.ProviderError, Provider,
            provider_id=self.provider_id, provider_type="UNKNOWN_TYPE",
            username=self.username, password=self.password, url=self.url,
            tenant=self.tenant, default_image=self.image,
            default_flavor=self.flavor
        )

    def test_providers(self):
        """Test fetching set of providers.
        """
        provider_1 = Provider(
            provider_id=self.provider_id, provider_type=self.provider_type,
            username=self.username, password=self.password, url=self.url,
            tenant=self.tenant, default_image=self.image,
            default_flavor=self.flavor
        )
        Provider.add(provider_1)

        provider_id = "other.provider"
        provider_2 = Provider(
            provider_id=provider_id, provider_type=self.provider_type,
            username=self.username, password=self.password, url=self.url,
            tenant=self.tenant, default_image=self.image,
            default_flavor=self.flavor
        )
        Provider.add(provider_2)

        ret = [ isinstance(provider, Provider) for provider in \
            Provider.providers()
        ]
        self.assertEqual(len(ret), 2)
        self.assertTrue(all(ret))

if __name__ == "__main__":
    unittest.main()

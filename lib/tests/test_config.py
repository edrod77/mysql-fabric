#
# Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
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

"""Unit tests for the configuration file handling.
"""

import os.path
import unittest
import urlparse
import mysql.fabric.config as _config

from mysql.fabric.options import OptionParser

def _resolve_config(config_file):
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), config_file))

class TestConfig(unittest.TestCase):
    """Unit test for the configuration file handling.
    """
    def test_basic(self):
        "Test reading config file and default values"
        config = _config.Config(_resolve_config('main-1.cfg'), None)

        # Read from main-1.cfg file
        self.assertEqual(config.get('protocol.xmlrpc', 'address'),
                         'my.example.com:8080')

    def test_override(self):
        "Check that configuration parameters can be overridden"
        params = {
            'logging': { 'level': 'INFO' },
            }
        config = _config.Config(_resolve_config('override-1.cfg'), params)

        # Checking overridden default
        self.assertEqual(config.get('logging.syslog', 'address'),
                         'log.example.com:481')

        # Checking overridden option from main-1.cfg
        self.assertEqual(config.get('protocol.xmlrpc', 'address'),
                         'other.example.com:7777')

        # Check overridden options
        self.assertEqual(config.get('logging', 'level'), 'INFO')

    def test_options(self):
        "Test that the option parsing works as expected"

        parser = OptionParser()

        # Test the defaults
        options, _args = parser.parse_args([])
        self.assertEqual(options.config_params, None)

        # Test parsing with options
        options, _args = parser.parse_args([
                '--param', 'logging.level=DEBUG',
                '--param', 'protocol.xmlrpc.address=my.example.com:9999',
                '--config=some.cfg',
                ])
        self.assertEqual(options.config_params, {
                'logging': { 'level': 'DEBUG' },
                'protocol.xmlrpc': { 'address': 'my.example.com:9999' },
                })
        self.assertEqual(options.config_file, "some.cfg")

    def test_file_handler(self):
        "Test file handlers are parsed correctly"

        import mysql.fabric.errors as _errors

        config = _config.Config(_resolve_config('main-1.cfg'), {
                'logging': { 'logdir': '/some/path' },
                })

        from mysql.fabric.services.manage import _create_file_handler

        urls = [
            ('file:fabric.log', '/some/path/fabric.log'),
            ('file:///foo.log', '/foo.log'),
        ]
        for url, expect in urls:
            handler = _create_file_handler(config, urlparse.urlparse(url), delay=1)
            self.assertEqual(handler.baseFilename, expect)

        for url in ['file://mats@example.com/some/foo.log']:
            self.assertRaises(
                _errors.ConfigurationError,
                _create_file_handler, config, urlparse.urlparse(url)
            )

    def test_syslog_handler(self):
        "Test that syslog handlers are parsed correctly"

        import mysql.fabric.errors as _errors

        config = _config.Config(_resolve_config('main-1.cfg'), {
                'logging': { 'logdir': '/some/path' },
                })

        from mysql.fabric.services.manage import _create_syslog_handler

        urls = [
            ('syslog:///dev/log', '/dev/log'),
            ('syslog://example.com', ['example.com', 514]),
            ('syslog://example.com:555', ['example.com', '555']),
        ]
        for url, expect in urls:
            handler = _create_syslog_handler(config, urlparse.urlparse(url))
            self.assertEqual(handler.address, expect)

        for url in ['syslog://example.com/some/foo.log']:
            self.assertRaises(
                _errors.ConfigurationError,
                _create_syslog_handler, config, urlparse.urlparse(url)
            )

if __name__ == "__main__":
    unittest.main()


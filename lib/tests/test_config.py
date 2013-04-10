"""Unit tests for the configuration file handling.
"""

import os.path
import unittest

import mysql.hub.config as _config

from mysql.hub.options import OptionParser

def _resolve_config(config_file):
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), config_file))

class TestConfig(unittest.TestCase):
    """Unit test for the configuration file handling.
    """

    def setUp(self):
        # Reset the site configuration file so that we read from the
        # test directory
        self.__original_site_config = _config.SITE_CONFIG
        _config.SITE_CONFIG = _resolve_config("main-1.cfg")

    def tearDown(self):
        _config.SITE_CONFIG = self.__original_site_config

    def test_basic(self):
        "Test reading config file and default values."
        config = _config.Config(None, None)

        # Checking defaults
        self.assertEqual(config.get('logging', 'level'), 'INFO')
        self.assertEqual(config.get('logging.syslog', 'address'), '/dev/log')

        # Read from main-1.cfg file
        self.assertEqual(config.get('protocol.xmlrpc', 'address'),
                         'my.example.com:8080')

    def test_override(self):
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
        "Test that the option parsing works as expected."

        parser = OptionParser()

        # Test the defaults
        options, _args = parser.parse_args([])
        self.assertEqual(options.config_params, None)
        self.assertEqual(options.config_file, "hub.cfg")
        self.assertEqual(options.ignore_site_config, False)

        # Test parsing with options
        options, _args = parser.parse_args([
                '--param', 'logging.level=DEBUG',
                '--param', 'protocol.xmlrpc.address=my.example.com:9999',
                '--ignore-site-config',
                '--config=some.cfg',
                ])
        self.assertEqual(options.config_params, {
                'logging': { 'level': 'DEBUG' },
                'protocol.xmlrpc': { 'address': 'my.example.com:9999' },
                })
        self.assertEqual(options.config_file, "some.cfg")
        self.assertEqual(options.ignore_site_config, True)

if __name__ == "__main__":
    unittest.main()


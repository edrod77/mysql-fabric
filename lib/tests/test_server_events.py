"""Unit tests for administrative on servers.
"""

import logging
import types
import unittest
import xmlrpclib
import time
import uuid as _uuid

import mysql.hub.config as _config
import mysql.hub.errors as _errors
import mysql.hub.executor as _executor
import mysql.hub.events as _events
import mysql.hub.services.server as _service
import mysql.hub.server as _server
import mysql.hub.persistence as _persistence

import tests.utils as _test_utils

class TestServerServices(unittest.TestCase):
    "Test the service interface"

    def setUp(self):
        params = {
                "protocol.xmlrpc": {
                "address": "localhost:15500"
                },
            }
        config = _config.Config(None, params, True)

        # Set up the manager
        from mysql.hub.commands.start import start
        start(config)

        # Set up the client
        url = "http://%s" % (config.get("protocol.xmlrpc", "address"),)
        self.proxy = xmlrpclib.ServerProxy(url)

        executor = _executor.Executor()
        executor.persister = _persistence.MySQLPersister("localhost:13000",
                                                         "root", "")
        _server.MySQLServer.create(executor.persister)
        _server.Group.create(executor.persister)

    def tearDown(self):
        self.proxy.shutdown()
        executor = _executor.Executor()
        _server.Group.drop(executor.persister)
        _server.MySQLServer.drop(executor.persister)

    def test_group_events(self):
        # Look up groups.
        status = self.proxy.server.lookup_groups()
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(status[2], [])

        # Insert a new group.
        status = self.proxy.server.create_group("group", "Testing group...")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_group).")

        # Try to insert a group twice.
        status = self.proxy.server.create_group("group", "Testing group...")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_group).")

        # Look up groups.
        status = self.proxy.server.lookup_groups()
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_groups).")
        self.assertEqual(status[2], [["group"]])

        # Look up a group.
        status = self.proxy.server.lookup_group("group")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_group).")
        self.assertEqual(status[2], {"group_id": "group", "description":
                                     "Testing group..."})

        # Try to look up a group that does not exist.
        status = self.proxy.server.lookup_group("group_1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_group).")
        self.assertEqual(status[2], "None")

        # Update a group.
        status = self.proxy.server.update_group("group", "Test Test Test")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_update_group).")

        # Try to update group that does not exist.
        status = self.proxy.server.update_group("group_1", "Test Test Test")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_update_group).")

    def test_server_events(self):
        # Insert a new server.
        self.proxy.server.create_group("group_1", "Testing group...")
        status = self.proxy.server.create_server("group_1", "localhost:13000",
                                                 "root", "")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_create_server).")

        # Try to insert a server twice.
        status = self.proxy.server.create_server("group_1", "localhost:13000",
                                                 "root", "")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_server).")

        # Try to insert a server into a non-existing group.
        status = self.proxy.server.create_server("group_2", "localhost:13000",
                                                 "root", "")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_create_server).")

        # Look up servers.
        status_servers = self.proxy.server.lookup_servers("group_1")
        self.assertEqual(status_servers[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_servers[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_servers[1][-1]["description"],
                         "Executed action (_lookup_servers).")
        status_uuid = self.proxy.server.lookup_uuid("localhost:13000", "root", "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")
        self.assertEqual(status_servers[2], [[status_uuid[2]]])

        # Try to look up servers in a group that does not exist.
        status = self.proxy.server.lookup_servers("group_x")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_servers).")
        self.assertEqual(status[2], "None")

        # Look up a server.
        status = self.proxy.server.lookup_server("group_1", status_uuid[2])
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_lookup_server).")
        self.assertEqual(status[2], {"passwd": "", "uri": "localhost:13000",
                                     "user": "root", "uuid": status_uuid[2]})

        # Try to look up a server in a group that does not exist.
        status = self.proxy.server.lookup_server("group_x", status_uuid[2])
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_server).")
        self.assertEqual(status[2], "None")

        # Try to look up a server that does not exist.
        status = self.proxy.server.lookup_server("group_1",
            "cc75b12c-98d1-414c-96af-9e9d4b179678")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_server).")
        self.assertEqual(status[2], "None")

        # Try to look up a server that does not exist
        status = self.proxy.server.lookup_uuid("localhost:15000", "root", "")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_lookup_uuid).")
        self.assertEqual(status[2], "None")

    def test_remove_group_events(self):
        # Prepare group and servers
        self.proxy.server.create_group("group", "Testing group...")
        self.proxy.server.create_group("group_1", "Testing group...")
        self.proxy.server.create_server("group_1", "localhost:13000",
                                        "root", "")

        # Remove a group.
        status = self.proxy.server.remove_group("group")
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_group).")

        # Try to remove a group twice.
        status = self.proxy.server.remove_group("group")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_group).")

        # Try to remove a group where there are servers.
        status = self.proxy.server.remove_group("group_1")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_group).")

    def test_remove_server_events(self):
        # Prepare group and servers
        self.proxy.server.create_group("group", "Testing group...")
        self.proxy.server.create_group("group_1", "Testing group...")
        self.proxy.server.create_server("group_1", "localhost:13000",
                                        "root", "")
        status_uuid = self.proxy.server.lookup_uuid("localhost:13000", "root",
                                                    "")
        self.assertEqual(status_uuid[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status_uuid[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status_uuid[1][-1]["description"],
                         "Executed action (_lookup_uuid).")

        # Try to remove a server from a non-existing group.
        status = self.proxy.server.remove_server("group_2",
            "bb75b12b-98d1-414c-96af-9e9d4b179678")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server with an invalid uuid.
        status = self.proxy.server.remove_server("group_1",
            "bb-98d1-414c-96af-9")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Try to remove a server that does not exist.
        status = self.proxy.server.remove_server("group_1",
            "bb75b12c-98d1-414c-96af-9e9d4b179678")
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Tried to execute action (_remove_server).")

        # Remove a server.
        status = self.proxy.server.remove_server("group_1", status_uuid[2])
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[1][-1]["state"], _executor.Job.COMPLETE)
        self.assertEqual(status[1][-1]["description"],
                         "Executed action (_remove_server).")

if __name__ == "__main__":
    unittest.main(argv=sys.argv)

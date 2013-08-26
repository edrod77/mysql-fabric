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

import unittest
import sys
import re

from cStringIO import StringIO

import mysql.fabric.persistence as _persistence
import mysql.fabric.command as _command
import mysql.fabric.services as _services
import mysql.fabric.protocols.xmlrpc as _xmlrpc
import mysql.fabric.config as _config
import mysql.fabric.events as _events
import mysql.fabric.executor as _executor

import tests.utils

class NewCommand(_command.Command):
    command_options = [
        { 'options': [ '--daemonize'],
          'dest':  'daemonize',
          'default': False,
          'help': "Daemonize the manager"
          },
        ]

    def __init__(self):
        super(NewCommand, self).__init__()
        self.execution = None

    def add_options(self, parser):
        super(NewCommand, self).add_options(parser)
        self.execution = "added_option"

class NewErrorCommand(_command.Command):
    command_options = [
        { 'dest':  'daemonize',
          'default': False,
          'help': "Daemonize the manager"
          },
        ]

    def __init__(self):
        super(NewErrorCommand, self).__init__()
        self.execution = None

    def add_options(self, parser):
        super(NewErrorCommand, self).add_options(parser)
        self.execution = "added_option"

class NewRemoteCommand(_command.Command):
    group_name = "test"
    command_name = "remote_command"

    def __init__(self):
        super(NewRemoteCommand, self).__init__()
        self.execution = None

    def execute(self):
        self.execution = "executed"
        return self.execution

NEW_PROCEDURE_COMMAND_0 = _events.Event()
class ClassCommand_0(_command.ProcedureCommand):
    group_name = "test"
    command_name = "procedure_command_0"

    def execute(self, param, synchronous=True):
        procedures = _events.trigger(
            NEW_PROCEDURE_COMMAND_0, self.get_lockable_objects(), param
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_COMMAND_0)
def _new_command_procedure_0(param):
    pass

NEW_PROCEDURE_COMMAND_1 = _events.Event()
class ClassCommand_1(_command.ProcedureCommand):
    group_name = "test"
    command_name = "procedure_command_1"

    def execute(self, param, synchronous=True):
        procedures = _events.trigger(
            NEW_PROCEDURE_COMMAND_1, self.get_lockable_objects(), param
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_COMMAND_1)
def _new_command_procedure_1(param):
    raise Exception("Error")

NEW_PROCEDURE_GROUP_0 = _events.Event()
class ClassGroup_0(_command.ProcedureGroup):
    group_name = "test"
    command_name = "procedure_group_0"

    def execute(self, group_id, synchronous=True):
        procedures = _events.trigger(
            NEW_PROCEDURE_GROUP_0, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_GROUP_0)
def _new_procedure_group_0(group_id):
    pass

NEW_PROCEDURE_GROUP_1 = _events.Event()
class ClassGroup_1(_command.ProcedureGroup):
    group_name = "test"
    command_name = "procedure_group_1"

    def execute(self, param, synchronous=True):
        procedures = _events.trigger(
            NEW_PROCEDURE_GROUP_1, self.get_lockable_objects(), param
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_GROUP_1)
def _new_procedure_group_1(param):
    pass

NEW_PROCEDURE_SHARD_0 = _events.Event()
class ClassShard_0(_command.ProcedureShard):
    group_name = "test"
    command_name = "procedure_shard_0"

    def execute(self, group_id, synchronous=True):
        lockable = self.get_lockable_objects("group_id")
        assert(lockable == set(['test']))
        procedures = _events.trigger(
            NEW_PROCEDURE_SHARD_0, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_SHARD_0)
def _new_procedure_shard_0(group_id):
    pass

NEW_PROCEDURE_SHARD_1 = _events.Event()
class ClassShard_1(_command.ProcedureShard):
    group_name = "test"
    command_name = "procedure_shard_1"

    def execute(self, table_name, shard_mapping_id, shard_id,
                synchronous=True):
        procedures = _events.trigger(
            NEW_PROCEDURE_SHARD_1, self.get_lockable_objects(),
            table_name, shard_mapping_id, shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_SHARD_1)
def _new_procedure_shard_1(table_name, shard_mapping_id, shard_id):
    pass

class TestCommand(unittest.TestCase):
    "Test command interface."

    def setUp(self):
        """Configure the existing environment
        """
        _command.register_command(
            "test", "procedure_command_0", ClassCommand_0
            )
        _command.register_command(
            "test", "procedure_command_1", ClassCommand_1
            )
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

        _command.unregister_command("test", "procedure_command_0")
        _command.unregister_command("test", "procedure_command_1")

    def test_command(self):
        """Create a command and check its basic properties.
        """
        # Create command.
        from optparse import OptionParser
        cmd = NewCommand()

        # Setup client-side data.
        cmd.setup_client("Fake Client", "Client Options", "Client Config")
        self.assertEqual(cmd.client, "Fake Client")
        self.assertEqual(cmd.options, "Client Options")
        self.assertEqual(cmd.config, "Client Config")
        self.assertRaises(
            AssertionError, cmd.setup_server, None, None, None
            )

        # Setup server-side data.
        cmd.setup_client(None, None, None)
        cmd.setup_server("Fake Server", "Server Options", "Server Config")
        self.assertEqual(cmd.server, "Fake Server")
        self.assertEqual(cmd.options, "Server Options")
        self.assertEqual(cmd.config, "Server Config")
        self.assertRaises(
            AssertionError, cmd.setup_client, None, None, None
            )

        # Add options to possible command-line.
        cmd.add_options(OptionParser())
        self.assertEqual(cmd.execution, "added_option")

    def test_command_error(self):
        """Create a command that has malformed options.
        """
        # Create a command.
        from optparse import OptionParser
        cmd = NewErrorCommand()

        # Try to add malformed options.
        self.assertRaises(KeyError, cmd.add_options, OptionParser())
        self.assertEqual(cmd.execution, None)

        # Try to dispatch a request when client information was not
        # configured.
        self.assertRaises(AttributeError, cmd.dispatch)

    def test_remote_command(self):
        """Create a remote command and fire it.
        """
        # Configure a local command.
        from __main__ import xmlrpc_next_port
        from optparse import OptionParser
        params = {
            'protocol.xmlrpc': {
                'address': 'localhost:%d' % (xmlrpc_next_port, ),
                },
            }
        config = _config.Config(None, params)
        local_cmd = NewRemoteCommand()
        local_cmd.setup_client(_xmlrpc.MyClient(), None, config)

        # Dispatch request through local command to remote command.
        self.assertEqual(local_cmd.dispatch(), "Command :\n{ return = executed\n}")
        self.assertEqual(local_cmd.execution, None)

    def test_procedure_return(self):
        """Check returned values from a procedure.
        """
        check = re.compile('\w{8}(-\w{4}){3}-\w{12}')

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = default).
        status = self.proxy.test.procedure_command_0("test")
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = "True").
        status = self.proxy.test.procedure_command_0("test", True)
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = "TrUe").
        status = self.proxy.test.procedure_command_0("test", "TrUe")
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = 1).
        status = self.proxy.test.procedure_command_0("test", 1)
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

        # Procedure is asynchronously executed and returns only the
        # the procedure's uuid (Synchronous = False).
        status = self.proxy.test.procedure_command_0("test", False)
        self.assertNotEqual(check.match(status), None)

        # Procedure is asynchronously executed and returns only the
        # the procedure's uuid (Synchronous = 0).
        status = self.proxy.test.procedure_command_0("test", 0)
        self.assertNotEqual(check.match(status), None)

        # Procedure is asynchronously executed and returns only the
        # the procedure's uuid (Synchronous = "False").
        status = self.proxy.test.procedure_command_0("test", "False")
        self.assertNotEqual(check.match(status), None)

        # Procedure is synchronously executed and returns the report
        # about execution.
        status = self.proxy.test.procedure_command_0("test", "abc")
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

        # Procedure is synchronously executed but throws an error.
        status = self.proxy.test.procedure_command_1("test", True)
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.ERROR)
        self.assertEqual(status[2], False)

        # Procedure is ansynchronously executed and returns only the
        # procedure's uuid. Note that it eventually throw an error.
        status = self.proxy.test.procedure_command_1("test", False)
        self.assertNotEqual(check.match(status), None)

    def test_procedure_group(self):
        """Check returned values from a procedure
        """
        check = re.compile('\w{8}(-\w{4}){3}-\w{12}')

        # Procedure has argument group_id.
        status = self.proxy.test.procedure_group_0("test")
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

        # Procedure does not have argument group_id.
        status = self.proxy.test.procedure_group_1("test")
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

    def test_procedure_shard(self):
        """Check returned values from a procedure that inherits from
        ProcedureShard.
        """
        check = re.compile('\w{8}(-\w{4}){3}-\w{12}')

        # Procedure has argument group_id.
        status = self.proxy.test.procedure_shard_0("test")
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

        # Procedure has argument table_name, shard_mapping_id and shard_id.
        status = self.proxy.test.procedure_shard_1("test", "test", "test")
        self.assertNotEqual(check.match(status[0]), None)
        self.assertEqual(status[1][-1]["success"], _executor.Job.SUCCESS)
        self.assertEqual(status[2], True)

if __name__ == "__main__":
    unittest.main()

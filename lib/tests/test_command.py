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

import unittest
import re
import tests.utils

import mysql.fabric.command as _command
import mysql.fabric.protocols.xmlrpc as _xmlrpc
import mysql.fabric.config as _config
import mysql.fabric.errors as _errors
import mysql.fabric.events as _events
import mysql.fabric.executor as _executor

from mysql.fabric.utils import (
    FABRIC_UUID,
    TTL,
)

from mysql.fabric.command import (
    CommandResult,
    ResultSet,
    ResultSetColumn,
)

class NewCommand(_command.Command):
    """Emulates a local command that executes with success.
    """
    command_options = [
        { 'options': [ '--daemonize'],
          'dest':  'daemonize',
          'default': False,
          'help': "Daemonize the manager"
          },
        ]

    def __init__(self):
        """Constructor for NewCommand object.
        """
        super(NewCommand, self).__init__()
        self.execution = None

    def add_options(self, parser):
        """Associates options to the command.
        """
        super(NewCommand, self).add_options(parser)
        self.execution = "added_option"

class NewErrorCommand(_command.Command):
    """Emulates a local command that executes with issue.
    """
    command_options = [
        { 'dest':  'daemonize',
          'default': False,
          'help': "Daemonize the manager"
          },
        ]

    def __init__(self):
        """Constructor for NewErrorCommand object.
        """
        super(NewErrorCommand, self).__init__()
        self.execution = None

    def add_options(self, parser):
        """Associates options to the command.
        """
        super(NewErrorCommand, self).add_options(parser)
        self.execution = "added_option"

class NewRemoteCommand(_command.Command):
    """Emulates a remote command that executes with success.
    """
    group_name = "test"
    command_name = "remote_command"

    def __init__(self):
        """Constructor for NewRemoteCommand object.
        """
        super(NewRemoteCommand, self).__init__()
        self.execution = None

    def execute(self):
        """Method that is remotely executed.
        """
        rs = ResultSet(names=["foo"], types=[str])
        rs.append_row(["executed"])
        return CommandResult(None, rs)

class NewErrorRemoteCommand(_command.Command):
    """Emulates a remote command that throws a Fault because of
    returning None.
    """
    group_name = "test"
    command_name = "error_remote_command"

    def execute(self):
        """A remote command that returns None.
        """

        rset = ResultSet(names=['foo'], types=[int])
        rset.append_row([2L**32])
        return CommandResult(None, results=rset)

NEW_PROCEDURE_COMMAND_0 = _events.Event()
class ClassCommand_0(_command.ProcedureCommand):
    """Emulates a remote command that triggers a procedure with success.
    """
    group_name = "test"
    command_name = "procedure_command_0"

    def execute(self, param, synchronous=True):
        """Method that is remotely executed.
        """
        procedures = _events.trigger(
            NEW_PROCEDURE_COMMAND_0, self.get_lockable_objects(), param
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_COMMAND_0)
def _new_command_procedure_0(param):
    """Procedure triggered by ClassCommand_0.
    """
    pass

NEW_PROCEDURE_COMMAND_1 = _events.Event()
class ClassCommand_1(_command.ProcedureCommand):
    """Emulates a remote command that triggers a procedure with error.
    """
    group_name = "test"
    command_name = "procedure_command_1"

    def execute(self, param, synchronous=True):
        """Method that is remotely executed.
        """
        procedures = _events.trigger(
            NEW_PROCEDURE_COMMAND_1, self.get_lockable_objects(), param
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_COMMAND_1)
def _new_command_procedure_1(param):
    """Procedure triggered by ClassCommand_1.
    """
    raise Exception("Error")

NEW_PROCEDURE_GROUP_0 = _events.Event()
class ClassGroup_0(_command.ProcedureGroup):
    """Emulates a remote command that inherits from ProcedureGroup and
    triggers a procedure with success.
    """
    group_name = "test"
    command_name = "procedure_group_0"

    def execute(self, group_id, synchronous=True):
        """Method that is remotely executed.
        """
        procedures = _events.trigger(
            NEW_PROCEDURE_GROUP_0, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_GROUP_0)
def _new_procedure_group_0(group_id):
    """Procedure triggered by ClassGroup_0.
    """
    pass

NEW_PROCEDURE_GROUP_1 = _events.Event()
class ClassGroup_1(_command.ProcedureGroup):
    """Emulates a remote command that inherits from ProcedureGroup and
    triggers a procedure with success.
    """
    group_name = "test"
    command_name = "procedure_group_1"

    def execute(self, param, synchronous=True):
        """Method that is remotely executed.
        """
        procedures = _events.trigger(
            NEW_PROCEDURE_GROUP_1, self.get_lockable_objects(), param
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_GROUP_1)
def _new_procedure_group_1(param):
    """Procedure triggered by ClassGroup_1.
    """
    pass

NEW_PROCEDURE_SHARD_0 = _events.Event()
class ClassShard_0(_command.ProcedureShard):
    """Emulates a remote command that inherits from ProcedureShard and
    triggers a procedure with success.
    """
    group_name = "test"
    command_name = "procedure_shard_0"

    def execute(self, group_id, synchronous=True):
        """Method that is remotely executed.
        """
        procedures = _events.trigger(
            NEW_PROCEDURE_SHARD_0, self.get_lockable_objects(), group_id
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_SHARD_0)
def _new_procedure_shard_0(group_id):
    """Procedure triggered by ClassShard_0.
    """
    pass

NEW_PROCEDURE_SHARD_1 = _events.Event()
class ClassShard_1(_command.ProcedureShard):
    """Emulates a remote command that inherits from ProcedureShard and
    triggers a procedure with success.
    """
    group_name = "test"
    command_name = "procedure_shard_1"

    def execute(self, table_name, shard_mapping_id, shard_id,
                synchronous=True):
        """Method that is remotely executed.
        """
        procedures = _events.trigger(
            NEW_PROCEDURE_SHARD_1, self.get_lockable_objects(),
            table_name, shard_mapping_id, shard_id
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_PROCEDURE_SHARD_1)
def _new_procedure_shard_1(table_name, shard_mapping_id, shard_id):
    """Procedure triggered by ClassShard_0.
    """
    pass

class TestCommand(tests.utils.TestCase):
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

        _command.unregister_command("test", "procedure_command_0")
        _command.unregister_command("test", "procedure_command_1")
            

    def test_command(self):
        """Create a command and check its basic properties.
        """
        # Create command.
        from mysql.fabric.options import OptionParser
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
        params = {
            'protocol.xmlrpc': {
                'address': 'localhost:{0}'.format(xmlrpc_next_port),
                'user': '',
                'password': '',
                },
            }
        config = _config.Config(None, params)
        local_cmd = NewRemoteCommand()
        local_cmd.setup_client(_xmlrpc.MyClient(), None, config)

        # Dispatch request through local command to remote command.
        rs = ResultSet(names=["foo"], types=[str])
        rs.append_row(["executed"])
        self.assertEqual(str(local_cmd.dispatch()),
                         str(CommandResult(None, results=rs)))
        self.assertEqual(local_cmd.execution, None)

    def test_error_remote_command(self):
        """Create a erroneous remote command and fire it.
        """
        # Configure a local command.
        from __main__ import xmlrpc_next_port
        params = {
            'protocol.xmlrpc': {
                'address': 'localhost:{0}'.format(xmlrpc_next_port),
                'user': '',
                'password': '',
                },
            }
        config = _config.Config(None, params)
        local_cmd = NewErrorRemoteCommand()
        local_cmd.setup_client(_xmlrpc.MyClient(), None, config)
        try:
            local_cmd.dispatch()
        except Exception as e:
            self.assertTrue(e, TypeError)
        import xmlrpclib
        self.assertRaises(xmlrpclib.Fault, local_cmd.dispatch)

    def test_procedure_return(self):
        """Check returned values from a procedure.
        """
        check = re.compile('\w{8}(-\w{4}){3}-\w{12}')

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = default).
        packet = self.proxy.test.procedure_command_0("test")
        self.check_xmlrpc_command_result(packet, True)

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = "True").
        packet = self.proxy.test.procedure_command_0("test", True)
        self.check_xmlrpc_command_result(packet, True)

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = "TrUe").
        packet = self.proxy.test.procedure_command_0("test", "TrUe")
        self.check_xmlrpc_command_result(packet, True)

        # Procedure is synchronously executed and returns by default
        # True and report about its execution (Synchronous = 1).
        packet = self.proxy.test.procedure_command_0("test", 1)
        self.check_xmlrpc_command_result(packet, True)

        # Procedure is asynchronously executed and returns only the
        # the procedure's uuid (Synchronous = False).
        packet = self.proxy.test.procedure_command_0("test", False)
        self.check_xmlrpc_command_result(packet, False)

        # Procedure is asynchronously executed and returns only the
        # the procedure's uuid (Synchronous = 0).
        packet = self.proxy.test.procedure_command_0("test", 0)
        self.check_xmlrpc_command_result(packet, False)

        # Procedure is asynchronously executed and returns only the
        # the procedure's uuid (Synchronous = "False").
        packet = self.proxy.test.procedure_command_0("test", "False")
        self.check_xmlrpc_command_result(packet, False)

        # Procedure is synchronously executed and returns the report
        # about execution.
        packet = self.proxy.test.procedure_command_0("test", "abc")
        self.check_xmlrpc_command_result(packet, True)

        # Procedure is synchronously executed but throws an error.
        packet = self.proxy.test.procedure_command_1("test", True)
        self.check_xmlrpc_command_result(packet, True, True)

        # Procedure is ansynchronously executed and returns only the
        # procedure's uuid. Note that it eventually throw an error.
        packet = self.proxy.test.procedure_command_1("test", False)
        self.check_xmlrpc_command_result(packet, False)

    def test_procedure_group(self):
        """Check returned values from a procedure
        """
        check = re.compile('\w{8}(-\w{4}){3}-\w{12}')

        # Procedure has argument group_id.
        packet = self.proxy.test.procedure_group_0("test")
        self.check_xmlrpc_command_result(packet, True)

        # Procedure does not have argument group_id.
        packet = self.proxy.test.procedure_group_1("test")
        self.check_xmlrpc_command_result(packet, True)

    def test_procedure_shard(self):
        """Check returned values from a procedure that inherits from
        ProcedureShard.
        """
        check = re.compile('\w{8}(-\w{4}){3}-\w{12}')

        # Procedure has argument group_id.
        packet = self.proxy.test.procedure_shard_0("test")
        self.check_xmlrpc_command_result(packet, True)

        # Procedure has argument table_name, shard_mapping_id and shard_id.
        packet = self.proxy.test.procedure_shard_1("test", "test", "test")
        self.check_xmlrpc_command_result(packet, True)


class TestResultSet(unittest.TestCase):
    "Test result set."

    def setUp(self):
        # Create a simplistic result set
        names = ["foo", "bar"]
        types = [  int, float]
        self.result = ResultSet(names=names, types=types)
        self.names = names
        self.types = types
        
    def test_definition(self):
        "Check that the types and number of columns are correct."
        self.assertEqual(len(self.result.columns), 2)
        for no, name in enumerate(self.names):
            self.assertEqual(self.result.columns[no].name, name)
        for no, typ in enumerate(self.types):
            self.assertEqual(self.result.columns[no].type, typ)

    def test_append_row(self):
        "Check append_row function"

        self.assertEqual(self.result.rowcount, 0)
        self.result.append_row((1, 2))
        self.assertEqual(self.result.rowcount, 1)
        self.result.append_row([5.0, 3.2])
        self.assertEqual(self.result.rowcount, 2)
        self.result.append_row(['50', '2.25'])

        # Test that we can iterate the rows and get the expected
        # rows. Also check that the value returned matches the
        # expected type.
        expected = [
            (1, 2.0), # Second element in tuple converted to float above
            (5, 3.2), # First element in tuple converted to int above
            (50, 2.25), # Both elements converted from string
        ]
        for row, ref in zip(self.result, expected):
            self.assertEqual(row, ref)
            for no, col in enumerate(row):
                # assertIsInstance is in Python 2.7, but not Python 2.6
                self.assertTrue(
                    isinstance(col, self.result.columns[no].type),
                    "Expected type '%s', was '%s'" % (
                        self.result.columns[no].type.__name__, type(col).__name__
                    )
                )

    def test_indexing(self):
        "Check indexing the result set."

        # Just for precaution, the result set should be empty at start.
        self.assertEqual(self.result.rowcount, 0)

        # This should fail and raise an exception
        self.assertRaises(IndexError, (lambda x: self.result[x]), 0)

        # Check that indexing works and return the row added.
        self.result.append_row((1, 2.0))
        self.assertEqual(self.result[0], (1, 2.0))

    def test_failures(self):
        "Test that the result set class throw errors at the right times."
        self.assertRaises(_errors.CommandResultError,
                          self.result.append_row, [1, 2.0, 5])
        self.assertRaises(_errors.CommandResultError,
                          self.result.append_row, [1])
        self.assertRaises(_errors.CommandResultError,
                          self.result.append_row, [])
        self.assertRaises(ValueError, self.result.append_row, [1, 'ERROR'])
        self.assertRaises(ValueError, self.result.append_row, ['ERROR', 2.0])
        

class TestCommandResult(unittest.TestCase):
    "Test command result"

    def setUp(self):
        self.names = ["foo", "bar"]
        self.types = [  int, float]
        self.rset = ResultSet(names=self.names, types=self.types)
        self.rset.append_row((1, 2.0))
        self.rset.append_row((2, 4.0))

    def test_basic(self):
        result = CommandResult(None)
        self.assertEqual(result.error, None)
        self.assertEqual(len(result.results), 0)

        result.append_result(self.rset)
        self.assertEqual(len(result.results), 1)

        # Check that indexing works and return the result set added.
        self.assertEqual(result.results[0], self.rset)

        # Check that passing something that is not a result set will
        # raise an error.
        self.assertRaises(_errors.CommandResultError, result.append_result, [])

        result = CommandResult("Not working")
        self.assertEqual(result.error, "Not working")

        self.assertRaises(_errors.CommandResultError, result.append_result, self.rset)


    def test_xmlrpc_execute(self):
        "Test XML-RPC encoding and decoding functions."
        cmd = _xmlrpc._CommandExecuteAndEncode(NewRemoteCommand())
        result1 = CommandResult(None)
        result1.append_result(self.rset)
        packet = cmd()
        self.assertEqual(packet, [
            _xmlrpc.FORMAT_VERSION,
            str(FABRIC_UUID), 
            TTL,
            '',                 # No error
            [                   # One result set with one row
                {
                    'info': {
                        'names': ['foo']
                    },
                    'rows': [
                        ("executed",),
                    ]
                }
            ]
        ])

        result2 = _xmlrpc._decode(packet)

    def test_xmlrpc_encoding(self):
        "Test the XML-RPC encoder and decoder."

        results = [
            CommandResult(None),
        ]
        
        for result in results:
            self.assertEqual(str(result),
                             str(_xmlrpc._decode(_xmlrpc._encode(result))))

if __name__ == "__main__":
    unittest.main()

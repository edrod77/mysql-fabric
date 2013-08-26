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

from cStringIO import StringIO

from mysql.fabric import (
    command as _command,
    persistence as _persistence,
)

import mysql.fabric.services.manage as _manage

import tests.utils

some_commands = (
    "test non_doc_command_1\n"
    "test non_dot_command_1 New local command whose doc does not "
        "contain a period mark\n"
    "test dot_command_1     New local command whose doc contains "
        "a period mark.\n"
    "test command_1         New local command that does not "
        "require argument(s).\n"
    "test command_2         New local command that "
        "requires argument(s).\n"
    "test remote_command_1  New remote command that does not "
        "require argument(s).\n"
    "test remote_command_2  New remote command that "
        "requires argument(s).\n"
    )

result_help = (
    "test non_doc_command_1(): \n"
    "test non_dot_command_1(): New local command whose doc does not "
        "contain a\nperiod mark\n\n"
    "test dot_command_1(): New local command whose doc contains "
        "a\nperiod mark.\n\n"
    "test command_1(): New local command that does not "
        "require argument(s).\n\n"
    "Detailed information on how to use the command.\n\n"
    "test command_2(arg_1, arg_2): New local command that "
        "requires argument(s).\n\n"
    "Detailed information on how to use the command.\n\n"
    "test remote_command_1(): New remote command that does not "
        "require argument(s).\n\n"
    "Detailed information on how to use the command.\n\n"
    "test remote_command_2(arg_1, arg_2): New remote command that "
        "requires argument(s).\n\n"
    "Detailed information on how to use the command.\n\n"
    "Command (manage, unknown) was not found.\n"
    )

class NonDocCommand(_command.Command):
    group_name = "test"
    command_name = "non_doc_command_1"

class NonDotCommand(_command.Command):
    """New local command whose doc does not contain a
    period mark
    """
    group_name = "test"
    command_name = "non_dot_command_1"

class DotCommand(_command.Command):
    """New local command whose doc contains a
    period mark.
    """
    group_name = "test"
    command_name = "dot_command_1"

class NewLocalCommandWithoutArgs(_command.Command):
    """New local command that does not require argument(s).

    Detailed information on how to use the command.
    """
    group_name = "test"
    command_name = "command_1"

    def dispatch(self):
        pass

class NewLocalCommandWithArgs(_command.Command):
    """New local command that requires argument(s).

    Detailed information on how to use the command.
    """
    group_name = "test"
    command_name = "command_2"

    def dispatch(self, arg_1, arg_2):
        pass

class NewRemoteCommandWithoutArgs(_command.Command):
    """New remote command that does not require argument(s).

    Detailed information on how to use the command.
    """
    group_name = "test"
    command_name = "remote_command_1"

    def execute(self):
        pass

class NewRemoteCommandWithArgs(_command.Command):
    """New remote command that requires argument(s).

    Detailed information on how to use the command.
    """
    group_name = "test"
    command_name = "remote_command_1"

    def execute(self, arg_1, arg_2):
        pass

class TestManageServices(unittest.TestCase):
    """Test manage services.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.old_stderr = sys.stderr
        sys.stderr = StringIO()
        _command.register_command(
            "test", "non_doc_command_1", NonDocCommand
            )
        _command.register_command(
            "test", "non_dot_command_1", NonDotCommand
            )
        _command.register_command(
            "test", "dot_command_1", DotCommand
            )
        _command.register_command(
            "test", "command_1", NewLocalCommandWithoutArgs
            )
        _command.register_command(
            "test", "command_2", NewLocalCommandWithArgs
            )
        _command.register_command(
            "test", "remote_command_1", NewRemoteCommandWithoutArgs
            )
        _command.register_command(
            "test", "remote_command_2", NewRemoteCommandWithArgs
            )
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        sys.stderr = self.old_stderr
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

        _command.unregister_command("test", "non_doc_command_1")
        _command.unregister_command("test", "non_dot_command_1")
        _command.unregister_command("test", "dot_command_1")
        _command.unregister_command("test", "command_1")
        _command.unregister_command("test", "command_2")
        _command.unregister_command("test", "remote_command_1")
        _command.unregister_command("test", "remote_command_2")

    def test_list_commands(self):
        """Execute list-commands command.
        """
        _manage.List().dispatch()
        commands = sys.stderr.getvalue().replace(" ", "")
        for command in some_commands.replace(" ", "").split("\n"):
            self.assertTrue(command in commands)

    def test_help(self):
        """Execute help command.
        """
        _manage.Help().dispatch("test", "non_doc_command_1")
        _manage.Help().dispatch("test", "non_dot_command_1")
        _manage.Help().dispatch("test", "dot_command_1")
        _manage.Help().dispatch("test", "command_1")
        _manage.Help().dispatch("test", "command_2")
        _manage.Help().dispatch("test", "remote_command_1")
        _manage.Help().dispatch("test", "remote_command_2")
        _manage.Help().dispatch("manage", "unknown")
        self.assertEqual(sys.stderr.getvalue(), result_help)

if __name__ == "__main__":
    unittest.main()

#!/usr/bin/python
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

import sys
import os

from mysql.fabric.services import (
    find_commands,
    find_client,
    )

from mysql.fabric.command import (
    get_groups,
    get_commands,
    get_command,
    )

from mysql.fabric import (
    __version__,
)

from mysql.fabric.options import (
    OptionParser,
)

from mysql.fabric.config import (
    Config,
)

from mysql.fabric.errors import (
    Error, ConfigurationError,
)

# TODO: BOTH xxx-yyy and xxx_yyy should be possible.
HELP_COMMANDS = ("help", "list-commands")

PARSER = OptionParser(
    usage="Usage: %fabric <group> <cmd> [<option> ...] arg ...",
    version=__version__
    )

def check_connector():
    """Check if the connector is properly configured.
    """
    # TODO: CHECK IF THIS SHOULD BE DONE IN RUNTIME OR SETUP.
    try:
        import mysql.connector
    except Exception as error:
        import mysql
        path = os.path.dirname(mysql.__file__)
        print "Tried to look for mysql.connector at (%s)" % (path, )
        print "Error:", error
        raise ConfigurationError("Connector not installed.")


def extract_command():
    """Extract group and command.
    """
    group_name = None
    command_name = None

    if len(sys.argv) > 1:
        # Identify group and command.
        if sys.argv[1] in HELP_COMMANDS:
            group_name = "manage"
            command_name = sys.argv.pop(1)
        elif len(sys.argv) > 2:
            group_name = sys.argv.pop(1)
            command_name = sys.argv.pop(1)
        else:
            PARSER.error("Wrong syntax.")
    else:
        PARSER.error("No command was defined.")

    return group_name, command_name


def create_command(group_name, command_name):
    """Create command object.
    """
    options = None
    config = None

    try:
        # Load information on available commands.
        find_commands()

        # Fetch command class and create the command instance.
        command = get_command(group_name, command_name)()

        # Set up options for command
        command.add_options(PARSER)

        # Parse arguments
        options, args = PARSER.parse_args()

        # If no configuration file was provided, figure out the
        # location of it based on the installed location of the
        # script location.
        if not options.config_file:
            directory = os.path.dirname(__file__)
            prefix = os.path.realpath(os.path.join(directory, '..'))
            if os.name == 'posix' and prefix in ('/', '/usr'):
                config_file = '/etc/mysql/fabric.cfg'
            else:
                config_file = os.path.join(prefix, 'etc', 'mysql', 'fabric.cfg')
            options.config_file = os.path.normpath(config_file)

        # Read configuration file
        if command_name not in HELP_COMMANDS:
            config = Config(options.config_file, options.config_params)

        # Create a protocol client for dispatching the command and set
        # up the client-side information for the command. Inside a
        # shell, this only have to be done once, but here we need to
        # set up the client-side of the command each time we call the
        # program.
        client = find_client()
        command.setup_client(client, options, config)
        return command, args
    except KeyError:
        PARSER.error(
            "Command (%s %s) was not found." % (group_name, command_name, )
            )


def fire_command(command, *args):
    """Fire a command.

    :param arg: Arguments used by the command.
    """
    try:
        # Execute command by dispatching it on the client side.
        # TODO: IMPROVE HOW RESULTS ARE PRESENTED.
        result = command.dispatch(*args)
        if result is not None:
            print result
    except TypeError:
        PARSER.error(
            "Wrong number of parameters were provided for command "
            "(%s %s)." % (command.group_name, command.command_name, )
            )


if __name__ == '__main__':
    try:
        # Check if the python connector is installed.
        check_connector()

        # Parse parameters.
        group_name, command_name  = extract_command()

        # Create command.
        command, args = create_command(group_name, command_name)

        # Fire command.
        fire_command(command, *args)
    except Error as error:
        # TODO: IMPROVE HOW ERRORS ARE PRESENTED.
        print error

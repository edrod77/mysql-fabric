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
import inspect
import textwrap

from mysql.fabric.services import (
    find_commands,
    find_client,
    )

from mysql.fabric.command import (
    Command,
    get_groups,
    get_command,
    get_commands,
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
    ConfigurationError,
)

_ERR_COMMAND_MISSING = "command '%s' in group '%s' was not found."
_ERR_GROUP_MISSING = "group '%s' does not exist"
_ERR_EXTRA_ARGS = "Too many arguments to '%s'"

PARSER = OptionParser(
    usage="Usage: %prog <grp> <cmd> [<option> ...] arg ...",
    version="%prog " + __version__,
    description="MySQL Fabric %s - MySQL server farm management framework" % (
        __version__,
    ),
)

def help_command(group_name, command_name):
    """Print help on a command or group and exit.

    This will print help on a command, or an error if the command does
    not exist, and then exit with exit code 2.

    """

    try:
        # Get the command and information on its parameters.
        cargs = None
        cls = get_command(group_name, command_name)
        command_text = cls.get_signature()
        paragraphs = []
        if cls.__doc__:
            wrapper = textwrap.TextWrapper()
            paragraphs = [
                wrapper.fill(para.strip()) for para in cls.__doc__.split("\n\n") if para
            ]
        print command_text, "\n\n", "\n\n".join(paragraphs)
    except KeyError:
        msg =  _ERR_COMMAND_MISSING % (command_name, group_name)
        PARSER.print_error(msg)

    PARSER.exit(2)

def help_group(group_name):
    """Print help on a command group and exit.

    This will print a list of the commands available in the group, or
    an error message if the group is not available, and then exit with
    exit code 2.

    """

    try:
        lines = [
            "",
            "Commands available in group '%s' are:" % (group_name,),
        ]
        for command_name in get_commands(group_name):
            lines.append("    " + get_command(group_name, command_name).get_signature())
        print "\n".join(lines)
    except KeyError:
        PARSER.print_error(_ERR_GROUP_MISSING % (group_name,))

    PARSER.exit(2)

def check_connector():
    """Check if the connector is properly configured.
    """
    try:
        import mysql.connector
    except Exception as error:
        import mysql
        path = os.path.dirname(mysql.__file__)
        raise ConfigurationError("Tried to look for mysql.connector "
            "at (%s). Connector not installed. Error (%s)." % (path, error)
            )

def show_groups():
    """List groups that have been registered.

    This function list all groups that have been used anywhere when
    registering commands.

    """

    print "Available groups:", ", ".join(group for group in get_groups())

def show_commands():
    """List the possible commands and their descriptions.

    """

    commands = []
    max_name_size = 0

    for group_name in get_groups():
        for command_name in get_commands(group_name):
            cls = get_command(group_name, command_name)

            doc_text = ""
            if cls.__doc__ and cls.__doc__.find(".") != -1:
                doc_text = cls.__doc__[0 : cls.__doc__.find(".") + 1]
            elif cls.__doc__:
                doc_text = cls.__doc__
            doc_text = [text.strip(" ") for text in doc_text.split("\n")]

            commands.append(
                (group_name, command_name, " ".join(doc_text))
                )

            name_size = len(group_name) + len(command_name)
            if name_size > max_name_size:
                max_name_size = name_size

    # Format each description and print the result.
    wrapper = textwrap.TextWrapper(subsequent_indent=(" " * (max_name_size + 3)))
    for group_name, command_name, help_text in commands:
        padding_size = max_name_size - len(group_name) - len(command_name)
        padding_size = 0 if padding_size < 0 else padding_size
        padding_text = "".rjust(padding_size, " ")
        help_text = wrapper.fill(help_text)
        text = (group_name, command_name, padding_text, help_text)
        print " ".join(text)

def show_help():
    """Show help on help

    """

    PARSER.print_help()

HELP_TOPIC = {
    'commands': (show_commands, 'List available commands'),
    'groups': (show_groups, 'List available groups'),
    'help': (show_help, 'Show help'),
}


def extract_command(args):
    """Extract group and command.

    If not both a group and command is provided, a usage message will
    be printed and the process will exit.

    """

    if len(args) < 2 or args[0] == "help":
        if len(args) == 0 or len(args) == 1 and args[0] == "help":
            PARSER.print_help()
            PARSER.exit(2)

        if args[0] == "help":
            if args[1] in HELP_TOPIC:   # Print help topic
                func, _ = HELP_TOPIC[args[1]]
                func()
                PARSER.exit(2)
            elif len(args) == 2:        # Print group help
                args.pop(0)
                help_group(args[0])
            elif len(args) == 3:        # Print command help
                args.pop(0)
                help_command(args[0], args[1])
            else:                       # Too many arguments
                PARSER.print_error(_ERR_EXTRA_ARGS % 'help')
                PARSER.print_help()
                PARSER.exit(2)

        assert len(args) == 1
        help_group(args[0])

    return args[0], args[1], args[2:]


def create_command(group_name, command_name, options, args):
    """Create command object.
    """
    options = None
    config = None

    try:
        # Fetch command class and create the command instance.
        command = get_command(group_name, command_name)()

        # Set up options for command
        command.add_options(PARSER)

        # Parse arguments
        options, args = PARSER.parse_args(args, options)

        # If no configuration file was provided, figure out the
        # location of it based on the installed location of the
        # script location.
        if not options.config_file:
            try:
                directory = os.path.dirname(__file__)
            except NameError:
                directory = os.path.abspath(inspect.getfile(inspect.currentframe()))
            prefix = os.path.realpath(os.path.join(directory, '..'))
            if os.name == 'posix' and prefix in ('/', '/usr'):
                config_file = '/etc/mysql/fabric.cfg'
            else:
                config_file = os.path.join(prefix, 'etc', 'mysql', 'fabric.cfg')
            options.config_file = os.path.normpath(config_file)

        # Read configuration file
        config = Config(options.config_file, options.config_params)

        # Create a protocol client for dispatching the command and set
        # up the client-side information for the command. Inside a
        # shell, this only have to be done once, but here we need to
        # set up the client-side of the command each time we call the
        # program.
        client = find_client()
        command.setup_client(client, options, config)
        return command, args
    except KeyError as error:
        PARSER.error(
            "Error (%s). Command (%s %s) was not found." %
            (error, group_name, command_name, )
            )


def fire_command(command, *args):
    """Fire a command.

    :param arg: Arguments used by the command.
    """
    try:
        # Execute command by dispatching it on the client side. Append the
        #optional arguments passed by the user to the argument list.
        result = command.dispatch(*(command.append_options_to_args(args)))
        if result is not None:
            print result
    except TypeError as error:
        PARSER.error(
            "Wrong number of parameters were provided for command '%s %s'." % (
                command.group_name, command.command_name,
            )
        )


if __name__ == '__main__':
    try:
        # Check if the python connector is installed.
        check_connector()

        # Load information on available commands.
        find_commands()

        # Identify exceptions to normal calling conventions, basically
        # for printing various forms of help.

        # This require us to first fetch all options before the
        # (potential) group and command using the default option
        # parser.
        PARSER.disable_interspersed_args()
        options, args = PARSER.parse_args()
        PARSER.enable_interspersed_args()

        # Options with side effects, such as --help and --version,
        # will never come here, so after this all option arguments are
        # eliminated and the first word in "args" is the the name of a
        # command group.

        # At this point, we (should) have at least one non-option word
        # in the arguments, so we try to get the group and the command
        # for the group. This might fail, if just a group is provided,
        # but then the function will print an apropriate message and
        # exit.
        group_name, command_name, args  = extract_command(args)

        # Here we fetch the command, add the options for the command,
        # run the option parser again, and fetch the command class.
        cmd, cargs = create_command(group_name, command_name, options, args)
        fire_command(cmd, *cargs)
    except Exception as error:
        print error

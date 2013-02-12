"""Module for supporting definition of Fabric commands.

This module aids in the definition of new commands to be incorporated
into the Fabric. Commands are defined as subclasses of the
:class:`Command` class and are automatically incorporated into the
client or server.

Commands have a *remote* and a *local* part, where the remote part is
executed by sending a request to the Fabric server for execution. The
local part of the command is executed by the client.

The default implementation of the local part just dispatch the command
to the server, so these commands do not need to define a local part at
all.

The documentation string of the command class is used as help text for
the command and can be shown using the "help" command. The first
sentence of the description is the brief description and is shown in
listings, while the remaining text is the more elaborate description
shown in command help message.
"""
_COMMANDS_CLASS = {}

def register_command(group_name, command_name, command):
    """Register a command within a group.
    """
    commands = _COMMANDS_CLASS.setdefault(group_name, {})
    commands[command_name] = command

def get_groups():
    """Return registered groups of commands.
    """
    return _COMMANDS_CLASS.keys()

def get_commands(group_name):
    """Return registered commands within a group.
    """
    return _COMMANDS_CLASS[group_name].keys()

def get_command(group_name, command_name):
    """Return a registered a command within a group.
    """
    return _COMMANDS_CLASS[group_name][command_name]

class Command(object):
    """Base class for all commands.

    Each subclass implement both the server side and the client side
    of a command.

    When defining a command, implementing the execute method will
    allow execution on the server. If there is anything that needs to
    be done locally, before dispatching the command, it should be
    added to the dispatch method.

    Command instances automatically get a few attributes defined when
    being created. These can be accessed as normal attributes inside
    the command.

    On the client side, the following attributes are defined:

    options
       Any options provided to the command.
    config
       Any information provided through a configuration file.
    client
       A protocol client instance, which can be used to communicate
       with the server. This is normally not necessary, but can be
       used to get access to configuration file information.

    On the server side, the following attributes are defined:

    server
      The protocol server instance the command is set up for. The
      configuration file information can be accessed through this.

    Commands are organized into groups through the *group_name* class
    property. If it is not defined though, the module where the command
    is defined is used as the group name. Something similar happens to
    the command name, which means that if the *command_name* class
    property is not defined, the class name is automatically used.
    """
    def __init__(self):
        self.__client = None
        self.__server = None
        self.__options = None
        self.__config = None

    @property
    def client(self):
        """Return the client proxy.
        """
        return self.__client

    @property
    def server(self):
        """Return the server proxy.
        """
        return self.__server

    @property
    def options(self):
        """Return command line options.
        """
        return self.__options

    @property
    def config(self):
        """Return configuration options.
        """
        return self.__config

    def setup_client(self, client, options, config):
        """Provide client-side information to the command.

        This is called after an instance of the command have been
        created on the client side and provide the client instance and
        options to the command.

        The client instance can be used to dispatch the command to the
        server.
        """
        assert self.__server is None
        self.__client = client
        self.__options = options
        self.__config = config

    def setup_server(self, server):
        """Provide server-side information to the command.

        This function is called after creating an instance of the
        command on the server-side and will set the server of the
        command. There will be one command instance for each protocol
        server available.

        :param server: Protocol server instance for the command.
        """
        assert self.__client is None
        assert self.__options is None and self.__config is None
        self.__server = server

    def add_options(self, parser):
        "Method called to set up uptions from the class instance."
        try:
            for option in self.command_options:
                kwargs = option.copy()
                del kwargs['options']
                parser.add_option(*option['options'], **kwargs)
        except AttributeError:
            pass

    def dispatch(self, *args):
        """Default dispatch method, executed on the client side.

        The default dispatch method just call the server-side of the
        command.
        """
        return self.client.dispatch(self, *args)

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
import mysql.hub.executor as _executor

_COMMANDS_CLASS = {}

def register_command(group_name, command_name, command):
    """Register a command within a group.

    :param group_name: The command-group to which a command belongs.
    :param command_name: The command that needs to be registered.
    :param command: The command class that contains the implementation
                    for this command
    """
    commands = _COMMANDS_CLASS.setdefault(group_name, {})
    commands[command_name] = command

def unregister_command(group_name, command_name):
    """Unregister a command within a group.

    :param group_name: The command-group to which a command belongs.
    :param command_name: The command that needs to be registered.
    """
    del _COMMANDS_CLASS[group_name][command_name]
    if not _COMMANDS_CLASS[group_name]:
        del _COMMANDS_CLASS[group_name]

def get_groups():
    """Return registered groups of commands.

    :return: Returns the different command groups.
    """
    return _COMMANDS_CLASS.keys()

def get_commands(group_name):
    """Return registered commands within a group.

    :param group_name: The command group whose commands need to be listed.
    :return: The command classes that handles the command functionality.
    """
    return _COMMANDS_CLASS[group_name].keys()

def get_command(group_name, command_name):
    """Return a registered command within a group.

    :param group_name: The command group whose commands need to be listed.
    :param command_name: The command whose implementation needs to be fetched.
    :return: The command classes that handles the command functionality.
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
    command_options = []

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

        :param client: The client instance for the command.
        :param options: The options for the command.
        :param config: The configuration for the command.
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
        """Method called to set up options from the class instance.

        :param parser: The parser used for parsing the command options.
        """
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

        :param args: The arguments for the command dispatch.
        """
        status = self.client.dispatch(self, *args)
        return self.command_status(status)

    @staticmethod
    def command_status(status):
        """Present the result reported by a command in a friendly-user way.

        :param status: The command status.
        """
        string = [
            "Command :",
            "{ return = %s",
            "}"
            ]
        result = "\n".join(string)
        return result % (status, )


class ProcedureCommand(Command):
    # TODO: IMPROVE THE CODE SO USERS MAY DECIDE NOT TO USE WAIT_FOR_PROCEDURES.
    """Class used to implement commands that are built as procedures and
    schedule job(s) to be executed. Any command that needs to access the
    state store must be built upon this class.

    A procedure is asynchronously executed and schedules one or more jobs
    (i.e. functions) that are eventually processed. The scheduling is done
    through the executor which enqueues them and serializes their execution
    within a Fabric Server.

    Any job object encapsulates a function to be executed, its parameters,
    its execution's status and its result. Due to its asynchronous nature,
    a job accesses a snapshot produced by previously executed functions
    which are atomically processed so that Fabric is never left in an
    inconsistent state after a failure.

    To make it easy to use these commands, one might hide the asynchronous
    behavior by exploiting the :meth:`wait_for_procedures`.
    """
    def __init__(self):
        """Create the ProcedureCommand object.
        """
        super(ProcedureCommand, self).__init__()

    def dispatch(self, *args):
        """Default dispatch method when the command is build as a
        procedure.

        It calls command.dispatch, gets the result and processes
        it generating a user-friendly result.

        :param args: The arguments for the command dispatch.
        """
        status = self.client.dispatch(self, *args)
        return self.procedure_status(status)

    @staticmethod
    def wait_for_procedures(procedure_param, synchronous):
        """Wait until a procedure completes its execution and return
        detailed information on it.

        However, if the parameter synchronous is not set, only the
        procedure's uuid is returned because it is not safe to access
        the procedure's information while it may be executing.

        :param procedure_param: Iterable with procedures.
        :param synchronous: Whether should wait until the procedure
                            finishes its execution or not.
        :return: Information on the procedure.
        :rtype: str(procedure.uuid), procedure.status, procedure.result
                or (str(procedure.uuid))
        """
        assert(len(procedure_param) == 1)
        synchronous = synchronous in (True, "True", "1")
        if synchronous:
            executor = _executor.Executor()
            for procedure in procedure_param:
                executor.wait_for_procedure(procedure)
            return str(procedure_param[-1].uuid), procedure_param[-1].status, \
                procedure_param[-1].result
        else:
            return str(procedure_param[-1].uuid)

    @staticmethod
    def procedure_status(status, details=False):
        """Transform a status reported by :func:`wait_for_procedures` into
        a string that can be used by the command-line interface.

        :param status: The status of the command execution.
        :param details: Boolean that indicates if detailed execution status
                        be returned.

        :return: Return the detailed execution status as a string.
        """
        string = [
            "Procedure :",
            "{ uuid        = %s,",
            "  finished    = %s,",
            "  success     = %s,",
            "  return      = %s,",
            "  activities  = %s",
            "}"
            ]
        result = "\n".join(string)

        if isinstance(status, str):
            return result % (status, "", "", "", "")

        proc_id = status[0]
        operation = status[1][-1]
        returned = None
        activities = ""
        complete = (operation["state"] == _executor.Job.COMPLETE)
        success = (operation["success"] == _executor.Job.SUCCESS)

        if success:
            returned = status[2]
            if details:
                steps = [step["description"] for step in status[1]]
                activities = "\n  ".join(steps)
        else:
            trace = operation["diagnosis"].split("\n")
            returned = trace[-2]
            if details:
                activities = "\n".join(trace)

        return result % (
            proc_id, complete, success, returned, activities
            )

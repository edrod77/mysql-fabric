"""Provide key functions to start, stop and check Fabric's availability and
information on available commands.
"""
import logging
import sys
import inspect
import getpass

import mysql.hub.utils as _utils
import mysql.hub.services as _services
import mysql.hub.events as _events
import mysql.hub.persistence as _persistence
import mysql.hub.failure_detector as _detector
import mysql.hub.config as _config

from mysql.hub.command import (
    Command,
    get_groups,
    get_commands,
    get_command,
    )

_LOGGER = logging.getLogger("mysql.hub.services.manage")

class Logging(Command):
    """Set logging level.
    """
    command_name = "logging_level"

    def execute(self, module, level):
        """ Set logging level.

        :param module: Module that will have its logging level changed.
        :param level: The logging level that will be set.
        :return: Return True if the logging level is changed. Otherwise,
        False.
        """
        try:
            __import__(module)
            logger = logging.getLogger(module)
            logger.setLevel(level)
        except ImportError as error:
            _LOGGER.exception(error)
            return False
        return True


class Ping(Command):
    """Check whether Fabric server is running or not.
    """
    command_name = "ping"

    def execute(self):
        """Check whether Fabric server is running or not.
        """
        return True


class Help(Command):
    """Give help on a command.
    """
    command_name = "help"

    def dispatch(self, group_name, command_name):
        """Give help on a command.

        :param group_name: Group which the command belongs to.
        :param command_name: Command name.
        """
        # TODO: IMPROVE HOW THIS IS PRESENTED. MAYBE WE SHOULD MOVE
        # THIS TO OTHER MODULE WHICH TAKES CARE OF FORMATING STUFF.
        try:
            # Get the command and information on its parameters.
            args = None
            cls = get_command(group_name, command_name)
            try:
                args = inspect.getargspec(cls.execute)[0]
            except AttributeError:
                args = inspect.getargspec(cls.dispatch)[0]
            command_text = \
                "%s %s(%s):" % (group_name, command_name, ", ".join(args[1:]))
            # Format the command documentation.
            doc_text = []
            if cls.__doc__:
                doc_text = [ doc.strip() for doc in cls.__doc__.split("\n") ]
            print >> sys.stderr, command_text, "\n".join(doc_text)
        except KeyError:
            print >> sys.stderr, "Command (%s, %s) was not found." % \
            (group_name, command_name, )


class List(Command):
    """List the possible commands.
    """
    command_name = "list-commands"

    def dispatch(self):
        """List the possible commands and their descriptions.
        """
        # TODO: IMPROVE HOW THIS IS PRESENTED. MAYBE WE SHOULD MOVE
        # THIS TO OTHER MODULE WHICH TAKES CARE OF FORMATING STUFF.
        commands = []
        max_name_size = 0

        # Get the commands and their brief description.
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
        for group_name, command_name, help_text in commands:
            padding_size = max_name_size - len(group_name) - len(command_name)
            padding_size = 0 if padding_size < 0 else padding_size
            padding_text = "".rjust(padding_size, " ")
            text = (group_name, command_name, padding_text, help_text)
            print >> sys.stderr, " ".join(text)


class Start(Command):
    """Start the Fabric server.
    """
    command_name = "start"

    command_options = [
        { 'options': [ '--daemonize'],
          'dest':  'daemonize',
          'default': False,
          'help': "Daemonize the manager"
          },
        ]

    def dispatch(self):
        """Start the Fabric server.
        """
        # Set up the logging information.
        logger = logging.getLogger("mysql.hub")
        handler = None

        # Set up syslog handler, if needed
        if self.options.daemonize:
            address = self.config.get('logging.syslog', 'address')
            handler = logging.handlers.SysLogHandler(address)
        else:
            handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "[%(levelname)s] %(asctime)s - %(threadName)s"
            " %(thread)d - %(message)s")
        handler.setFormatter(formatter)
        logging_level = self.config.get('logging', 'level')
        logger.setLevel(logging_level)
        logger.addHandler(handler)

        # Daemonize ourselves, if we should
        if self.options.daemonize:
            _utils.daemonize()

        logger.info("Fabric node starting.")
        _start(self.config)
        logger.info("Fabric node stopped.")


def _start(config):
    """Start Fabric Server.
    """
    address = config.get('storage', 'address')
    try:
        host, port = address.split(':')
        port = int(port)
    except ValueError:
        host = address
        port = 3306 # TODO: DEFINE A CONSTANT

    user = config.get('storage', 'user')
    database = config.get('storage', 'database')
    try:
        password = config.get('storage', 'password')
    except _config.NoOptionError:
        password = getpass.getpass()

    # Set up the components
    address = config.get("protocol.xmlrpc", "address")
    service_manager = _services.ServiceManager(address)

    # Load all services into the service manager
    service_manager.load_services()

    # Initialize the persistence system. This have to be after the
    # services are loaded to ensure that all persistent classes are
    # defined (hence added to the list of persistent classes).
    _persistence.init(host=host, port=port,
                      user=user, password=password,
                      database=database)
    _persistence.init_thread()

    # Start the executor, failure detector and then service manager.
    _events.Handler().start()
    _detector.FailureDetector.register_groups()
    service_manager.start()


class Stop(Command):
    """Stop the Fabric server.
    """
    command_name = "stop"

    def execute(self):
        """Stop the Fabric server.
        """
        return _shutdown()


def _shutdown():
    """Shutdown Fabric server.
    """
    _detector.FailureDetector.unregister_groups()
    _services.ServiceManager().shutdown()
    _events.Handler().shutdown()
    return True


class FabricLookups(Command):
    """Return a list of Fabric servers.
    """
    command_name = "lookup_fabrics"

    def execute(self):
        """Return a list with all the available Fabric Servers.

        :return: List with existing Fabric Servers.
        :rtype: ["host:port", ...]
        """
        service = _services.ServiceManager()
        return [service.address]

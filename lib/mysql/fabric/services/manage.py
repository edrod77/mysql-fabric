"""Provide key functions to start, stop and check Fabric's availability and
information on available commands.
"""
import getpass
import inspect
import logging
import logging.handlers
import os.path
import sys
import urlparse

from mysql.fabric import (
    config as _config,
    errors as _errors,
    events as _events,
    executor as _executor,
    failure_detector as _detector,
    persistence as _persistence,
    recovery as _recovery,
    services as _services,
    utils as _utils,
)

from mysql.fabric.command import (
    Command,
    get_groups,
    get_commands,
    get_command,
    )

_LOGGER = logging.getLogger(__name__)

# Logging levels.
_LOGGING_LEVELS = {
    "CRITICAL" : logging.CRITICAL,
    "ERROR" : logging.ERROR,
    "WARNING" : logging.WARNING,
    "INFO" : logging.INFO,
    "DEBUG" : logging.DEBUG
}

# Number of concurrent threads that are created to handle requests.
DEFAULT_N_THREADS = 1

# Number of concurrent executors that are created to handle jobs.
DEFAULT_N_EXECUTORS = 1

class Logging(Command):
    """Set logging level.
    """
    group_name = "manage"
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
            logger.setLevel(_LOGGING_LEVELS[level.upper()])
        except (KeyError, ImportError) as error:
            _LOGGER.exception(error)
            return False
        return True


class Ping(Command):
    """Check whether Fabric server is running or not.
    """
    group_name = "manage"
    command_name = "ping"

    def execute(self):
        """Check whether Fabric server is running or not.
        """
        return True


class Help(Command):
    """Give help on a command.
    """
    group_name = "manage"
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
    group_name = "manage"
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
    group_name = "manage"
    command_name = "start"

    command_options = [
        { 'options': [ '--daemonize'],
          'dest':  'daemonize',
          'default': False,
          'action' : "store_true",
          'help': "Daemonize the manager"
          },
        ]

    def dispatch(self):
        """Start the Fabric server.
        """
        # Configure logging.
        _configure_logging(self.config, self.options.daemonize)

        # Configure connections.
        _configure_connections(self.config)

        # Daemonize ourselves.
        if self.options.daemonize:
            _utils.daemonize()

        # Start Fabric server.
        _LOGGER.info("Fabric node starting.")
        _start(self.options, self.config)
        _services.ServiceManager().wait()
        _LOGGER.info("Fabric node stopped.")


class Setup(Command):
    """Setup Fabric Storage System.

    Create a database and necessary objects.
    """
    group_name = "manage"
    command_name = "setup"

    def dispatch(self):
        """Setup Fabric Storage System.
        """
        # Configure logging.
        _configure_logging(self.config, False)

        # Configure connections.
        _configure_connections(self.config)

        # Create database and objects.
        _persistence.setup()


class Teardown(Command):
    """Teardown Fabric Storage System.

    Drop database and its objects.
    """
    group_name = "manage"
    command_name = "teardown"

    def dispatch(self):
        """Teardown Fabric Storage System.
        """
        # Configure logging.
        _configure_logging(self.config, False)

        # Configure connections.
        _configure_connections(self.config)

        # Drop database and objects.
        _persistence.teardown()


def _create_file_handler(config, info, delay=0):
    """Define a file handler where logging information will be
    sent to.
    """
    from logging.handlers import RotatingFileHandler
    assert info.scheme == 'file'
    if info.netloc:
        raise _errors.ConfigurationError(
            "Malformed file URL '%s'" % (info.geturl(),)
        )
    if os.path.isabs(info.path):
        path = info.path
    else:
        # Relative path, fetch the logdir from the configuration.
        # Using 'logging' section instead of 'DEFAULT' to allow
        # configuration parameters to be overridden in the logging
        # section.
        logdir = config.get('logging', 'logdir')
        path = os.path.join(logdir, info.path)
    return RotatingFileHandler(path, delay=delay)

# A URL provided should either have a path or an address, but not
# both. For example:
#
#   syslog:///dev/log       ---> Address is /dev/log
#   syslog://localhost:555  ---> Address is ('localhost', 555)
#   syslog://my.example.com ---> Address is ('my.example.com', 541)
#
# The following is not allowed:
#
#   syslog://example.com/foo/bar
def _create_syslog_handler(config, info):
    """Define a syslog handler where logging information will be
    sent to.
    """
    from logging.handlers import SYSLOG_UDP_PORT, SysLogHandler
    assert info.scheme == 'syslog'
    if info.netloc and info.path:
        raise _errors.ConfigurationError(
            "Malformed syslog URL '%s'" % (info.geturl(),)
        )
    if info.netloc:
        assert not info.path
        address = info.netloc.split(':')
        if len(address) == 1:
            address.append(SYSLOG_UDP_PORT)
    elif info.path:
        assert not info.netloc
        address = info.path

    return SysLogHandler(address=address)

_LOGGING_HANDLER = {
    'file': _create_file_handler,
    'syslog': _create_syslog_handler,
}

def _configure_logging(config, daemon):
    """Configure the logging system.
    """
    # Set up the logging information.
    logger = logging.getLogger("mysql.fabric")
    handler = None

    # Set up logging handler
    if daemon:
        urlinfo = urlparse.urlparse(config.get('logging', 'url'))
        handler = _LOGGING_HANDLER[urlinfo.scheme](config, urlinfo)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "[%(levelname)s] %(asctime)s - %(threadName)s"
        " - %(message)s")
    handler.setFormatter(formatter)
    try:
        level = config.get("logging", "level")
        logger.setLevel(_LOGGING_LEVELS[level.upper()])
    except KeyError:
        logger.setLevel(_LOGGING_LEVELS["INFO"])
    logger.addHandler(handler)


def _configure_connections(config):
    """Configure information on database connection and remote
    servers.
    """

    # Configure the number of concurrent executors.
    try:
        number_executors = config.get('executor', "executors")
        number_executors = int(number_executors)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        number_executors = DEFAULT_N_EXECUTORS
    executor = _executor.Executor()
    executor.set_number_executors(number_executors)

    # Fetch options to configure the XML-RPC.
    address = config.get('protocol.xmlrpc', "address")

    # Configure the number of concurrent threads.
    try:
        number_threads = config.get('protocol.xmlrpc', "threads")
        number_threads = int(number_threads)
    except (_config.NoOptionError, ValueError):
        number_threads = DEFAULT_N_THREADS

    # Define XML-RPC configuration.
    _services.ServiceManager(address, number_threads)

    # Fetch options to configure the state store.
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
    try:
        timeout = config.get("storage", "connect_timeout")
        timeout = float(timeout)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        timeout = None

    # Define state store configuration.
    _persistence.init(host=host, port=port,
                      user=user, password=password,
                      database=database, timeout=timeout)


def _start(options, config):
    """Start Fabric server.
    """
    # Load all services into the service manager
    _services.ServiceManager().load_services(options, config)

    # Initilize the state store.
    _persistence.init_thread()

    # Start the executor, failure detector and then service manager.
    _events.Handler().start()
    _recovery.recovery()
    _detector.FailureDetector.register_groups()
    _services.ServiceManager().start()


class Stop(Command):
    """Stop the Fabric server.
    """
    group_name = "manage"
    command_name = "stop"

    def execute(self):
        """Stop the Fabric server.
        """
        _shutdown()
        return True


def _shutdown():
    """Shutdown Fabric server.
    """
    _detector.FailureDetector.unregister_groups()
    _services.ServiceManager().shutdown()
    _events.Handler().shutdown()
    _events.Handler().wait()


class FabricLookups(Command):
    """Return a list of Fabric servers.
    """
    group_name = "manage"
    command_name = "lookup_fabrics"

    def execute(self):
        """Return a list with all the available Fabric Servers.

        :return: List with existing Fabric Servers.
        :rtype: ["host:port", ...]
        """
        service = _services.ServiceManager()
        return [service.address]

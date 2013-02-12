"""Package containing all services available in the manager.

Services are interfaces accessible externally over the network.
"""
import socket
import logging
import pkgutil
import re
import threading
import types
import inspect

import mysql.hub.protocols.xmlrpc as _protocol

from mysql.hub.utils import (
    Singleton
    )

from mysql.hub.errors import (
    ServiceError
    )

from mysql.hub.command import (
    Command,
    register_command,
    get_groups,
    get_commands,
    get_command,
    )

_LOGGER = logging.getLogger(__name__)

def find_services():
    """Find which are the available commands.
    """
    services = [
        imp.find_module(name).load_module(name)
        for imp, name, ispkg in pkgutil.iter_modules(__path__)
        if not ispkg
        ]

    for mod in services:
        for sym, val in mod.__dict__.items():
            if isinstance(val, type) and issubclass(val, Command) and \
               val <> Command and re.match("[A-Za-z]\w+", sym):
               try:
                   val.group_name
               except AttributeError:
                   val.group_name = mod.__name__
               try:
                   val.command_name
               except AttributeError:
                   val.command_name = sym.lower()
               register_command(val.group_name, val.command_name, val)

    # TODO: We temporarily keep this while we are changing the current
    # services into commands.
    return services

def find_client():
    """Return a proxy to access the Facric server.
    """
    return _protocol.MyClient()

class ServiceManager(Singleton):
    """This is the service manager, which processes service requests.

    The service manager is currently implemented as an XML-RPC server,
    but once we start using several different protocols, we need to
    dispatch serveral different servers.

    Services are not automatically loaded when the service manager is
    constructed, so the load_services have to be called explicitly to
    load the services in the package.
    """
    def __init__(self, address):
        """Setup all protocol services.
        """
        Singleton.__init__(self)
        self.__address = address
        host, port = self.__address.split(':')
        _LOGGER.info("XML-RPC protocol server configured for listening "
                     "on %s:%s.", host, str(port))
        self.__rpc_server = _protocol.MyServer(host, int(port))

    @property
    def address(self):
        """Return address in use by the service.

        :return: Address as host:port.
        :rtype: String.
        """
        return self.__address

    def start(self):
        """Start and run all services managed by the service manager.

        There can be multiple protocol servers active, so this
        function will just dispatch the threads for handling those
        servers and then return.
        """
        _LOGGER.info("XML-RPC protocol server started.")
        self.__rpc_server.serve_forever()

    def shutdown(self):
        """Shut down all services managed by the service manager.
        """
        self.__rpc_server.shutdown()

    def load_services(self):
        """Load services into each protocol server.
        """
        _LOGGER.info("Loading Services.")

        services = find_services()

        for group_name in get_groups():
            for command_name in get_commands(group_name):
                command = get_command(group_name, command_name)
                if hasattr(command, "execute"):
                    self.__rpc_server.register_command(command())

        # TODO: We temporarily keep this while we are changing the current
        # services into commands.
        for mod in services:
            for sym, val in mod.__dict__.items():
                if isinstance(val, types.FunctionType) \
                        and re.match("[A-Za-z]\w+", sym):
                   _LOGGER.debug("Registering %s.", mod.__name__ + '.' + sym)
                   self.__rpc_server.register_function(
                       val, mod.__name__ + '.' + sym
                   )

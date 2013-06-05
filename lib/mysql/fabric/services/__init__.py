"""Package containing all services available in the manager.

Services are interfaces accessible externally over the network.
"""
import socket
import logging
import pkgutil
import threading
import os

import mysql.fabric.protocols.xmlrpc as _protocol

from mysql.fabric.utils import (
    Singleton
    )

from mysql.fabric.errors import (
    ServiceError
    )

from mysql.fabric.command import (
    get_groups,
    get_commands,
    get_command,
    )

_LOGGER = logging.getLogger(__name__)

def find_commands():
    """Find which are the available commands.
    """
    for imp, name, ispkg in pkgutil.walk_packages(__path__, __name__ + "."):
        mod = imp.find_module(name).load_module(name)
        _LOGGER.debug("%s %s has got __name__ %s" % (
            "Package" if ispkg else "Module", name, mod.__name__))

def find_client():
    """Return a proxy to access the Fabric server.
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

    @property
    def rpc_server(self):
        """Return a reference to the Server Service.
        """
        return self.__rpc_server

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

    def load_services(self, options, config):
        """Load services into each protocol server.

        :param options: The options for the commands that shall be
                        created.
        :param config: The configuration for the commands that shall
                       be created.
        """
        _LOGGER.info("Loading Services.")

        find_commands()

        for group_name in get_groups():
            for command_name in get_commands(group_name):
                command = get_command(group_name, command_name)
                if hasattr(command, "execute"):
                    _LOGGER.debug(
                        "Registering %s.", command.group_name + '.' + \
                        command.command_name
                        )
                    cmd = command()
                    cmd.setup_server(self.__rpc_server, options, config)
                    self.__rpc_server.register_command(cmd)

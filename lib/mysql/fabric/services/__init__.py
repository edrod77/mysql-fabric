"""Package containing all services available in the manager.

Services are interfaces accessible externally over the network.
"""
import socket
import logging
import pkgutil
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

    Currently the service manager only supports a XML-RPC server,
    but once we start using several different protocols, we need
    to dispatch several different servers.

    Services are not automatically loaded when the service manager is
    constructed, so the load_services have to be called explicitly to
    load the services in the package.
    """
    def __init__(self, address, number_threads):
        """Setup all protocol services.
        """
        Singleton.__init__(self)
        self.__address = address
        host, port = self.__address.split(':')
        self.__rpc_server = _protocol.MyServer(
            host, int(port), number_threads
        )

    @property
    def address(self):
        """Return address in use by the service.

        :return: Address as host:port.
        :rtype: String.
        """
        return self.__address

    def start(self):
        """Start all services managed by the service manager.
        """
        self.__rpc_server.start()

    def shutdown(self):
        """Shut down all services managed by the service manager.
        """
        self.__rpc_server.shutdown()

    def wait(self):
        """Wait until all the sevices are properly finished.
        """
        self.__rpc_server.wait()

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
                    # TODO: The same object is shared among the sessions.
                    # We need to improve this.
                    cmd = command()
                    cmd.setup_server(self.__rpc_server, options, config)
                    self.__rpc_server.register_command(cmd)

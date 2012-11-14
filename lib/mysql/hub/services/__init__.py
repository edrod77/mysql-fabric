"""Package containing all services available in the manager.

Services are interfaces accessible externally over the network.
"""

import SimpleXMLRPCServer as _xmlrpc
import socket
import logging
import pkgutil
import re
import threading
import types

from mysql.hub.utils import Singleton
from mysql.hub.errors import ServiceError

_LOGGER = logging.getLogger(__name__)

def _load_services_into_server(server):
    """Load all services found in this package into a server.

    If the instance already had services registered, they will be
    removed and the new services reloaded.
    """

    services = [
        imp.find_module(name).load_module(name)
        for imp, name, ispkg in pkgutil.iter_modules(__path__)
        if not ispkg
        ]

    for mod in services:
        for sym, val in mod.__dict__.items():
            if isinstance(val, types.FunctionType) \
                    and re.match("[A-Za-z]\w+", sym):
                _LOGGER.debug("Registering %s.", mod.__name__ + '.' + sym)
                server.register_function(val, mod.__name__ + '.' + sym)

# TODO: Move this class into the mysql.hub.protocol package once we
# have created support for loading multiple protocol servers.
class MyXMLRPCServer(_xmlrpc.SimpleXMLRPCServer):
    def __init__(self, address):
        _xmlrpc.SimpleXMLRPCServer.__init__(self, address, logRequests=False)
        self.__running = False

    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _xmlrpc.SimpleXMLRPCServer.server_bind(self)

    def serve_forever(self, poll_interval=0.5):
        self.__running = True
        while self.__running:
            self.handle_request()

    def shutdown(self):
        self.__running = False

class ServiceManager(Singleton):
    """This is the service manager, which processes service requests.

    The service manager is currently implemented as an XML-RPC server,
    but once we start using several different protocols, we need to
    dispatch serveral different servers.

    Services are not automatically loaded when the service manager is
    constructed, so the load_services have to be called explicitly to
    load the services in the package.
    """
    def __init__(self, config=None, shutdown=None):
        """Setup all protocol services.
        """
        assert(config is not None or shutdown is not None)
        Singleton.__init__(self)
        self.__xmlrpc_thread = None

        # TODO: Move setup of XML-RPC protocol server into protocols package
        address = config.get("protocol.xmlrpc", "address")
        _host, port = address.split(':')
        self.__xmlrpc = MyXMLRPCServer(("localhost", int(port)))
        self.__xmlrpc.register_function(shutdown, "shutdown")

    def start(self):
        """Start and run all services managed by the service manager.

        There can be multiple protocol servers active, so this
        function will just dispatch the threads for handling those
        servers and then return.
        """
        _LOGGER.info("XML-RPC protocol server started.")
        self.__xmlrpc_thread = threading.Thread(
            target=self.__xmlrpc.serve_forever, name="ServiceManager")
        self.__xmlrpc_thread.start()

    def shutdown(self):
        """Shut down all services managed by the service manager.
        """
        self.__xmlrpc.shutdown()
        def xmlrpc_wait():
            self.__xmlrpc_thread.join()
            _LOGGER.info("XML-RPC protocol server stopped.")
        thread = threading.Thread(target=xmlrpc_wait,
                                  name="WaitServiceManager")
        thread.start()

    def load_services(self):
        """Set up protocol servers and load services into each
        protocol server.
        """
        _LOGGER.info("Loading Services.")
        self.__xmlrpc.register_introspection_functions()
        for server in [self.__xmlrpc]:
            _load_services_into_server(server)

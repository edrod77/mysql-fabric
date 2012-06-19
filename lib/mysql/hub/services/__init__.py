"""Package containing all services available in the manager.

Services are interfaces accessible externally over the network.
"""

import pkgutil
import re
import types

from SimpleXMLRPCServer import SimpleXMLRPCServer

# TODO: Move this class into the mysql.hub.protocol package once we
# have created support for loading multiple protocol servers.
class MyXMLRPCServer(SimpleXMLRPCServer):
    def serve_forever(self):
	self.__running = True
	while self.__running:
	    self.handle_request()

    def shutdown(self):
        self.__running = False

class ServiceManager(object):
    """This is the service manager, which processes service requests.

    The service manager is currently implemented as an XML-RPC server,
    but once we start using several different protocols, we need to
    dispatch serveral different servers.

    Services are not automatically loaded when the service manager is
    constructed, so the load_services have to be called explicitly to
    load the services in the package.
    """

    _SERVICES = [
        imp.find_module(name).load_module(name)
        for imp, name, ispkg in pkgutil.iter_modules(__path__)
        if not ispkg
        ]

    def _register_standard_functions(self, server):
        """Register a set of standard top-level functions with the
        protocol server.

        shutdown
           Shutdown the entire manager.
        """

        # It is necessary to create a separate function since the
        # registered function cannot return None and shutdown returns
        # None.
        def _kill():
            self.__manager.shutdown()
            return True

        server.register_function(_kill, "shutdown")

    def __init__(self, manager):
        """Start all protocol services.
        """

        self.__manager = manager

        # TODO: Move the setup of the XML-RPC protocol server into the protocols package
        port = manager.config.getint("protocol.xmlrpc", "port")
        self.__xmlrpc = MyXMLRPCServer(("localhost", port))
        self._register_standard_functions(self.__xmlrpc)

    def start(self):
        """Start all services managed by the service manager.
        """
        self.__xmlrpc.serve_forever()

    def shutdown(self):
        """Shut down all services managed by the service manager.
        """
        log = self.__manager.logger
        log.info("Shutting down service manager")
        self.__xmlrpc.shutdown()

    def load_services(self):
        """Set up protocol servers and load services into each
        protocol server.
        """

        for server in [self.__xmlrpc]:
            self._load_services_into_server(server)

    def _load_services_into_server(self, server):
        """Load all services found in this package into a server.

        If the instance already had services registered, they will be
        removed and the new services reloaded.
        """

        for mod in self._SERVICES:
            for sym, val in mod.__dict__.items():
                if isinstance(val, types.FunctionType) \
                        and re.match("[A-Za-z]\w+", sym):
                    server.register_function(val, mod.__name__ + '.' + sym)

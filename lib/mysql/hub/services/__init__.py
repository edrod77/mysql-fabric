"""Package containing all services available in the manager.

Services are interfaces accessible externally over the network.
"""

import SimpleXMLRPCServer as _xmlrpc
import logging
import pkgutil
import re
import threading
import types

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

    def serve_forever(self, poll_interval=0.5):
        self.__running = True
        while self.__running:
            self.handle_request()

    def shutdown(self):
        self.__running = False

class ServiceManager(threading.Thread):
    """This is the service manager, which processes service requests.

    The service manager is currently implemented as an XML-RPC server,
    but once we start using several different protocols, we need to
    dispatch serveral different servers.

    Services are not automatically loaded when the service manager is
    constructed, so the load_services have to be called explicitly to
    load the services in the package.
    """

    def _register_standard_functions(self, server):
        """Register a set of standard top-level functions with the
        protocol server.

        shutdown
           Shutdown the entire Fabric node.

        set_logging_level
           Set the logging level of the Fabric node.
        """

        # It is necessary to create a separate function since the
        # registered function cannot return None and shutdown returns
        # None.
        def _kill():
            self.__manager.shutdown()
            return True

        def _set_logging_level(module, level):
            _LOGGER.info("Trying to set logging level (%s) for module (%s)." %\
                          (level, module))
            try:
                __import__(module)
                logger = logging.getLogger(module)
                logger.setLevel(level)
            except Exception as error:
                _LOGGER.exception(error)
                return error
            return True

        server.register_function(_kill, "shutdown")
        server.register_function(_set_logging_level, "set_logging_level")

    def __init__(self, manager):
        """Setup all protocol services.
        """
        super(ServiceManager, self).__init__(name="ServiceManager")
        self.__manager = manager

        # TODO: Move setup of XML-RPC protocol server into protocols package
        address = manager.config.get("protocol.xmlrpc", "address")
        _host, port = address.split(':')
        self.__xmlrpc = MyXMLRPCServer(("localhost", int(port)))
        self._register_standard_functions(self.__xmlrpc)

    def run(self):
        """Start and run all services managed by the service manager.

        There can be multiple protocol servers active, so this
        function will just dispatch the threads for handling those
        servers and then return.
        """
        _LOGGER.info("XML-RPC protocol server started")
        self.__xmlrpc.serve_forever()
        _LOGGER.info("XML-RPC protocol server stopped")

    def shutdown(self):
        """Shut down all services managed by the service manager.
        """
        _LOGGER.info("Shutting down service manager")
        self.__xmlrpc.shutdown()

    def load_services(self):
        """Set up protocol servers and load services into each
        protocol server.
        """

        _LOGGER.info("Loading Services.")
        self.__xmlrpc.register_introspection_functions()
        for server in [self.__xmlrpc]:
            _load_services_into_server(server)

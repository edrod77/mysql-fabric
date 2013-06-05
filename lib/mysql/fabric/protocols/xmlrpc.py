"""Define a XML-RPC server and client.
"""
import xmlrpclib
import socket
import sys

from SimpleXMLRPCServer import (
    SimpleXMLRPCServer,
    )

class MyServer(SimpleXMLRPCServer):
    """Simple XML-RPC server.
    """

    def __init__(self, host, port):
        """Create a MyServer object.
        """
        SimpleXMLRPCServer.__init__(self, (host, port), logRequests=False)
        self.__running = False
        self.register_introspection_functions()

    def server_bind(self):
        """Manipulate the option reuse address and bind the socket to
        the server.
        """
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        SimpleXMLRPCServer.server_bind(self)

    def shutdown(self):
        """Shutdown the server.
        """
        self.__running = False

    def register_command(self, command):
        "Register a command with the server."
        self.register_function(
            command.execute, command.group_name + "." + command.command_name
            )

    def serve_forever(self, poll_interval=0.5):
        """Define the server's main loop.
        """
        self.__running = True
        while self.__running:
            self.handle_request()


class MyClient(xmlrpclib.ServerProxy):
    """Simple XML-RPC Client.

    This class defines the client-side interface of the command subsystem.
    """
    def __init__(self):
        # TODO: Notice that the call to the __init__ is placed in the dispatch
        # when the call to the server happens. Maybe we should move this to
        # to here.
        """Create a MyClient object.
        """
        pass

    def dispatch(self, command, *args):
        """Default dispatch method.

        This is the default dispatch method that will just dispatch
        the command with arguments to the server.
        """

        uri = "http://%s" % command.config.get('protocol.xmlrpc', 'address')
        xmlrpclib.ServerProxy.__init__(self, uri)
        try:
            reference = command.group_name + "." + command.command_name
            return getattr(self, reference)(*args)
        except xmlrpclib.Fault as error:
            # TODO: IMPROVE ERROR HANDLING. MAYBE WE SHOULD CREATE AN
            # EXCEPTION IF THE ERROR MESSAGE HAS INFORMATION ON ONE.
            # FOR EXAMPLE:
            # <Fault 1: "<class 'mysql.fabric.errors.JobError'>:Job not found.">
            print >> sys.stderr, error

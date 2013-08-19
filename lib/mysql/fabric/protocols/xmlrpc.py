#
# Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#

"""Define a XML-RPC server and client.
"""
import xmlrpclib
import socket
import sys
import threading
import Queue
import logging

from SimpleXMLRPCServer import (
    SimpleXMLRPCServer,
)
from SocketServer import (
    ThreadingMixIn,
)

import mysql.fabric.persistence as _persistence

_LOGGER = logging.getLogger(__name__)


class MyServer(threading.Thread, ThreadingMixIn, SimpleXMLRPCServer):
    """Simple XML-RPC server.
    """
    def __init__(self, host, port, number_threads):
        """Create a MyServer object.
        """
        SimpleXMLRPCServer.__init__(self, (host, port), logRequests=False)
        threading.Thread.__init__(self, name="XML-RPC-Server")
        self.register_introspection_functions()
        self.__number_threads = number_threads
        self.__requests = None
        self.__threads = []
        self.__is_running = True
        self.daemon = True
        self.__lock = threading.Condition()

    def run(self):
        """Call the main routine.
        """
        self.serve_forever()

    def shutdown(self):
        """Stop the server.
        """
        # Avoid possible errors if different threads try to
        # stop the server.
        with self.__lock:
            if not self.__is_running:
                return
            self.__is_running = False
            self.__lock.notify_all()

        for thread in self.__threads:
            assert(self.__requests is not None)
            self.__requests.put(None)

    def wait(self):
        """Wait until the server shuts down.
        """
        with self.__lock:
            while self.__is_running:
                self.__lock.wait()

        for thread in self.__threads:
            thread.join()

    def register_command(self, command):
        """Register a command with the server.
        """
        self.register_function(
            command.execute, command.group_name + "." + command.command_name
            )

    def server_bind(self):
        """Manipulate the option reuse address and bind the socket to
        the server.
        """
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        SimpleXMLRPCServer.server_bind(self)

    def process_request_thread(self):
        """Obtain request from queue instead of directly from server socket.
        """
        _LOGGER.info("Started XML-RPC-Session.")
        _persistence.init_thread()
        while True:
            assert(self.__requests is not None)
            request = self.__requests.get()
            if request is None:
                break
            ThreadingMixIn.process_request_thread(self, *request)
        _persistence.deinit_thread()

    def handle_request(self):
        """Put requests into a queue for the dispatchers.
        """
        try:
            request, client_address = self.get_request()
        except socket.error:
            return
        if self.verify_request(request, client_address):
            assert(self.__requests is not None)
            self.__requests.put((request, client_address))

    def serve_forever(self):
        """Main routine which handles one request at a time.
        """
        # TODO: Define a lower and upper bound.
        self.__requests = Queue.Queue(self.__number_threads)

        _LOGGER.info(
            "XML-RPC protocol server %s started.", self.server_address
        )
        _LOGGER.info("Setting %s XML-RPC session(s).", self.__number_threads)

        for nt in range(0, self.__number_threads):
            thread = threading.Thread(
                target = self.process_request_thread,
                name="XML-RPC-Session-%s" % (nt, )
            )
            thread.daemon = True
            thread.start()
            self.__threads.append(thread)

        while self.__is_running:
            self.handle_request()
        self.server_close()


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

        address = command.config.get('protocol.xmlrpc', 'address')
        host, port = address.split(":")
        if not host:
            host = "localhost"
        uri = "http://%s:%s" % (host, port)
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

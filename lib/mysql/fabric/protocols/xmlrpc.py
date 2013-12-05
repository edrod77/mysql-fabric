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
    """Multi-threaded XML-RPC server whose threads are created during startup.

    .. note::
       Threads cannot be dynamically created so that the Multi-threaded XML-RPC
       cannot easily adapt to changes in the load.

    :param host: Address used by the XML-RPC Server.
    :param port: Port used by the XML-RPC Server.
    :param number_threads: Number of threads that will be started.
    """
    def __init__(self, host, port, number_threads):
        """Create a MyServer object.
        """
        SimpleXMLRPCServer.__init__(self, (host, port), logRequests=False)
        threading.Thread.__init__(self, name="XML-RPC-Server")
        self.register_introspection_functions()
        self.__number_threads = number_threads
        self.__threads = Queue.Queue(number_threads)
        self.__is_running = True
        self.daemon = True
        self.__lock = threading.Condition()

    def register_command(self, command):
        """Register a command with the server.

        :param command: Command to be registered.
        """
        self.register_function(
            command.execute, command.group_name + "." + command.command_name
            )

    def register_thread(self, thread):
        """Register a reference to an idle thread that can used to
        process incoming requests.
        """
        self.__threads.put(thread)

    def shutdown(self):
        """Shutdown the server.
        """
        thread = SessionThread.get_reference()
        assert(thread is not None)
        thread.shutdown()

    def shutdown_now(self):
        """Shutdown the server immediately without waiting until any
        ongoing activity finishes.
        """
        # Avoid possible errors if different threads try to stop the
        # server.
        with self.__lock:
            if not self.__is_running:
                return
            self.server_close()
            self.__is_running = False
            self.__lock.notify_all()

    def wait(self):
        """Wait until the server shuts down.
        """
        with self.__lock:
            while self.__is_running:
                self.__lock.wait()

    def run(self):
        """Main routine which handles one request at a time.
        """
        _LOGGER.info(
            "XML-RPC protocol server %s started.", self.server_address
        )

        self._create_sessions()
        while self.__is_running:
            try:
                request, client_address = self.get_request()
                self._process_request(request, client_address)
            except Exception as error:
                _LOGGER.warning("Error accessing request: (%s)." % (error, ))

    def _process_request(self, request, client_address):
        """Process a request by delegating it to an idle session thread.
        """
        if self.verify_request(request, client_address):
            thread = self.__threads.get()
            _LOGGER.debug(
                "Enqueuing request (%s) from (%s) through thread (%s)." %
                (request, client_address, thread)
            )
            thread.process_request(request, client_address)

    def _create_sessions(self):
        """Create session threads.
        """
        _LOGGER.info("Setting %s XML-RPC session(s).", self.__number_threads)

        for nt in range(0, self.__number_threads):
            thread = SessionThread("XML-RPC-Session-%s" % (nt, ), self)
            try:
                thread.start()
            except Exception as error:
                _LOGGER.error("Error starting thread: (%s)." % (error, ))


class SessionThread(threading.Thread):
    """Session thread which is responsible for handling incoming requests.

    :param name: Thread's name.
    :param server: Reference to server object which knows how to handle
                   requests.
    """
    local_thread = threading.local()

    def __init__(self, name, server):
        """Create a SessionThread object.
        """
        threading.Thread.__init__(self, name=name)
        self.__requests = Queue.Queue()
        self.__server = server
        self.__is_shutdown = False
        self.daemon = True

    @staticmethod
    def get_reference():
        """Get a reference to a SessionThread object associated
        to the current thread or None.

        :return: Reference to a SessionThread object associated
                 to the current thread or None.
        """
        try:
            return SessionThread.local_thread.thread
        except AttributeError:
            pass
        return None

    def process_request(self, request, client_address):
        """Register a request to be processed by this SessionThread
        object.
        """
        self.__requests.put((request, client_address))

    def run(self):
        """Process registered requests.
        """
        _LOGGER.info("Started XML-RPC-Session.")
        try:
            _persistence.init_thread()
        except Exception as error:
            _LOGGER.warning("Error connecting to backing store: (%s)." %
                            (error, )
            )

        SessionThread.local_thread.thread = self
        self.__server.register_thread(self)

        while True:
            request, client_address = self.__requests.get()
            _LOGGER.debug(
                "Processing request (%s) from (%s) through thread (%s)." %
                (request, client_address, self)
            )
            # There is no need to catch exceptions here because the method
            # process_request_thread already does so. It is the main entry
            # point in the code which means that any uncaught exception
            # in the code will be reported as xmlrpclib.Fault.
            self.__server.process_request_thread(request, client_address)
            _LOGGER.debug(
                "Finishing request (%s) from (%s) through thread (%s)." %
                (request, client_address, self)
            )
            if self.__is_shutdown:
                self.__server.shutdown_now()
            self.__server.register_thread(self)

        try:
            _persistence.deinit_thread()
        except Exception as error:
            _LOGGER.warning("Error connecting to backing store: (%s)." %
                            (error, ))

    def shutdown(self):
        """Register that this thread is responsible for shutting down
        the server.
        """
        self.__is_shutdown = True


class MyClient(xmlrpclib.ServerProxy):
    """Simple XML-RPC Client.

    This class defines the client-side interface of the command subsystem.
    The connection to the XML-RPC Server is made when the dispatch method
    is called. This is done because the information on the server is passed
    to the command object after its creation.
    """
    def __init__(self):
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
        reference = command.group_name + "." + command.command_name
        return getattr(self, reference)(*args)

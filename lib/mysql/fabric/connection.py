#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
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

"""Define functions that can be used throughout the code.
"""
import logging
import threading

import mysql.connector
import mysql.connector.errors

from mysql.fabric.errors import (
    DatabaseError,
    ProgrammingError
)

from mysql.connector.cursor import (
    MySQLCursor,
    MySQLCursorRaw,
    MySQLCursorNamedTuple
)

from mysql.connector.conversion import (
    MySQLConverter,
)

from mysql.fabric.utils import (
    Singleton
)

_LOGGER = logging.getLogger(__name__)

MYSQL_DEFAULT_PORT = 3306

def split_host_port(address, default_port):
    """Return a tuple with host and port.

    If a port is not found in the address, the default port is returned.
    """
    if address.find(":") >= 0:
        host, port = address.split(":")
    else:
        host, port = (address, default_port)
    return host, port

def combine_host_port(host, port, default_port):
    """Return a string with the parameters host and port.

    :return: String host:port.
    """
    if host:
        if host == "127.0.0.1":
            host_info = "localhost"
        else:
            host_info = host
    else:
        host_info = "unknown host"

    if port:
        port_info = port
    else:
        port_info = default_port

    return "%s:%s" % (host_info, port_info)

class MySQLConnection(object):
    """Create a MySQLConnection object.
    """
    def __init__(self, *args, **kwargs):
        """Create a MySQLConnection object.
        """
        self.__cnx = mysql.connector.MySQLConnection()
        if args or kwargs:
            self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        """Effectively connect to the server.
        """
        try:
            self.__cnx.connect(*args, **kwargs)
        except mysql.connector.Error as error:
            raise DatabaseError(error)

    def exec_stmt(self, stmt_str, options=None):
        """Execute a statement for the client and return a result set or a
        cursor.

        This is the singular method to execute queries. If something goes
        wrong while executing a statement, the exception
        :class:`~mysql.fabric.errors.DatabaseError` is raised.

        :param stmt_str: The statement (e.g. query, updates, etc) to execute.
        :param options: Options to control behavior:

                        - params - Parameters for statement.
                        - columns - If true, return a rows as named tuples
                          (default is False).
                        - raw - If true, do not convert MySQL's types to
                          Python's types (default is True).
                        - fetch - If true, execute the fetch as part of the
                          operation (default is True).

        :return: Either a result set as list of tuples (either named or
                 unnamed) or a cursor.
        """
        if self.__cnx is None:
            raise DatabaseError("Invalid database connection.")

        options = options or {}
        params = options.get('params', ())
        columns = options.get('columns', False)
        fetch = options.get('fetch', True)
        raw = options.get('raw', False)

        if raw and columns:
            raise ProgrammingError(
                "No raw cursor available returning named tuple"
            )

        _LOGGER.debug("Statement ({statement}, Params({parameters}).".format(
            statement=stmt_str.replace('\n', '').replace('\r', ''),
            parameters=params)
        )

        cur = None
        try:
            cur = self.__cnx.cursor(raw=raw, named_tuple=columns)
            cur.execute(stmt_str, params)
        except mysql.connector.errors.Error as error:
            if cur:
                cur.close()

            errno = getattr(error, 'errno', None)
            raise DatabaseError(
                "Command (%s, %s) failed accessing (%s). %s." %
                (stmt_str, params, self.address(), error), errno
            )

        assert(cur is not None)
        if fetch:
            results = None
            try:
                if self.__cnx.unread_result:
                    results = cur.fetchall()
            except mysql.connector.errors.InterfaceError as error:
                raise DatabaseError(
                    "Command (%s, %s) failed fetching data from (%s). %s." %
                    (stmt_str, params, self.address(), error)
                )
            finally:
                cur.close()
            return results

        return cur

    def shutdown(self):
        """Close the connection.
        """
        try:
            if self.__cnx:
                self.__cnx.shutdown()
        except mysql.connector.errors.Error as error:
            raise DatabaseError(
                "Error trying to disconnect from (%s). %s." %
                (self.address(), error)
            )

    def is_valid_mysql_connection(self):
        """Check if it is a valid MySQL connection.
        """
        if self.__cnx is not None and self.__cnx.is_connected():
            return True
        return False

    def reestablish_mysql_connection(self, attempt, delay):
        """Try to reconnect if it is not already connected.
        """
        try:
            self.__cnx.reconnect(attempt, delay)
        except (AttributeError, mysql.connector.errors.InterfaceError):
            raise DatabaseError("Invalid database connection.")

    def address(self):
        """Return address associated to connection.
        """
        return ":".join([self.__cnx.server_host, str(self.__cnx.server_port)])

    @property
    def user(self):
        """Return user employeed to create connection.
        """
        return self.__cnx.user

class MySQLConnectionManager(Singleton):
    """Manages MySQL Servers' connections.

    The pool is internally implemented as a dictionary that maps a server's
    uuid to a sequence of connections.
    """
    def __init__(self):
        """Creates a MySQLConnectionManager object.
        """
        super(MySQLConnectionManager, self).__init__()
        self.__pool = {}
        self.__lock = threading.RLock()
        self.__tracker = {}

    def _do_create_connection(self, server):
        """Create a connection and return it.

        Connections are established in two phases. First a connection object
        is created and registered into a tracker and than the connection is
        finally established. With a reference to the connection object, an
        external function might kill connections to a server thus unblocking
        any call that might be hanged because of a faulty server.
        """
        cnx = None

        with self.__lock:
            cnx = MySQLConnection()
            self._track_connection(server, cnx)

        host, port = split_host_port(server.address, MYSQL_DEFAULT_PORT)
        cnx.connect(
            autocommit=True, host=host, port=port,
            user=server.user, passwd=server.passwd
        )
        return cnx

    def _do_get_connection(self, server):
        """Return a connection from the pool.
        """
        cnx = None
        with self.__lock:
            try:
                cnx = self.__pool[server.uuid].pop()
                self._track_connection(server, cnx)
            except (KeyError, IndexError):
                pass
        return cnx

    def _track_connection(self, server, cnx):
        """Register that a connection is about to be used.
        """
        tracker = self.__tracker.get(server.uuid, [])
        assert cnx not in tracker
        tracker.append(cnx)
        self.__tracker[server.uuid] = tracker
        _LOGGER.debug("Tracking %s %s", str(server.uuid), str(cnx))

    def _untrack_connection(self, server, cnx):
        """Unregister a connection after its use.
        """
        tracker = self.__tracker[server.uuid]
        tracker.remove(cnx)
        if not tracker:
            del self.__tracker[server.uuid]
        _LOGGER.debug("Untracking %s %s", str(server.uuid), str(cnx))

    def get_connection(self, server):
        """Get a connection.

        The method gets a connection from a pool if there is any or
        create a fresh one.
        """
        cnx = self._do_get_connection(server)
        while cnx:
            assert server.user != None and cnx.user == server.user
            if cnx.is_valid_mysql_connection():
                return cnx
            cnx = self._do_get_connection(server)
        return self._do_create_connection(server)

    def release_connection(self, server, cnx):
        """Release a connection to the pool.

        It is up to the developer to check if the connection is still
        valid and belongs to this server before returning it to the
        pool.
        """
        assert cnx is not None
        with self.__lock:
            try:
                self._untrack_connection(server, cnx)
                if server.uuid not in self.__pool:
                    self.__pool[server.uuid] = []
                self.__pool[server.uuid].append(cnx)
            except (KeyError, ValueError) as error:
                pass

    def get_number_connections(self, server):
        """Return the number of connections available in the pool.
        """
        with self.__lock:
            try:
                return len(self.__pool[server.uuid])
            except KeyError:
                pass
        return 0

    def purge_connections(self, server):
        """Close and remove all connections that belongs to a MySQL Server
        which is associated to a server.
        """
        with self.__lock:
            try:
                for cnx in self.__pool[server.uuid]:
                    cnx.shutdown()
                del self.__pool[server.uuid]
            except KeyError:
                pass

            try:
                for cnx in self.__tracker[server.uuid]:
                    cnx.shutdown()
                del self.__tracker[server.uuid]
            except KeyError:
                pass

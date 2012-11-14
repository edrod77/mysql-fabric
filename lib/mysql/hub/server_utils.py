"""Define functions that can be used throughout the code.
"""
import logging
import collections
try:
    import mysql.connector
    import mysql.connector.cursor.MySQLCursor
except ImportError:
    pass

import mysql.hub.errors as _errors

_LOGGER = logging.getLogger(__name__)

MYSQL_DEFAULT_PORT = 3306

def split_host_port(uri, default_port):
    """Return a tuple with host and port.

    If a port is not found in the uri, the default port is returned.
    """
    if uri.find(":") >= 0:
        host, port = uri.split(":")
    else:
        host, port = (uri, default_port)
    return host, port

def combine_host_port(host, port, default_port):
    """Return a string with the parameters host and port.
        
    :return: String host:port.
    """
    if host:
        host_info = host
    else:
        host_info = "unknown host"

    if port:
        port_info = port
    else:
        port_info = default_port

    return "%s:%s" % (host_info, port_info)

try:
    class MySQLCursorNamedTuple(mysql.connector.cursor.MySQLCursor):
        """Create a cursor with named columns.
        """
        def _row_to_python(self, rowdata, desc=None):
            try:
                if not desc:
                    desc = self.description
                to_python = self._connection.converter.to_python
                gen = (to_python(desc[i], v) for i, v in enumerate(rowdata))
                tuple_factory = \
                    collections.namedtuple('Row', self.column_names)._make
                return tuple_factory(gen)
            except StandardError as error:
                raise mysql.connector.errors.InterfaceError(
                    "Failed converting row to Python types; %s" % error)
except NameError:
    pass

def exec_mysql_query(cnx, query_str, options=None):
    """Execute a query for the client and return a result set or a
    cursor.

    This is the singular method to execute queries. It should be the only
    method used as it contains critical error code to catch the issue
    with mysql.connector throwing an error on an empty result set. If
    something goes wrong while executing a statement, the exception
    :class:`mysql.hub.errors.DatabaseError` is raised.

    :param cnx: Database connection.
    :param query_str: The query to execute
    :param options: Options to control behavior:

                    - params - Parameters for query.
                    - columns - If true, return a rows as named tuples
                      (default is False).
                    - raw - If true, use a buffered raw cursor
                      (default is True).
                    - fetch - If true, execute the fetch as part of the
                      operation and use a buffered cursor (default is True).

    :return: Either a result set as list of tuples (either named or unamed)
             or a cursor.
    """
    _LOGGER.debug("Query (%s).", query_str)

    if cnx is None or not cnx.is_connected():
        raise _errors.DatabaseError("Connection is invalid.")

    options = options if options is not None else {}
    params = options.get('params', ())
    columns = options.get('columns', False)
    fetch = options.get('fetch', True)
    raw = options.get('raw', True)

    results = ()
    cur = cnx.cursor(fetch, raw,
                     MySQLCursorNamedTuple if columns else None)

    try:
        cur.execute(query_str, params)
    except mysql.connector.Error as error:
        cur.close()
        raise _errors.DatabaseError(
            "Command (%s, %s) failed: %s" % (query_str, params, str(error)),
            error.errno)
    except Exception as error:
        cur.close()
        raise _errors.DatabaseError(
            "Unknown error. Command: (%s, %s) failed: %s" % (query_str, \
            params, str(error)))

    if fetch or columns:
        try:
            results = cur.fetchall()
        except mysql.connector.errors.InterfaceError as error:
            if error.msg.lower() == "no result set to fetch from.":
                pass # This error means there were no results.
            else:    # otherwise, re-raise error
                raise _errors.DatabaseError(
                    "Error (%s) fetching data for command: (%s)." % \
                    (str(error), query_str))
        cur.close()
        return results
    else:
        return cur

def create_mysql_connection(**kwargs):
    """Create a connection.
    """
    try:
        cnx = mysql.connector.Connect(**kwargs)
        _LOGGER.debug("Created connection (%s).", cnx)
        return cnx
    except mysql.connector.Error as error:
        raise _errors.DatabaseError("Cannot connect to the server. "\
            "Error %s" % (str(error)), error.errno)

def destroy_mysql_connection(cnx):
    """Close the connection.
    """
    try:
        _LOGGER.debug("Destroying connection (%s).", cnx)
        cnx.disconnect()
    except Exception as error:
        raise _errors.DatabaseError("Error tyring to disconnect. "\
                                    "Error %s" % (str(error)))

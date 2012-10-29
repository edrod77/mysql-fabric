"""Define interfaces to manage servers, specifically MySQL Servers.

A server is uniquely identified through a *UUID* (Universally Unique
Identifier) and has a *URI* (Uniform Resource Identifier) which is
used to connect to it through the Python Database API. If a server
process such as MySQL already provides a uuid, the server's concrete
class used to create a MySQL object must ensure that they match
otherwise the different uuids may cause problems in other modules. A
*URI* has the following format: *host:port*.

Any sort of provisioning must not be performed when the server object
is instantiated. The provisioning steps must be done in other modules.

Servers are organized into groups which have unique names. This aims
at defining administrative domains and easing management activities.
In the case of MySQL Servers, one of the servers in the group may
become a master.
"""
import threading
import uuid as _uuid
import logging
import functools

try:
    import mysql.connector
except ImportError:
    pass

import mysql.hub.errors as _errors

_LOGGER = logging.getLogger(__name__)

def server_logging(function):
    """This logs information on functions being called within server
    instances.
    """
    @functools.wraps(function)
    def wrapper_check(*args, **keywargs):
        """Inner function that logs information on wrapped function.
        """
        _LOGGER.debug("Executing function: %s.", function.__name__)
        try:
            ret = function(*args, **keywargs)
        except Exception as error:
            _LOGGER.debug("Error executing function: %s.", function.__name__)
            _LOGGER.exception(error)
            raise
        else:
            _LOGGER.debug("Executed function: %s.", function.__name__)
        return ret
    return wrapper_check


class Group(object):
    """Provide interfaces to organize servers into groups.

    This class does not provide any monitoring feature and this becomes
    necessary one should extend it or rely on an external service.

    """
    def __init__(self, group_id, description=None):
        """Constructor for the Group.
        """
        assert(isinstance(group_id, basestring))
        self.__group_id = group_id
        self.description = description
        self.__lock = threading.RLock()
        self.__servers = {}

    def __eq__(self,  other):
        """Two groups are equal if they have the same id.
        """
        return isinstance(other, Group) and \
               self.__group_id == other.group_id

    def __hash__(self):
        """A group is hashable through its uuid.
        """
        return hash(self.__group_id)

    @property
    def group_id(self):
        """Return the group's id.
        """
        return self.__group_id

    def add_server(self, server):
        """Add a server into this group.
        """
        assert(isinstance(server, Server))
        with self.__lock:
            self.__servers[server.uuid] = server

    def remove_server(self, server):
        """Remove a server from this group.
        """
        assert(isinstance(server, Server))
        with self.__lock:
            if server.uuid in self.__servers:
                del self.__servers[server.uuid]

    @property
    def servers(self):
        """Return the set of servers in this group.

        Specifically, this method returns a copy of the dictionary that
        contains the set of servers in this group.
        """
        with self.__lock:
            return self.__servers.copy()


#TODO: Remove this after pushing the persistence layer HAM-28. Pylint will
#      stop complaining after as there will be more than one class derived
#      from Server.
class Server(object): #pylint: disable=R0922
    """Abstract class used to provide interfaces to access a server.

    Notice that a server may be only a wrapper to a remote server.
    """
    def __init__(self, uuid, uri):
        """Constructor for the Server.

        :param uuid: Uniquely identifies the server.
        :param uri: Used to connect to the server
        """
        assert(isinstance(uuid, _uuid.UUID))
        self.__uuid = uuid
        self.__uri = uri
        self.__available_cnxs = 0
        self.__pool = []
        self.__lock = threading.RLock()

    def _do_connection(self, *args, **kwargs):
        """Create a new connection.

        It is user's responsibility to provide the appropriate arguments
        which vary according to the server type, e.g. MySQL, Oracle.
        """
        raise NotImplementedError("Trying to execute abstract method "\
                                  "connect(*args, **kwargs).")

    def connection(self, *args, **kwargs):
        """Get a connection.

        The method gets a connection from a pool if there is any. Otherwise,
        a new connection is created. The pool does not take into account any
        connection's property to identify the stored connections.
        """
        cnx = None
        with self.__lock:
            if self.__pool:
                cnx = self.__pool.pop()
                self.__available_cnxs -= 1
            else:
                cnx = self._do_connection(*args, **kwargs)
        return cnx

    def release_connection(self, cnx):
        """Release a connection to the pool.

        After using a connection, it should be returned to the pool. It is
        up to the developer to check if the connection is still valid and
        belongs to this server before returning it to the pool.
        """
        assert(cnx is not None)
        with self.__lock:
            self.__pool.append(cnx)
            self.__available_cnxs += 1

    def purge_connections(self):
        """Close and remove all connections from the pool.
        """
        try:
            self.__lock.acquire()
            for cnx in self.__pool:
                cnx.disconnect()
        finally:
            self.__pool = []
            self.__lock.release()

    @property
    def available_connections(self):
        """Return the number of connections available in the pool.
        """
        with self.__lock:
            ret = self.__available_cnxs
        return ret

    def __eq__(self,  other):
        """Two servers are equal if they have the same uuid.
        """
        return isinstance(other, Server) and self.__uuid == other.uuid

    def __hash__(self):
        """A server is hashable through its uuid.
        """
        return hash(self.__uuid)

    @property
    def uuid(self):
        """Return the server's uuid.
        """
        return self.__uuid

    @property
    def uri(self):
        """Return the server's uri.
        """
        return self.__uri

    @staticmethod
    def split_host_port(uri, default_port):
        """Return a tuple with host and port.

        If a port is not found in the uri, the default port is returned.
        """
        if uri is not None and uri.find(":") >= 0:
            host, port = uri.split(":")
        else:
            host, port = (uri, default_port)
        return host, port

    @staticmethod
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


class MySQLResultSet(object):
    """Used to easily iterate through a result set.

    This class defines an iterator and gets as input the names of the columns
    and tuples (i.e. records) where both para tuples. It returns itself as an
    iterator so that cannot be used in nested loops.

    It can be used as follows::

      for record in result_set:
        record.get_value(column)

    where column is either a position or a name.
    """
    def __init__(self, metadata, tuples=None):
        """Constructor for MySQLResultSet.

        :param metadata: Names of columns.
        :type metada: tuple
        :param tuples: List of tuples (i.e. records).
        :type tuples: tuple
        """
        assert(metadata is None or isinstance(metadata, list))
        assert(isinstance(tuples, list))
        self.__metadata = {}
        self.__tuples = tuples
        self.__max_tuples = len(tuples)
        self.__fetch_tuple = 0
        for position, item in enumerate(metadata):
            self.__metadata[item] = position

    def __iter__(self):
        """Return an iterator.
        """
        self.__fetch_tuple = 0
        return self

    def next(self):
        """Return the next record.
        """
        if self.__fetch_tuple >= self.__max_tuples:
            raise StopIteration
        self.__fetch_tuple += 1
        return self

    def get_value(self, column):
        """Enables to get column's value.

        :param: Either column's name or position.
        :return: Column's value.
        """
        if self.__fetch_tuple == 0:
            raise _errors.ResultSetError("Result set is not initialized. "\
                                         "Please, call next().")
        by_name = isinstance(column, str)
        by_pos = isinstance(column, int)
        assert((by_name or by_pos) and (not by_name or not by_pos))
        try:
            pos = column if by_pos else self.__metadata[column]
            return self.__tuples[self.__fetch_tuple - 1][pos]
        except (KeyError, IndexError):
            raise _errors.ResultSetError("Column %s has not been found." % \
                                         (column))


class MySQLServer(Server):
    """Concrete class that provides an interface to access a MySQL Server
    Instance.

    To create a MySQLServer object, one needs to provide at least three
    parameters: uuid, uri (i.e., host:port) and user's name. Most likely,
    a password is also necessary. If the uuid is not known beforehand,
    one can find this out as follows::

      import uuid as _uuid

      OPTIONS = {
        "uuid" : _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
        "uri"  : "localhost:13000",
        "user" : "root",
        "passwd" : ""
      }
      uuid = MySQLServer.discover_uuid(**OPTIONS)
      OPTIONS["uuid"] = _uuid.UUID(uuid)

      server = MySQLServer(**OPTIONS)

    After creating the object, it is necessary to connect it to the MySQL
    Server by explicitly calling connect(). This is required because all the
    necessary information to connect to the server may not have been defined
    at initialization time. For example, one may have created the object
    before reading its state from a persistence layer and setting them.

    So after connecting the object to a server, users may execute statements
    by calling exec_query() as follows::

      server.connect()
      ret = server.exec_query("SELECT VERSION()")
      print "MySQL Server has version", ret[0][0]

    Changing the value of the properties user or password triggers a call to
    disconnect.
    """
    SESSION_CONTEXT, GLOBAL_CONTEXT = range(0, 2)
    CONTEXT_STR = ["SESSION", "GLOBAL"]
    DEFAULT_PORT = 3306

    def __init__(self, uuid, uri, user=None, passwd=None,
                 default_charset="latin1"):
        """Constructor for MySQLServer.
        """
        super(MySQLServer, self).__init__(uuid=uuid, uri=uri)
        self.__user = user
        self.__passwd = passwd
        self.__cnx = None
        self.__default_charset = default_charset
        self.__read_only = None
        self.__server_id = None
        self.__version = None
        self.__gtid_enabled = None
        self.__binlog_enabled = None

    @staticmethod
    @server_logging
    def discover_uuid(**kwargs):
        """Retrieve the uuid from a server.

        :param kwargs: Dictionary with parmaters to connect to a server.
        """
        host = port = None
        params = kwargs.copy()
        keys = params.keys()

        if "uri" in params.keys():
            host, port = MySQLServer.split_host_port(params["uri"],
                                                     MySQLServer.DEFAULT_PORT)
        if host:
            params.setdefault("host", host)
        if port:
            params.setdefault("port", int(port))

        if "uri" in keys:
            del params["uri"]
        if "uuid" in keys:
            del params["uuid"]

        cnx = MySQLServer._create_connection(**params)
        cur = cnx.cursor()
        try:
            cur.execute("SELECT @@GLOBAL.SERVER_UUID")
            server_uuid = cur.fetchall()[0][0]
        except Exception as error:
            raise _errors.DatabaseError(
                "Error trying get server_uuid: %s." % str(error))
        finally:
            cur.close()
            MySQLServer._destroy_connection(cnx)

        return server_uuid

    def connection(self):
        """Override the method connetion defined at Server to avoid that users
        can create different connections to access a MySQL Server.

        Any access to a MySQL Server should be done through exec_query() and
        any other method provided in this class.
        """
        raise _errors.DatabaseError("It is not possible creating a new "\
                                    "connection.")

    def _do_connection(self, **kwargs):
        """Create a new connection.
        """
        cannot_override = ["host", "port", "user", "passwd"]
        wrong_parameters = \
            [key for key in kwargs.keys() if key in cannot_override]
        if wrong_parameters != []:
            raise _errors.ConfigurationError(
                "Option(s) %s cannot be overridden.", wrong_parameters)

        params = kwargs.copy()
        host, port = MySQLServer.split_host_port(self.uri,
                                                 MySQLServer.DEFAULT_PORT)
        if host:
            params["host"] = host
        if port:
            params["port"] = int(port)
        if self.__user:
            params["user"] = self.__user
        if self.__passwd:
            params["passwd"] = self.__passwd

        params.setdefault("charset", self.__default_charset)

        return MySQLServer._create_connection(**params)

    @server_logging
    def connect(self, **kwargs):
        """Connect to a MySQL Server instance.
        """
        # We disconnect first and connect again.
        self.disconnect()

        # Set up an internal connection.
        self.__cnx = super(MySQLServer, self).connection(**kwargs)

        # Get server's uuid
        ret_uuid = self.get_variable("SERVER_UUID")
        ret_uuid = _uuid.UUID(ret_uuid)
        if ret_uuid != self.uuid:
            self.disconnect()
            raise _errors.MismatchUuidError("Uuids do not match "\
              "(stored (%s), read (%s))." % (self.uuid, ret_uuid))

        # Get server's id.
        self.__server_id = int(self.get_variable("SERVER_ID"))

        # Get server's version.
        self.__version = self.get_variable("VERSION")

        # Get information on gtid support.
        if not self.check_version_compat((5, 6, 5)):
            self.__gtid_enabled = False
        else:
            ret_gtid = self.get_variable("GTID_MODE")
            self.__gtid_enabled = ret_gtid in ("ON", "1")

        ret_binlog = self.get_variable("LOG_BIN")
        self.__binlog_enabled = not ret_binlog in ("OFF", "0")

        # Read read_only.
        self._check_read_only()

        _LOGGER.debug("Connected to server with uuid (%s), server_id (%d), " \
                      "version (%s), gtid (%s), binlog (%s), read_only (%s)." \
                      , self.uuid, self.__server_id, self.__version, \
                      self.__gtid_enabled, self.__binlog_enabled, \
                      self.__read_only)

    @server_logging
    def disconnect(self):
        """Disconnect from the server.
        """
        if self.__cnx is not None:
            _LOGGER.debug("Disconnecting from server with uuid (%s), " \
                          "server_id (%s), version (%s), gtid (%s), " \
                          "binlog (%s), read_only (%s).", self.uuid, \
                          self.__server_id, self.__version, \
                          self.__gtid_enabled, self.__binlog_enabled, \
                          self.__read_only)
            try:
                MySQLServer._destroy_connection(self.__cnx)
            finally:
                self.__cnx = None
                self.__read_only = None
                self.__server_id = None
                self.__version = None
                self.__gtid_enabled = None
                self.__binlog_enabled = None
        self.purge_connections()

    def is_alive(self):
        """Determine if connection to server is still alive.

        Ping and is_connected only work partially, try exec_query to make
        sure connection is really alive.
        """
        res = True
        try:
            if self.__cnx is None:
                res = False
            elif self.__cnx.is_connected():
                self.exec_query("SHOW DATABASES")
        except _errors.DatabaseError:
            res = False
        return res

    def _check_read_only(self):
        """Check if the database was set to read-only mode.
        """
        ret_read_only = self.get_variable("READ_ONLY")
        self.__read_only = not ret_read_only in ("OFF", "0")

    @property
    def read_only(self):
        """Check read only mode on/off.
        """
        return self.__read_only

    @read_only.setter
    def read_only(self, enabled):
        """Turn read only mode on/off.
        """
        self.set_variable("READ_ONLY", "ON" if enabled else "OFF")
        self._check_read_only()

    @property
    def server_id(self):
        """Return the server id.
        """
        return self.__server_id

    @property
    def version(self):
        """Return version number of the server.
        """
        return self.__version

    @property
    def gtid_enabled(self):
        """Return if gtid is enabled.
        """
        return self.__gtid_enabled

    @property
    def binlog_enabled(self):
        """Check binary logging status.
        """
        return self.__binlog_enabled

    @property
    def default_charset(self):
        """Return the defualt character set.
        """
        return self.__default_charset

    @property
    def user(self):
        """Return user's name who is used to connect to a server.
        """
        return self.__user

    @user.setter
    def user(self, user):
        """Set user's name who is used to connect to a server.
        """
        if self.__user != user:
            self.disconnect()
            self.__user = user

    @property
    def passwd(self):
        """Return user's password who is used to connect to a server.
        """
        return self.__passwd

    @passwd.setter
    def passwd(self, passwd):
        """Set user's passord who is used to connect to a server.
        """
        if self.__passwd != passwd:
            self.disconnect()
            self.__passwd = passwd

    def check_version_compat(self, expected_version):
        """Check version of the server against requested version.

        This method can be used to check for version compatibility.

        :param expected_version: Target server version.
        :type expected_version: (major, minor, release)
        :return: True if server version is GE (>=) version specified,
                 False if server version is LT (<) version specified.
        :rtype: Bool
        """
        index = self.__version.find("-")
        version_str = self.__version[0 : index] \
            if self.__version.find("-") >= 0 else self.__version
        version = tuple(int(part) for part in version_str.split("."))
        return version >= expected_version

    def get_gtid_status(self):
        """Get the GTID information for the server.

        This method attempts to retrieve the GTID lists. If the server
        does not have GTID turned on or does not support GTID, the method
        will throw the exception DatabaseError.

        :return: A MySQLResultSet with GTID information.
        """
        # Check servers for GTID support
        if not self.__gtid_enabled:
            raise _errors.DatabaseError("Global Transaction IDs are not "\
                                        "supported.")

        query_str = (
            "SELECT @@GLOBAL.GTID_DONE as GTID_DONE, "
            "@@GLOBAL.GTID_LOST as GTID_LOST, "
            "@@GLOBAL.GTID_OWNED as GTID_OWNED"
        )

        ret = self.exec_query(query_str, {"columns" : True})
        return MySQLResultSet(ret[0], ret[1])

    def has_storage_engine(self, target):
        """Check to see if an engine exists and is supported.

        :param target: Name of engine to find.
        :return: True if engine exists and is active. False if it does
                 not exist or is not supported/not active/disabled.
        """
        if len(target) == 0:
            return True # This says we will use default engine on the server.

        query_str = (
            "SELECT UPPER(engine) as engine, UPPER(support) as support "
            "FROM INFORMATION_SCHEMA.ENGINES"
        )

        if target:
            engines = self.exec_query(query_str)
            for engine in engines:
                if engine[0].upper() == target.upper() and \
                   engine[1].upper() in ['YES', 'DEFAULT']:
                    return True
        return False

    def get_binary_logs(self, options=None):
        """Return information on the binary logs.

        :param options: Query options.
        :return: A MySQLResultSet with information on the binary logs.
        """
        options = options if options is not None else {}
        options.update({"columns": True})
        ret = self.exec_query("SHOW BINARY LOGS", options)
        return MySQLResultSet(ret[0], ret[1])

    def set_session_binlog(self, enabled=True):
        """Enable or disable binary logging for the client.

        Note: user must have SUPER privilege

        :param disable: If 'disable', turn off the binary log
                        otherwise turn binary log on.
        """
        self.set_variable("SQL_LOG_BIN", "ON" if enabled else "OFF",
                          MySQLServer.SESSION_CONTEXT)

    def session_binlog_enabled(self):
        """Check if binary logging is enabled for the client.
        """
        ret = self.get_variable("SQL_LOG_BIN",
                                MySQLServer.SESSION_CONTEXT)
        return ret in ["ON", '1']

    def foreign_key_checks_enabled(self):
        """Check foreign key status for the client.
        """
        ret = self.get_variable("FOREIGN_KEY_CHECKS",
                                MySQLServer.SESSION_CONTEXT)
        return ret in ["ON", '1']

    def set_foreign_key_checks(self, enabled=True):
        """Enable or disable foreign key checks for the client.

        :param disable: If True, turn off foreign key checks otherwise turn
                        foreign key checks on.
        """
        self.set_variable("FOREIGN_KEY_CHECKS", "ON" if enabled else "OFF",
                          MySQLServer.SESSION_CONTEXT)

    def get_variable(self, variable, context=None):
        """Execute the SELECT command for the client and return a result set.
        """
        context_str = MySQLServer.CONTEXT_STR[\
            context if context is not None else MySQLServer.GLOBAL_CONTEXT]
        ret = self.exec_query("SELECT @@%s.%s as %s" % \
                              (context_str, variable, variable))
        return ret[0][0]

    def set_variable(self, variable, value, context=None):
        """Execute the SET command for the client and return a result set.
        """
        context_str = MySQLServer.CONTEXT_STR[\
            context if context is not None else MySQLServer.GLOBAL_CONTEXT]
        return self.exec_query("SET @@%s.%s = %s" \
                               % (context_str, variable, value))

    @server_logging
    def exec_query(self, query_str, options=None):
        """Execute a query for the client and return a result set or a
        cursor.

        This is the singular method to execute queries. It should be the only
        method used as it contains critical error code to catch the issue
        with mysql.connector throwing an error on an empty result set.

        Note: will handle exception and print error if query fails

        Note: if fetchall is False, the method returns the cursor instance

        :param query_str: The query to execute
        :param options: Options to control behavior:

        - params - Parameters for query.
        - columns - Add column headings as first row (default is False).
        - fetch - Execute the fetch as part of the operation and use a
                  buffered cursor (default is True)
        - raw - If True, use a buffered raw cursor (default is True)

        It returns a result set or a cursor.
        """
        _LOGGER.debug("Query (%s).", query_str)

        if self.__cnx is None or not self.__cnx.is_connected():
            raise _errors.DatabaseError("Connection is invalid.")

        options = options if options is not None else {}
        params = options.get('params', ())
        columns = options.get('columns', False)
        fetch = options.get('fetch', True)
        raw = options.get('raw', True)

        results = ()
        cur = self.__cnx.cursor(fetch, raw)

        try:
            cur.execute(query_str, params)
        except mysql.connector.Error as error:
            cur.close()
            raise _errors.DatabaseError(
                "Command (%s) failed: %s" % (query_str, str(error)),
                error.errno)
        except Exception as error:
            cur.close()
            raise _errors.DatabaseError(
                "Unknown error. Command: (%s) failed: %s" % (query_str), \
                str(error))

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
            if columns:
                col_headings = cur.column_names
                col_names = []
                for col in col_headings:
                    col_names.append(col)
                results = col_names, results
            cur.close()
            self.__cnx.commit()
            return results
        else:
            return cur

    def __del__(self):
        """Destructor for MySQLServer.
        """
        self.disconnect()

    @staticmethod
    @server_logging
    def _create_connection(**kwargs):
        """Create a connection.
        """
        try:
            cnx = mysql.connector.Connect(**kwargs)
            _LOGGER.debug("Created connection (%s).", cnx)
            return cnx
        except mysql.connector.Error as error:
            raise _errors.DatabaseError("Cannot connect to the server. "\
                "Error %s" % (str(error)), error.errno)

    @staticmethod
    @server_logging
    def _destroy_connection(cnx):
        """Close the connection.
        """
        try:
            _LOGGER.debug("Destroying connection (%s).", cnx)
            cnx.disconnect()
        except Exception as error:
            raise _errors.DatabaseError("Error tyring to disconnect. "\
                                        "Error %s" % (str(error)))

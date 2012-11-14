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

import mysql.hub.errors as _errors
import mysql.hub.utils as _utils
import mysql.hub.persistence as _persistence
import mysql.hub.server_utils as _server_utils

_LOGGER = logging.getLogger(__name__)

def server_logging(function):
    """This logs information on functions being called within server
    instances.
    """
    @functools.wraps(function)
    def wrapper_check(*args, **keywargs):
        """Inner function that logs information on wrapped function.
        """
        _LOGGER.debug("Start executing function: %s.", function.__name__)
        try:
            ret = function(*args, **keywargs)
        except Exception as error:
            _LOGGER.debug("Error executing function: %s.", function.__name__)
            _LOGGER.exception(error)
            raise
        else:
            _LOGGER.debug("Finish executing function: %s.", function.__name__)
        return ret
    return wrapper_check


class Group(_persistence.Persistable):
    """Provide interfaces to organize servers into groups.

    This class does not provide any monitoring feature and this becomes
    necessary one should extend it or rely on an external service.
    """
    CREATE_GROUP = ("CREATE TABLE groups"
                        "(group_id VARCHAR(64) NOT NULL, "
                        "description VARCHAR(256), "
                        "CONSTRAINT pk_group_id PRIMARY KEY (group_id))")

    #SQL Statement for creating the table for storing the relationship
    #between a group and the server.

    #TODO: DEFINE FOREIGN KEY constraints.
    CREATE_GROUP_SERVER = \
                ("CREATE TABLE group_server"
                 "(server_uuid VARCHAR(40) NOT NULL, "
                 "group_id VARCHAR(64) NOT NULL, "
                 "CONSTRAINT pk_server_uuid_group_uuid "
                 "PRIMARY KEY(group_id, server_uuid))")

    #SQL Statements for dropping the table created for storing the Group
    #information
    DROP_GROUP = ("DROP TABLE groups")

    #SQL Statements for dropping the table used for storing the relation
    #between Group and the servers
    DROP_GROUP_SERVER = ("DROP TABLE group_server")

    #SQL statement for inserting a new group into the table
    INSERT_GROUP = ("INSERT INTO groups VALUES(%s, %s)")

    #SQL statement for inserting a new server into a group
    INSERT_GROUP_SERVER = ("INSERT INTO group_server VALUES(%s, %s)")

    #SQL statement for checking for the presence of a server within a group
    QUERY_GROUP_SERVER = ("SELECT server_uuid from group_server where "
                          "group_id = %s AND server_uuid = %s")

    #SQL statement for selecting all groups
    QUERY_GROUPS = ("SELECT group_id from groups")

    #SQL statement for selecting all the servers from a group
    QUERY_GROUP_SERVERS = ("SELECT server_uuid from group_server where "
                           "group_id = %s")

    #SQL statement for updating the group table identified by the group id.
    UPDATE_GROUP = ("UPDATE groups SET description = %s "
                    "WHERE group_id = %s")

    #SQL statement used for deleting the group identified by the group id.
    REMOVE_GROUP = ("DELETE FROM groups WHERE group_id = %s")

    #SQL Statement to delete a server from a group.
    DELETE_GROUP_SERVER = ("DELETE FROM group_server WHERE group_id = %s AND "
                           "server_uuid = %s")

    #SQL Statement to retrieve a specific group from the state_store.
    QUERY_GROUP = ("SELECT group_id, description FROM groups WHERE "
                   "group_id = %s")


    def __init__(self, persister, group_id, description=None):
        """Constructor for the Group. Check to see if the Group is already
        present in the state store, if it is, then load the information from
        the state store, else persist the input information into the state
        store.

        :param persister The server that is used to store the
                                   group information.
        :param group_id The id that uniquely identifies the group
        :param description The description of the group
        """
        assert(isinstance(group_id, basestring))
        if persister is None:
            raise _error.PersistenceError("Missing handle to the state store")
        self.__persister = persister
        self.__group_id = group_id
        self.__description = description

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

        :param The Server object that needs to be added to this Group.
        """
        assert(isinstance(server, Server))
        self.__persister.exec_query (Group.INSERT_GROUP_SERVER,
                                              {"params":(str(server.uuid),
                                                   self.__group_id)})

    def remove_server(self, server):
        """Remove a server from this group.

        :param The Server object that needs to be removed from this Group.
        """
        assert(isinstance(server, Server))
        self.__persister.exec_query(Group.DELETE_GROUP_SERVER,
                                             {"params":(str(self.__group_id),
                                                   str(server.uuid))})

    @property
    def description(self):
        """Return the description for the group.
        """
        return self.__description

    @description.setter
    def description(self, description):
        """Set the description for this group. Update the description for the
        Group in the state store.

        :param description The new description for the group that needs to be
                           updated.
        """
        self.__persister.exec_query(Group.UPDATE_GROUP,
                                    {"params":(description, self.__group_id)})
        self.__description = description

    def servers(self):
        """Return the uuids for the set of servers in this group.
        """
        cur = self.__persister.exec_query(Group.QUERY_GROUP_SERVERS,
                                {"raw" : False,
                                "fetch" : False,
                                "params" : (self.__group_id,)})
        rows = cur.fetchall()
        return rows

    @staticmethod
    def groups(persister):
        """Return the group_ids of all the available groups.
        """
        return persister.exec_query(Group.QUERY_GROUPS,
                                    {"raw" : False, "fetch" : True})

    def remove(self):
        """Remove the Group object from the state store.
        """
        self.__persister.exec_query(Group.REMOVE_GROUP,
                                             {"params" : (self.__group_id,)})

    def contains_server(self, uuid):
        """Check if the server represented by the uuid is part of the
        current Group.

        :param uuid The uuid of the server whose membership needs to be
                            verified.
        :return True if the server is part of the Group.
                False if the server is not part of the Group.
        """
        cur = self.__persister.exec_query(Group.QUERY_GROUP_SERVER,
                                            {"raw" : False, "fetch" : False,
                                            "params":(self.__group_id,
                                                      str(uuid))})
        row = cur.fetchone()

        if row:
            return True
        else:
            return False

    @staticmethod
    def fetch(persister, group_id):
        """Return the group object, by loading the attributes for the group_id
        from the state store.

        :param persister: The persistence store object that can be used
                          to access the state store.
        :param group_id The group_id for the Group object that needs to be
                        retrieved.
        :return The Group object corresponding to the group_id
                None if the Group object does not exist.
        """
        cur = persister.exec_query(Group.QUERY_GROUP,
                                            {"raw" : False, \
                                            "fetch" : False, \
                                            "params" : (group_id,)})
        row = cur.fetchone()
        if row:
            return Group(persister, row[0], row[1])

    @staticmethod
    def add(persister, group_id, description):
        """Create a Group and return the Group object.

        :param persister The DB server that can be used to access the
                                    state store.
        """
        persister.exec_query(Group.INSERT_GROUP, {"params":
                                                           (group_id,
                                                            description)})
        return Group(persister, group_id, description)


    @staticmethod
    def create(persister):
        """Create the objects(tables) that will store the Group information in
        the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_query(Group.CREATE_GROUP)
        try:
            persister.exec_query(Group.CREATE_GROUP_SERVER)
        except:
            #If the creation of the second table fails Drop the first
            #table.
            persister.exec_query(Group.DROP_GROUP)
            raise


    @staticmethod
    def drop(persister):
        """Drop the objects(tables) that represent the Group information in
        the persistent store.

        :param persister: The DB server that can be used to access the
                          state store.
        :raises: DatabaseError If the drop of the related table fails.
        """
        persister.exec_query(Group.DROP_GROUP_SERVER)
        persister.exec_query(Group.DROP_GROUP)


class Server(_persistence.Persistable):
    """Abstract class used to provide interfaces to access a server.

    Notice that a server may be only a wrapper to a remote server.
    """
    #TODO: Check if uri is the correct term.
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
        raise NotImplementedError("Trying to execute abstract method "
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

    def exec_query(self, query_str, options=None):
        """Execute statements against the server.
        """
        raise NotImplementedError("Trying to execute abstract method "
                                  "exec_query.")

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
    #SQL Statement for creating the table used to store details about the
    #server.
    CREATE_SERVER = ("CREATE TABLE "
                        "servers "
                        "(server_uuid VARCHAR(40) NOT NULL, "
                        "server_uri VARCHAR(128), "
                        "user CHAR(16), "
                        "passwd TEXT, "
                        "CONSTRAINT pk_server_uuid PRIMARY KEY (server_uuid))")

    #SQL Statement for dropping the table used to store the details about the
    #server.
    DROP_SERVER = ("DROP TABLE servers")

    #SQL statement for inserting a new server into the table
    INSERT_SERVER = ("INSERT INTO servers values(%s, %s, %s, %s)")

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_USER = ("UPDATE servers SET user = %s WHERE server_uuid = %s")
    UPDATE_SERVER_PASSWD = ("UPDATE servers SET passwd = %s "
                            "WHERE server_uuid = %s")

    #SQL statement used for deleting the server identified by the server id.
    REMOVE_SERVER = ("DELETE FROM servers WHERE server_uuid = %s")

    #SQL Statement to retrieve the server from the state_store.
    QUERY_SERVER = ("SELECT server_uuid, server_uri, user, passwd FROM "
                    "servers where server_uuid = %s")

    SESSION_CONTEXT, GLOBAL_CONTEXT = range(0, 2)
    CONTEXT_STR = ["SESSION", "GLOBAL"]

    def __init__(self, persister, uuid, uri=None, user=None,
                 passwd=None, default_charset="latin1"):
        """Constructor for MySQLServer. The constructor searches for the uuid
        in the state store and if the uuid is present it loads the server from
        the state store. otherwise it creates and persists a new Server object.

        :param persister The DB server object that will be used to
                                    access the state store.
        :param uuid The uuid of the server
        :param uri  The uri of the server
        :param user The username used to access the server
        :param passwd The password used to access the server
        :param default_charset The default charset that will be used
        """
        super(MySQLServer, self).__init__(uuid=uuid, uri=uri)
        if persister is None:
            raise _error.PersistenceError("Missing handle to the state store")
        self.__persister = persister
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

        if "uri" in keys:
            host, port = _server_utils.split_host_port(params["uri"],
                _server_utils.MYSQL_DEFAULT_PORT)
            params.setdefault("host", host)
            params.setdefault("port", int(port))
            del params["uri"]

        if "uuid" in keys:
            del params["uuid"]

        cnx = _server_utils.create_mysql_connection(**params)
        cur = cnx.cursor()
        try:
            cur.execute("SELECT @@GLOBAL.SERVER_UUID")
            server_uuid = cur.fetchall()[0][0]
        except Exception as error:
            raise _errors.DatabaseError(
                "Error trying get server_uuid: %s." % str(error))
        finally:
            cur.close()
            _server_utils.destroy_mysql_connection(cnx)

        return server_uuid

    def connection(self):
        """Override the method connection defined at Server to avoid that users
        can create different connections to access a MySQL Server.

        Any access to a MySQL Server should be done through exec_query() and
        any other method provided in this class.
        """
        raise _errors.DatabaseError("It is not possible to create a new "
                                    "connection.")

    def _do_connection(self, **kwargs):
        """Create a new connection.
        """
        cannot_override = ["host", "port", "user", "passwd"]
        wrong_parameters = \
            [key for key in kwargs.keys() if key in cannot_override]
        if wrong_parameters:
            raise _errors.ConfigurationError(
                "Option(s) %s cannot be overridden.", wrong_parameters)

        params = kwargs.copy()
        host, port = _server_utils.split_host_port(self.uri,
                                            _server_utils.MYSQL_DEFAULT_PORT)
        params["host"] = host
        params["port"] = int(port)
        if self.__user:
            params["user"] = self.__user
        if self.__passwd:
            params["passwd"] = self.__passwd
        params.setdefault("autocommit", True)
        params.setdefault("charset", self.__default_charset)

        return _server_utils.create_mysql_connection(**params)

    @server_logging
    def connect(self, **kwargs):
        """Connect to a MySQL Server instance.
        """
        # TODO: We need to revisit how the connection pool is implemented.
        # The current design assumes a pool per object. However, after some
        # discussions on the persistence layer, I think there should be a
        # single pool shared by all objects.

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
                _server_utils.destroy_mysql_connection(self.__cnx)
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

        :return True If read_only is set
                False If read_only is not set.
        """
        return self.__read_only

    @read_only.setter
    def read_only(self, enabled):
        """Turn read only mode on/off. Persist the information in the state
        store.

        :param enabled The read_only flag value.
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
        """Set user's name who is used to connect to a server. Persist the
        user information in the state store.

        :param user The user name.
        """
        if self.__user != user:
            self.disconnect()
            self.__persister.exec_query(MySQLServer.UPDATE_SERVER_USER,
                                        {"params":(user, str(self.uuid))})
            self.__user = user

    @property
    def passwd(self):
        """Return user's password who is used to connect to a server. Load
        the server information from the state store and return the password.
        """
        return self.__passwd

    @passwd.setter
    def passwd(self, passwd):
        """Set user's passord who is used to connect to a server. Persist the
        password information in the state store.

        :param passwd The password that needs to be set.
        """
        if self.__passwd != passwd:
            self.disconnect()
            self.__persister.exec_query(
                                    MySQLServer.UPDATE_SERVER_PASSWD,
                                    {"params":(passwd, str(self.uuid))})
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

        :return: A named tuple with GTID information.

        In order to access the result set one may do what follows::

          ret = server.get_gtid_status()
          for record in ret:
            print "GTID_DONE", record.GTID_DONE, record[0]
            print "GTID_LOST", record.GTID_LOST, record[1]
            print "GTID_OWNED", record_GTID_OWNED, record[2]
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

        return self.exec_query(query_str, {"columns" : True})

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

    def get_binary_logs(self):
        """Return information on binary logs. Look up `SHOW BINARY LOGS` in
        the MySQL Manual for further details.

        :return: A named tuple with information on binary logs.
        """
        return self.exec_query("SHOW BINARY LOGS", {"columns" : True})

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

    def exec_query(self, query_str, options=None):
        return _server_utils.exec_mysql_query(self.__cnx, query_str, options)

    def __del__(self):
        """Destructor for MySQLServer.
        """
        try:
            self.disconnect()
        except AttributeError:
            pass

    def remove(self):
        """remove the server information from the persistent store.
        """
        self.__persister.exec_query(MySQLServer.REMOVE_SERVER,
                                             {"params":
                                              (str(self.uuid),)})

    @staticmethod
    def fetch(persister, uuid):
        """Return the server object corresponding to the uuid.

        :param persister The persistence server object that will be
                                    used to access the state store.
        :param uuid The server id of the server object that needs to be
                            returned.
        :return The server object that corresponds to the server id
                None if the server id does not exist.
        """
        cur = persister.exec_query(MySQLServer.QUERY_SERVER,
                                            {"raw" : False, "fetch" : False,
                                            "params":(str(uuid),)})
        row = cur.fetchone()
        if row:
            return MySQLServer(persister, _uuid.UUID(row[0]), row[1],
                               row[2], row[3])

    @staticmethod
    def create(persister):
        """Create the objects(tables) that will store the Server information in
        the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_query(MySQLServer.CREATE_SERVER)

    @staticmethod
    def drop(persister):
        """Drop the objects(tables) that represent the Server information in
        the persistent store.

        :param persister: The DB server that can be used to access the
                          state store.
        :raises: DatabaseError If the drop of the related table fails.
        """
        persister.exec_query(MySQLServer.DROP_SERVER)

    @staticmethod
    def add(persister, uuid, uri=None, user=None, passwd=None,
            default_charset="latin1"):
        """Persist the Server information and return the Server object.

        :param uuid The uuid of the server being created
        :param uri  The uri  of the server being created
        :param user The user name to be used for logging into the server
        :param passwd The password to be used for logging into the server
        :return a Server object
        """
        persister.exec_query(MySQLServer.INSERT_SERVER,
                                      {"params":(str(uuid),
                                                 uri,
                                                 user,
                                                 passwd)})
        return MySQLServer(persister, uuid, uri, user, passwd,
                           default_charset)

"""Define interfaces to manage servers, specifically MySQL Servers.

A server is uniquely identified through a *UUID* (Universally Unique
Identifier) and has an *Address* (i.e., Hostname:Port) which is
used to connect to it through the Python Database API. If a server
process such as MySQL already provides a uuid, the server's class
used to create a MySQL object must ensure that they match otherwise
the different uuids may cause problems in other modules.

Any sort of provisioning must not be performed when the server object
is instantiated. The provisioning steps must be done in other modules.

Servers are organized into groups which have unique names. This aims
at defining administrative domains and easing management activities.
In the case of MySQL Servers whose version is lower or equal to 5.6,
one of the servers in the group may become a master.
"""
import threading
import uuid as _uuid
import logging
import functools

import mysql.hub.errors as _errors
import mysql.hub.persistence as _persistence
import mysql.hub.server_utils as _server_utils
import mysql.hub.utils as _utils
import mysql.hub.failure_detector as _detector

_LOGGER = logging.getLogger(__name__)

# TODO: Improve this function and make it generic.
def server_logging(function):
    """This logs information on functions being called within server
    instances.
    """
    @functools.wraps(function)
    def wrapper_check(*args, **kwrds):
        """Inner function that logs information on wrapped function.
        """
        _LOGGER.debug("Start executing function: %s(%s).", function.__name__,
                      str(kwrds))
        try:
            ret = function(*args, **kwrds)
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

    :param group_id: The id that uniquely identifies the group.
    :param description: The group's description.
    :param master: The master's uuid in the group.
    :rtype master: UUID
    :param status: Group's status.
    :rtype status: ACTIVE or INACTIVE.
    """
    CREATE_GROUP = ("CREATE TABLE groups"
                    "(group_id VARCHAR(64) NOT NULL, "
                    "description VARCHAR(256), "
                    "master_uuid VARCHAR(40), "
                    "status BIT(1) NOT NULL,"
                    "CONSTRAINT pk_group_id PRIMARY KEY (group_id))")

    #Create the referential integrity constraint with the servers table
    ADD_FOREIGN_KEY_CONSTRAINT_MASTER_UUID = \
                                ("ALTER TABLE groups "
                                  "ADD CONSTRAINT fk_master_uuid_groups "
                                  "FOREIGN KEY(master_uuid) REFERENCES "
                                  "servers(server_uuid)")

    #Drop the referential integrity constraint with the servers table
    DROP_FOREIGN_KEY_CONSTRAINT_MASTER_UUID = \
                                ("ALTER TABLE groups "
                                "DROP FOREIGN KEY fk_master_uuid_groups")

    #SQL Statement for creating the table for storing the relationship
    #between a group and the server.
    CREATE_GROUP_SERVER = \
                ("CREATE TABLE groups_servers"
                 "(group_id VARCHAR(64) NOT NULL, "
                 "server_uuid VARCHAR(40) NOT NULL, "
                 "CONSTRAINT pk_group_id_server_uuid "
                 "PRIMARY KEY(group_id, server_uuid), "
                 "INDEX idx_server_uuid (server_uuid))")

    #Create the referential integrity constraint with the groups table
    ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID = \
                                 ("ALTER TABLE groups_servers "
                                  "ADD CONSTRAINT fk_group_id_groups_servers "
                                  "FOREIGN KEY(group_id) REFERENCES "
                                  "groups(group_id)")

    #Drop the referential integrity constraint with the groups table
    DROP_FOREIGN_KEY_CONSTRAINT_GROUP_ID = \
                                  ("ALTER TABLE groups_servers "
                                   "DROP FOREIGN KEY "
                                   "fk_group_id_groups_servers")

    #Create the referential integrity constraint with the servers table
    ADD_FOREIGN_KEY_CONSTRAINT_SERVER_UUID = \
                                  ("ALTER TABLE groups_servers "
                                  "ADD CONSTRAINT "
                                  "fk_server_uuid_groups_servers "
                                  "FOREIGN KEY(server_uuid) REFERENCES "
                                  "servers(server_uuid)")

    #Drop the referential integrity constraint with the servers table
    DROP_FOREIGN_KEY_CONSTRAINT_SERVER_UUID = \
                               ("ALTER TABLE groups_servers "
                                "DROP FOREIGN KEY "
                                "fk_server_uuid_groups_servers")

    #SQL Statements for dropping the table created for storing the Group
    #information
    DROP_GROUP = ("DROP TABLE groups")

    #SQL Statements for dropping the table used for storing the relation
    #between Group and the servers
    DROP_GROUP_SERVER = ("DROP TABLE groups_servers")

    #SQL statement for inserting a new group into the table
    INSERT_GROUP = ("INSERT INTO groups(group_id, description, status) "
                    "VALUES(%s, %s, %s)")

    #SQL statement for inserting a new server into a group
    INSERT_GROUP_SERVER = ("INSERT INTO groups_servers(group_id, server_uuid) "
                           "VALUES(%s, %s)")

    #SQL statement for checking for the presence of a server within a group
    QUERY_GROUP_SERVER_EXISTS = ("SELECT server_uuid FROM groups_servers "
                                 "WHERE group_id = %s AND server_uuid = %s")

    #SQL statement for selecting all groups
    QUERY_GROUPS = ("SELECT group_id FROM groups")

    #SQL statement for selecting all the servers from a group
    QUERY_GROUP_SERVERS = ("SELECT server_uuid FROM groups_servers WHERE "
                           "group_id = %s")

    #SQL statement for getting group(s) which the server belongs to.
    QUERY_GROUP_FROM_SERVER = ("SELECT group_id FROM groups_servers WHERE "
                               "server_uuid = %s")

    #SQL statement for updating the group table identified by the group id.
    UPDATE_GROUP = ("UPDATE groups SET description = %s WHERE group_id = %s")

    #SQL statement used for deleting the group identified by the group id.
    REMOVE_GROUP = ("DELETE FROM groups WHERE group_id = %s")

    #SQL Statement to delete a server from a group.
    DELETE_GROUP_SERVER = ("DELETE FROM groups_servers WHERE group_id = %s AND "
                           "server_uuid = %s")

    #SQL Statement to retrieve a specific group from the state_store.
    QUERY_GROUP = ("SELECT group_id, description, master_uuid, status "
                   "FROM groups WHERE group_id = %s")

    #SQL Statement to update the group's master.
    UPDATE_MASTER = ("UPDATE groups SET master_uuid = %s WHERE group_id = %s")

    #SQL Statement to update the group's status.
    UPDATE_STATUS = ("UPDATE groups SET status = %s WHERE group_id = %s")

    #Group's statuses
    INACTIVE, ACTIVE = range(0, 2)

    #List with Group's statuses
    GROUP_STATUS = [INACTIVE, ACTIVE]

    def __init__(self, group_id, description=None, master=None,
                 status=INACTIVE):
        """Constructor for the Group.
        """
        assert(isinstance(group_id, basestring))
        assert(master is None or isinstance(master, _uuid.UUID))
        assert(status in Group.GROUP_STATUS)
        super(Group, self).__init__()
        self.__group_id = group_id
        self.__description = description
        self.__master = master
        self.__status = status

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

    def add_server(self, server, persister=None):
        """Add a server into this group.

        :param persister: The DB server that can be used to access the
                          state store.
        :param server: The Server object that needs to be added to this
                       Group.
        """
        assert(isinstance(server, MySQLServer))
        persister.exec_stmt(Group.INSERT_GROUP_SERVER,
                            {"params": (self.__group_id, str(server.uuid))})

    def remove_server(self, server, persister=None):
        """Remove a server from this group.

        :param persister: The DB server that can be used to access the
                          state store.
        :param server: The Server object that needs to be removed from this
                       Group.
        """
        assert(isinstance(server, MySQLServer))
        persister.exec_stmt(Group.DELETE_GROUP_SERVER,
                            {"params":(self.__group_id, str(server.uuid))})

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the tables in this Group.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
                Group.ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID)
        persister.exec_stmt(
                Group.ADD_FOREIGN_KEY_CONSTRAINT_SERVER_UUID)
        persister.exec_stmt(
                Group.ADD_FOREIGN_KEY_CONSTRAINT_MASTER_UUID)
        return True

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the constraints on the tables in this Group.

        :param persister: The DB server that can be used to access the
                  state store.
        """
        persister.exec_stmt(Group.DROP_FOREIGN_KEY_CONSTRAINT_GROUP_ID)
        persister.exec_stmt(Group.DROP_FOREIGN_KEY_CONSTRAINT_SERVER_UUID)
        persister.exec_stmt(Group.DROP_FOREIGN_KEY_CONSTRAINT_MASTER_UUID)
        return True

    @property
    def description(self):
        """Return the description for the group.
        """
        return self.__description

    @description.setter
    def description(self, description=None, persister=None):
        """Set the description for this group. Update the description for the
        Group in the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :param description: The new description for the group that needs to be
                            updated.
        """
        persister.exec_stmt(Group.UPDATE_GROUP,
            {"params":(description, self.__group_id)})
        self.__description = description

    def servers(self, persister=None):
        """Return a set with the servers in this group.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        ret = set()
        rows = persister.exec_stmt(Group.QUERY_GROUP_SERVERS,
                                   {"params" : (self.__group_id,)})
        for row in rows:
            server = MySQLServer.fetch(_uuid.UUID(row[0]))
            try:
                server.connect()
            except _errors.DatabaseError:
                pass
            ret.add(server)
        return ret

    @property
    def master(self):
        """Return the master for the group.
        """
        return self.__master

    @master.setter
    def master(self, master, persister=None):
        """Set the master for this group.

        :param persister: The DB server that can be used to access the
                          state store.
        :param master: The master for the group that needs to be updated.
        """
        assert(master is None or isinstance(master, _uuid.UUID))
        if not master:
            param_master = None
        else:
            param_master = str(master)

        persister.exec_stmt(Group.UPDATE_MASTER,
            {"params":(param_master, self.__group_id)})
        self.__master = master

    @property
    def status(self):
        """Return the group's status.
        """
        return self.__status

    @status.setter
    def status(self, status, persister=None):
        """Set the group's status.

        :param status: The new group's status.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        assert(status in Group.GROUP_STATUS)
        persister.exec_stmt(Group.UPDATE_STATUS,
            {"params":(status, self.__group_id)})
        self.__status = status

    @staticmethod
    def groups(persister=None):
        # TODO: This must be changed. This must return a set of groups.
        """Return the group_ids of all the available groups.

        :param persister: Persister to persist the object to.
        """
        return persister.exec_stmt(Group.QUERY_GROUPS)

    @staticmethod
    def group_from_server(uuid, persister=None):
        """Return the group which a server belongs to.

        :param uuid: Server's uuid.
        :return: Group or None.
        """
        cur = persister.exec_stmt(
            Group.QUERY_GROUP_FROM_SERVER,
            {"fetch" : False, "params" : (str(uuid), )}
            )
        row = cur.fetchone()

        if row:
            return Group.fetch(row[0])

    @staticmethod
    def remove(group, persister=None):
        """Remove the group object from the state store.

        :param group: A reference to a group.
        :param persister: Persister to persist the object to.
        """
        persister.exec_stmt(
            Group.REMOVE_GROUP, {"params" : (group.group_id, )}
            )

    def contains_server(self, uuid, persister=None):
        """Check if the server represented by the uuid is part of the
        current Group.

        :param uuid: The uuid of the server whose membership needs to be
                     verified.
        :param persister: Persister to persist the object to.
        :return: True if the server is part of the Group.
                 False if the server is not part of the Group.
        """
        assert isinstance(uuid, (_uuid.UUID, basestring))
        cur = persister.exec_stmt(Group.QUERY_GROUP_SERVER_EXISTS,
            {"fetch" : False, "params":(self.__group_id, str(uuid))})
        row = cur.fetchone()

        if row:
            return True
        else:
            return False

    @staticmethod
    def fetch(group_id, persister=None):
        """Return the group object, by loading the attributes for the group_id
        from the state store.

        :param group_id: The group_id for the Group object that needs to be
                         retrieved.
        :param persister: Persister to persist the object to.
        :return: The Group object corresponding to the group_id
                 None if the Group object does not exist.
        """
        group = None
        cur = persister.exec_stmt(
            Group.QUERY_GROUP, {"fetch" : False, "raw" : False,
            "params" : (group_id,)}
            )
        row = cur.fetchone()
        if row:
            group_id, description, master, status = row
            if master:
                master = _uuid.UUID(master)
            group = Group(
                group_id=group_id, description=description, master=master,
                status=status
                )
        return group

    @staticmethod
    def add(group, persister=None):
        """Write a group object into the state store.

        :param group: A reference to a group.
        :param persister: Persister to persist the object to.
        """
        persister.exec_stmt(Group.INSERT_GROUP,
            {"params": (group.group_id, group.description, group.status)}
            )

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that will store the Group information in
        the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :raises: DatabaseError If the table already exists.
        """
        persister.exec_stmt(Group.CREATE_GROUP)
        try:
            persister.exec_stmt(Group.CREATE_GROUP_SERVER)
        except _errors.DatabaseError:
            #If the creation of the second table fails Drop the first
            #table.
            persister.exec_stmt(Group.DROP_GROUP)
            raise

    @staticmethod
    def drop(persister=None):
        """Drop the objects(tables) that represent the Group information in
        the persistent store.

        :param persister: The DB server that can be used to access the
                          state store.
        :raises: DatabaseError If the drop of the related table fails.
        """
        _detector.FailureDetector.unregister_groups()
        persister.exec_stmt(Group.DROP_GROUP_SERVER)
        persister.exec_stmt(Group.DROP_GROUP)

class ConnectionPool(_utils.Singleton):
    """Manages MySQL Servers' connections.

    The pool is internally implemented as a dictionary that maps a server's
    uuid to a sequence of connections.
    """
    def __init__(self):
        """Creates a ConnectionPool object.
        """
        super(ConnectionPool, self).__init__()
        self.__pool = {}
        self.__lock = threading.RLock()

    def get_connection(self, uuid):
        """Get a connection.

        The method gets a connection from a pool if there is any.
        """
        assert(isinstance(uuid, _uuid.UUID))
        cnx = None
        with self.__lock:
            try:
                cnx = self.__pool[uuid].pop()
            except (KeyError, IndexError):
                pass

        if cnx and not cnx.is_connected():
            cnx = None

        return cnx

    def release_connection(self, uuid, cnx):
        """Release a connection to the pool.

        It is up to the developer to check if the connection is still
        valid and belongs to this server before returning it to the pool.
        """
        assert(isinstance(uuid, _uuid.UUID))
        with self.__lock:
            if uuid not in self.__pool:
                self.__pool[uuid] = []
            self.__pool[uuid].append(cnx)

    def get_number_connections(self, uuid):
        """Return the number of connections available in the pool.
        """
        assert(isinstance(uuid, _uuid.UUID))
        with self.__lock:
            try:
                return len(self.__pool[uuid])
            except KeyError:
                pass
        return 0

    def purge_connections(self, uuid):
        """Close and remove all connections that belongs to a MySQL Server
        which is identified by its uuid.
        """
        assert(isinstance(uuid, _uuid.UUID))
        with self.__lock:
            try:
                for cnx in self.__pool[uuid]:
                    cnx.disconnect()
                del self.__pool[uuid]
            except KeyError:
                pass


class MySQLServer(_persistence.Persistable):
    """Proxy class that provides an interface to access a MySQL Server
    Instance.

    To create a MySQLServer object, one needs to provide at least three
    parameters: uuid, address (i.e., host:port) and user's name. Most likely,
    a password is also necessary. If the uuid is not known beforehand,
    one can find this out as follows::

      import uuid as _uuid

      OPTIONS = {
        "uuid" : _uuid.UUID("FD0AC9BB-1431-11E2-8137-11DEF124DCC5"),
        "address"  : "localhost:13000",
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
    by calling exec_stmt() as follows::

      server.connect()
      ret = server.exec_stmt("SELECT VERSION()")
      print "MySQL Server has version", ret[0][0]

    Changing the value of the properties user or password triggers a call to
    disconnect.

    :param uuid: The uuid of the server.
    :param address:  The address of the server.
    :param user: The username used to access the server.
    :param passwd: The password used to access the server.
    :param default_charset: The default charset that will be used.
    :param status: Server's status.
    :type status: OFFLINE, RUNNING, SPARE, etc.
    """
    #SQL Statement for creating the table used to store details about the
    #server.
    CREATE_SERVER = (
        "CREATE TABLE servers "
        "(server_uuid VARCHAR(40) NOT NULL, "
        "server_address VARCHAR(128) NOT NULL, "
        "user CHAR(16), passwd TEXT, "
        "status ENUM(%s) NOT NULL, "
        "CONSTRAINT pk_server_uuid PRIMARY KEY (server_uuid))")

    #SQL Statement for dropping the table used to store the details about the
    #server.
    DROP_SERVER = ("DROP TABLE servers")

    #SQL statement for inserting a new server into the table
    INSERT_SERVER = ("INSERT INTO servers(server_uuid, server_address, user, "
                     "passwd, status) values(%s, %s, %s, %s, %s)")

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_USER = ("UPDATE servers SET user = %s WHERE server_uuid = %s")

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_PASSWD = (
        "UPDATE servers SET passwd = %s WHERE server_uuid = %s"
        )

    #SQL statement for updating the server table identified by the server id.
    UPDATE_SERVER_STATUS = (
        "UPDATE servers SET status = %s WHERE server_uuid = %s"
        )

    #SQL statement used for deleting the server identified by the server id.
    REMOVE_SERVER = ("DELETE FROM servers WHERE server_uuid = %s")

    #SQL Statement to retrieve the server from the state_store.
    QUERY_SERVER = (
        "SELECT server_uuid, server_address, user, passwd, status "
        "FROM servers WHERE server_uuid = %s"
        )

    # Define a session context for a variable.
    SESSION_CONTEXT = "SESSION"

    # Define a global context for a variable.
    GLOBAL_CONTEXT = "GLOBAL"

    # Set of contexts.
    CONTEXTS = [SESSION_CONTEXT, GLOBAL_CONTEXT]

    # Define an off-line status.
    OFFLINE = "OFFLINE"

    # Define a running status.
    RUNNING = "RUNNING"

    # Define an spare status.
    SPARE = "SPARE"

    # Define an faulty status.
    FAULTY = "FAULTY"

    # Define a recovering status.
    RECOVERING = "RECOVERING"

    # Set of possible statuses.
    SERVER_STATUS = [OFFLINE, RUNNING, SPARE, FAULTY, RECOVERING]

    def __init__(self, uuid, address, user=None, passwd=None,
                 status=RUNNING, default_charset="latin1"):
        """Constructor for MySQLServer.
        """
        super(MySQLServer, self).__init__()
        assert(isinstance(uuid, _uuid.UUID))
        assert(status in MySQLServer.SERVER_STATUS)
        self.__cnx = None
        self.__uuid = uuid
        self.__address = address
        self.__pool = ConnectionPool()
        self.__user = user
        self.__passwd = passwd
        self.__default_charset = default_charset
        self.__status = status
        self.__read_only = None
        self.__server_id = None
        self.__version = None
        self.__gtid_enabled = None
        self.__binlog_enabled = None

    @staticmethod
    @server_logging
    def discover_uuid(**kwargs): # TODO: Change the format of the parameter.
        """Retrieve the uuid from a server.

        :param kwargs: Dictionary with parmaters to connect to a server.
        """
        host = port = None
        params = kwargs.copy()
        keys = params.keys()

        params.setdefault("autocommit", True)
        params.setdefault("use_unicode", False)

        if "address" in keys:
            host, port = _server_utils.split_host_port(params["address"],
                _server_utils.MYSQL_DEFAULT_PORT)
            params.setdefault("host", host)
            params.setdefault("port", int(port))
            del params["address"]

        if "uuid" in keys:
            del params["uuid"]

        cnx = _server_utils.create_mysql_connection(**params)
        try:
            row = _server_utils.exec_mysql_stmt(cnx,
                "SELECT @@GLOBAL.SERVER_UUID")
            server_uuid = row[0][0]
        finally:
            _server_utils.destroy_mysql_connection(cnx)

        return server_uuid

    def _do_connection(self):
        """Get a connection.
        """
        cnx = self.__pool.get_connection(self.__uuid)
        if cnx:
            return cnx

        host, port = _server_utils.split_host_port(self.address,
            _server_utils.MYSQL_DEFAULT_PORT)
        user = self.__user or None
        passwd = self.__passwd or None

        return _server_utils.create_mysql_connection(
            autocommit=True, use_unicode=False, database="mysql",
            charset=self.__default_charset, host=host, port=port,
            user=user, passwd=passwd)

    def connect(self):
        """Connect to a MySQL Server instance.
        """
        self.disconnect()

        # Set up an internal connection.
        self.__cnx = self._do_connection()

        # Get server's uuid
        ret_uuid = self.get_variable("SERVER_UUID")
        ret_uuid = _uuid.UUID(ret_uuid)
        if ret_uuid != self.uuid:
            self.disconnect()
            raise _errors.UuidError(
                "Uuids do not match (stored (%s), read (%s))." % \
                (self.uuid, ret_uuid)
                )

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
            self.__pool.release_connection(self.__uuid, self.__cnx)
            self.__cnx = None
            self.__read_only = None
            self.__server_id = None
            self.__version = None
            self.__gtid_enabled = None
            self.__binlog_enabled = None

    def has_root_privileges(self):
        """Check if the current user has root privileges.
        """
        host, _ = _server_utils.split_host_port(self.__address,
            _server_utils.MYSQL_DEFAULT_PORT)

        ret = self.exec_stmt(
            "SELECT user FROM mysql.user WHERE user = %s "
            "AND host = %s AND grant_priv = 'Y' AND super_priv = 'Y'",
            {"params" : (self.__user, host)})
        if ret:
            return True
        return False

    def is_alive(self):
        """Determine if connection to server is still alive.

        Ping and is_connected only work partially, try exec_stmt to make
        sure connection is really alive.
        """
        res = False
        try:
            if self.__cnx is not None and self.__cnx.is_connected():
                self.exec_stmt("SHOW DATABASES")
                res = True
        except _errors.DatabaseError:
            pass
        return res

    def _check_read_only(self):
        """Check if the database was set to read-only mode.
        """
        ret_read_only = self.get_variable("READ_ONLY")
        self.__read_only = not ret_read_only in ("OFF", "0")

    @property
    def uuid(self):
        """Return the server's uuid.
        """
        return self.__uuid

    @property
    def address(self):
        """Return the server's address.
        """
        return self.__address

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
    def user(self, user, persister=None):
        """Set user's name who is used to connect to a server. Persist the
        user information in the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :param user: The user name.
        """
        if self.__user != user:
            # TODO: This will be removed from here when we remove the user
            # property from the server object.
            self.disconnect()
            persister.exec_stmt(MySQLServer.UPDATE_SERVER_USER,
                                {"params":(user, str(self.uuid))})
            self.__user = user

    @property
    def passwd(self):
        """Return user's password who is used to connect to a server. Load
        the server information from the state store and return the password.
        """
        return self.__passwd

    @passwd.setter
    def passwd(self, passwd, persister=None):
        """Set user's passord who is used to connect to a server. Persist the
        password information in the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        :param passwd: The password that needs to be set.
        """
        if self.__passwd != passwd:
            # TODO: This will be removed from here when we remove the passwd
            # property from the server object.
            self.disconnect()
            persister.exec_stmt(MySQLServer.UPDATE_SERVER_PASSWD,
                                {"params":(passwd, str(self.uuid))})
            self.__passwd = passwd

    @property
    def status(self):
        """Return the server's status.
        """
        return self.__status

    @status.setter
    def status(self, status, persister=None):
        """Set the server's status.

        :param status: The new server's status.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        assert(status in MySQLServer.SERVER_STATUS)
        persister.exec_stmt(MySQLServer.UPDATE_SERVER_STATUS,
                            {"params":(status, str(self.uuid))})
        self.__status = status

    def check_version_compat(self, expected_version):
        """Check version of the server against requested version.

        This method can be used to check for version compatibility.

        :param expected_version: Target server version.
        :type expected_version: (major, minor, release)
        :return: True if server version is GE (>=) version specified,
                 False if server version is LT (<) version specified.
        :rtype: Bool
        """
        assert(isinstance(expected_version, tuple))
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
            print "GTID_EXECUTED", record.GTID_EXECUTED, record[0]
            print "GTID_PURGED", record.GTID_PURGED, record[1]
            print "GTID_OWNED", record_GTID_OWNED, record[2]
        """
        # Check servers for GTID support
        if self.__cnx and not self.__gtid_enabled:
            raise _errors.ProgrammingError("Global Transaction IDs are not "\
                                           "supported.")

        query_str = (
            "SELECT @@GLOBAL.GTID_EXECUTED as GTID_EXECUTED, "
            "@@GLOBAL.GTID_PURGED as GTID_PURGED, "
            "@@GLOBAL.GTID_OWNED as GTID_OWNED"
        )

        return self.exec_stmt(query_str, {"columns" : True})

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
            engines = self.exec_stmt(query_str)
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
        return self.exec_stmt("SHOW BINARY LOGS", {"columns" : True})

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
        if not context:
            context = MySQLServer.GLOBAL_CONTEXT
        assert(context in MySQLServer.CONTEXTS)
        ret = self.exec_stmt("SELECT @@%s.%s as %s" % \
                             (context, variable, variable))
        return ret[0][0]

    def set_variable(self, variable, value, context=None):
        """Execute the SET command for the client and return a result set.
        """
        if not context:
            context = MySQLServer.GLOBAL_CONTEXT
        assert(context in MySQLServer.CONTEXTS)
        return self.exec_stmt("SET @@%s.%s = %s" \
                              % (context, variable, value))

    def exec_stmt(self, stmt_str, options=None):
        """Execute statements against the server.
        See :meth:`mysql.hub.server_utils.exec_stmt`.
        """
        return _server_utils.exec_mysql_stmt(self.__cnx, stmt_str, options)

    def __del__(self):
        """Destructor for MySQLServer.
        """
        self.disconnect()

    @staticmethod
    def remove(server, persister=None):
        """Remove a server from the state store.

        :param server: A reference to a server.
        :param persister: Persister to persist the object to.
        """
        persister.exec_stmt(
            MySQLServer.REMOVE_SERVER, {"params": (str(server.uuid), )}
            )

    @staticmethod
    def fetch(uuid, persister=None):
        """Return a server object corresponding to the uuid.

        :param uuid: The server id of the server object that needs to be
                     returned.
        :param persister: Persister to persist the object to.
        :return: The server object that corresponds to the server id
                 None if the server id does not exist.
        """
        cur = persister.exec_stmt(MySQLServer.QUERY_SERVER,
                                  {"fetch" : False, "params":(str(uuid),)})
        row = cur.fetchone()
        if row:
            uuid, address, user, passwd, status = row
            return MySQLServer(
                _uuid.UUID(uuid), address=address, user=user, passwd=passwd,
                status=status
                )

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that will store the Server
        information in the state store.

        :param persister: Persister to persist the object to.
        :raises: DatabaseError If the table already exists.
        """
        statuses = "'" + "', '".join(MySQLServer.SERVER_STATUS) + "'"
        persister.exec_stmt(MySQLServer.CREATE_SERVER % statuses)

    @staticmethod
    def drop(persister=None):
        """Drop the objects(tables) that represent the Server information in
        the persistent store.

        :param persister: Persister to persist the object to.
        :raises: DatabaseError If the drop of the related table fails.
        """
        persister.exec_stmt(MySQLServer.DROP_SERVER)

    @staticmethod
    def add(server, persister=None):
        """Write a server object into the state store.

        :param server: A reference to a server.
        :param persister: Persister to persist the object to.
        """
        assert(isinstance(server, MySQLServer))

        persister_uuid = persister.uuid
        if persister_uuid is not None and persister_uuid == server.uuid:
            raise _errors.UuidError("The MySQLPersister cannot be managed.")

        persister.exec_stmt(MySQLServer.INSERT_SERVER,
            {"params":(str(server.uuid), server.address, server.user,
            server.passwd, server.status)}
            )

    def __eq__(self,  other):
        """Two servers are equal if they are both subclasses of MySQLServer
        and have equal UUID.
        """
        return isinstance(other, MySQLServer) and self.__uuid == other.uuid

    def __hash__(self):
        """A server is hashable through its uuid.
        """
        return hash(self.__uuid)

    def __str__(self):
        ret = "<server(uuid={0}, address={1}, status={2}>".\
            format(self.__uuid, self.__address, self.__status)
        return ret

"""Define interfaces to manage servers.

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


class Server(object):
    """Abstract class used to provide interfaces to access a server.

    Notice that a server may be only a wrapper to a remote server.

    """
    def __init__(self, uuid, uri=None, running=None):
        """Constructor for the Server.

        Any server must be uniquely identified through a *UUID* (Universally
        Unique Identifier) and has a *URI* (Uniform Resource Identifier)
        used to connect to it through the Python Database API. A *URI* has the
        following format: *host:port*.

        Derived classes must not do any sort of provisioning at this point
        and it is assumed that the server process is already running.

        :param uuid: Uniquely identifies the server.
        :param uri: Used to connect to the server
        :param running: Indicate if the server is running or not.

        """
        assert(isinstance(uuid, _uuid.UUID))
        self.__uuid = uuid
        self.__uri = uri
        self.__available_connections = 0
        self.__pool = []
        self.__lock = threading.RLock()
        self.running = running

    def do_connection(self, *args, **kwargs):
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
        connection = None
        with self.__lock:
            if self.__pool:
                connection = self.__pool.pop()
                self.__available_connections -= 1
            else:
                connection = self.do_connection(*args, **kwargs)
        return connection

    def release_connection(self, connection):
        """Release a connection to the pool.

        After using a connection, it should be returned to the pool. It is
        up to the developer to check if the connection is still valid and
        belongs to this server before returning it to the pool.

        """
        assert(connection is not None)
        with self.__lock:
            self.__pool.append(connection)
            self.__available_connections += 1

    def purge_connections(self):
        """Close and remove all connections from the pool.
        """
        try:
            self.__lock.acquire()
            for connection in self.__pool:
                connection.disconnect()
        finally:
            self.__pool = []
            self.__lock.release()

    @property
    def available_connections(self):
        """Return the number of connections available in the pool.
        """
        with self.__lock:
            ret = self.__available_connections
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

    @uri.setter
    def uri(self, uri):
        """Set the server's uri.

        Before setting the property all connections in the pool are
        purged.

        """
        if self.uri != uri:
            self.purge_connections()
            self.__uri = uri

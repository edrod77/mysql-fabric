"""Define a set of classes and interfaces that are responsible for persite
information into a state store.
"""

import mysql.hub.server_utils as _server_utils
import mysql.hub.utils as _utils

class Persistable(object):
    @staticmethod
    def create(persister):
        """Create the tables to represent the current object in the state store.
        """
        raise NotImplementedError("Trying to execute abstract method create")

    @staticmethod
    def drop(persister):
        """Drop the tables to represent the current object in the state store.
        """
        raise NotImplementedError("Trying to execute abstract method drop")

    @staticmethod
    def add(persister):
        """Add the current object to the state store.
        """
        raise NotImplementedError("Trying to execute abstract method add")

    @staticmethod
    def remove(persister):
        """remove the current object from the state store.
        """
        raise NotImplementedError("Trying to execute abstract method remove")

    @staticmethod
    def fetch(persister):
        """Fetch the current object from the state store.
        """
        raise NotImplementedError("Trying to execute abstract method fetch")


class MySQLPersister(object):
    """Define a class that is responsible for implementing a state store.
    """
    def __init__(self, uri, user, passwd):
        """Constructor for MySQLPersister.
        """
        super(MySQLPersister, self).__init__()
        self.__cnx = None
        host, port = _server_utils.split_host_port(uri,
            _server_utils.MYSQL_DEFAULT_PORT)
        self.__cnx = _server_utils.create_mysql_connection(
            host=host, port=int(port), database="mysql", user=user,
            passwd=passwd, autocommit=True)
        #TODO: WHERE SHOULD WE CALL THIS FROM?
        self.exec_query("CREATE DATABASE IF NOT EXISTS fabric")
        self.exec_query("USE fabric")

    def __del__(self):
        """Destructor for MySQLPersister.
        """
        try:
            if self.__cnx:
                _server_utils.destroy_mysql_connection(self.__cnx)
        except AttributeError:
            pass

    def begin(self):
        """Start a new transaction.
        """        
        self.exec_query("BEGIN")

    def commit(self):
        """Commit an on-going transaction.
        """
        self.exec_query("COMMIT")

    def rollback(self):
        """Roll back an on-going transaction.
        """
        self.exec_query("ROLLBACK")

    def exec_query(self, query_str, options=None):
        """Execute statements against the server.
        """
        return _server_utils.exec_mysql_query(self.__cnx, query_str, options)

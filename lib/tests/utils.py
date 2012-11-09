"""Module holding support utilities for tests.
"""
import sqlite3

from mysql.hub.server import Server

class SkipTests(type):
    """Metaclass which is used to skip test cases as follows::

      import unittest
      import tests.utils as _utils

      class TestCaseClass(unittest.TestCase):
        __metaclass__ = _utils.SkipTests
    """
    def __new__(cls, name, bases, dct):
        """Create a new instance for SkipTests.
        """
        for name, item in dct.items():
            if callable(item) and name.startswith("test"):
                dct[name] = None
        return type.__new__(cls, name, bases, dct)


class PersistenceServer(Server):
    def __init__(self, uuid, uri):
        super(PersistenceServer, self).__init__(uuid, uri)
        self._do_connection()

    def _do_connection(self):
        self.__cnx = sqlite3.connect("/tmp/fabric.db")
        self.__cnx.execute("ATTACH DATABASE '/tmp/fabric.db' AS fabric")

    def commit(self):
       self.__cnx.commit()

    def rollback(self):
        self.__cnx.rollback()

    #TODO: Improve this method.
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
        if self.__cnx is None:
            raise _errors.DatabaseError("Connection is invalid.")

        options = options if options is not None else {}
        params = options.get('params', ())
        columns = options.get('columns', False)
        fetch = options.get('fetch', True)
        raw = options.get('raw', True)

        results = ()
        cur = self.__cnx.cursor()

        try:
            cur.execute(query_str, params)
        except Exception as error:
            print error

        if fetch or columns:
            try:
                results = cur.fetchall()
            except Exception as error:
                pass
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

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

"""Define features that can be used throughout the code.
"""
import os
import sys
import inspect
import ctypes
import re

TTL = 0
VERSION_TOKEN = 0
FABRIC_UUID = 0

class SingletonMeta(type):
    """Define a Singleton.
    This Singleton class can be used as follows::

      class MyClass(object):
        __metaclass__ = SingletonMeta
      ...
    """
    _instances = {}
    def __call__(mcs, *args, **kwargs):
        if mcs not in mcs._instances:
            mcs._instances[mcs] = super(SingletonMeta, mcs).__call__(*args,
                                                                     **kwargs)
        return mcs._instances[mcs]


class Singleton(object):
    """Define a Singleton.
    This Singleton class can be used as follows::

      class MyClass(Singleton):
      ...
    """
    __metaclass__ = SingletonMeta

def _do_fork():
    """Create a process.
    """
    try:
        if os.fork() > 0:
            sys.exit(0)
    except OSError, error:
        sys.stderr.write("fork failed with errno %d: %s\n" %
                         (error.errno, error.strerror))
        sys.exit(1)

def daemonize(stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    """Standard procedure for daemonizing a process.

    This process daemonizes the current process and put it in the
    background. When daemonized, logs are written to syslog.

    [1] Python Cookbook by Martelli, Ravenscropt, and Ascher.
    """
    _do_fork()
    os.chdir("/")        # The current directory might be removed.
    os.umask(0)
    os.setsid()
    _do_fork()
    sys.stdout.flush()
    sys.stderr.flush()
    sin = file(stdin, 'r')
    sout = file(stdout, 'a+')
    serr = file(stderr, 'a+', 0)
    os.dup2(sin.fileno(), sys.stdin.fileno())
    os.dup2(sout.fileno(), sys.stdout.fileno())
    os.dup2(serr.fileno(), sys.stdin.fileno())


def async_raise(tid, exctype):
    """Raise an exception within the context of a thread.

    :param tid: Thread Id.
    :param exctype: Exception class.
    :raises: exctype.
    """
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances).")

    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(tid), ctypes.py_object(exctype)
        )

    if res == 0:
        raise ValueError("Invalid thread id.")
    elif res != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
        raise SystemError("Failed to throw an exception.")

def split_dump_pattern(pattern):
    """Split a comma separated string of patterns, into a list of patterns.

    :param pattern: A comma separated string of patterns.
    """
    regex = re.compile('\s*,\s*')
    return regex.split(pattern)

def split_database_table(fully_qualified_table_name):
    """Split a fully qualified table name, which is the database name
    followed by the table name (database_name.table_name).

    :param fully_qualified_table_name: The fully qualified table name.
    """
    return fully_qualified_table_name.split('.')

def _wrap_output(output):
        """Used to wrap the the output in a standard format, viz,
        (_utils.FABRIC_UUID, _utils.VERSION_TOKEN, _utils.TTL, <o/p>).

        :param output: The output that needs to be wrapped.

        :return: the "output" parameter is returned in the following four
                     tuple format.
        """
        return (FABRIC_UUID, VERSION_TOKEN, TTL, output)



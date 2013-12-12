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

"""Module contains the abstract classes for implementing backup and
restoring a server from a backup. The module also contains the concrete
class for implementing the concrete class for doing backup using
mysqldump.
"""

from abc import ABCMeta, abstractmethod

import shlex
import subprocess

import mysql.fabric.errors as _errors
import mysql.fabric.server_utils as _server_utils

from urlparse import urlparse

from mysql.fabric.server import MySQLServer

class BackupImage(object):
    """Class that represents a backup image to which the output
    of a backup method is directed.

    :param uri: The URI to the destination on which the backup image needs
                       to be stored.
    """
    def __init__(self,  uri):
        """The constructor for initializing the backup image.
        """

        self.__uri = uri
        if self.__uri is not None:
            parsed = urlparse(self.__uri)
            self.__scheme = parsed.scheme
            self.__netloc = parsed.netloc
            self.__path = parsed.path
            self.__params = parsed.params
            self.__query = parsed.query
            self.__fragment = parsed.fragment
            self.__username = parsed.username
            self.__password = parsed.password
            self.__hostname = parsed.hostname
            self.__port = parsed.port

    @property
    def uri(self):
        """Return the uri.
        """
        return self.__uri

    @property
    def scheme(self):
        """Return the type of the URI like ftp, http, file etc.
        """
        return self.__scheme

    @property
    def netloc(self):
        """Return the network location in the URI.
        """
        return self.__netloc

    @property
    def path(self):
        """Return the path to the file represented by the URI.
        """
        return self.__path

    @property
    def params(self):
        """Return the parameters in the URI.
        """
        return self.__params

    @property
    def query(self):
        """Return the query in the backup image URI.
        """
        return self.__query

    @property
    def fragment(self):
        """Return the fragment in the backup image URI.
        """
        return self.__fragment

    @property
    def username(self):
        """Return the username in the backup image URI.
        """
        return self.__username

    @property
    def password(self):
        """Return the password in the backup image URI.
        """
        return self.__password

    @property
    def hostname(self):
        """Return the hostname in the backup image URI.
        """
        return self.__hostname

    @property
    def port(self):
        """Return the port number in the backup image URI.
        """
        return self.__port

class BackupMethod(object):
    """Abstract class that represents the interface methods that need to be
    implemented by a class that encapsulates a MySQL backup and restore
    method.
    """
    __metaclass__ = ABCMeta

    @staticmethod
    def backup(server):
        """Perform the backup.

        :param server: The server that needs to be backed up.
        """
        pass

    @staticmethod
    def restore(server,  image):
        """Restore the backup from the image to the server.

        :param server: The server on which the backup needs to be restored.
        :param image: The image that needs to be restored.
        """
        pass

    @staticmethod
    def copyBackup(image):
        """Will be used in cases when the backup needs to be taken on a source
        and will be copied to the destination machine.

        :param image: BackupImage object containning the location where the
                      backup needs to be copied to.
        """
        pass

class MySQLDump(BackupMethod):
    """Class that implements the BackupMethod abstract interface
    using MySQLDump.
    """

    MYSQL_DEFAULT_PORT = 3306
    MYSQLDUMP_ERROR_WARNING_LOG = "sharding_mysqldump_warning.log"

    @staticmethod
    def backup(server, mysqldump_binary=None):
        """Perform the backup using mysqldump.

        The backup results in creation a .sql file on the FABRIC server,
        this method needs to be optimized going forward. But for now
        this will suffice.

        :param server: The MySQLServer that needs to be backed up.
        :param mysqldump_binary: The fully qualified mysqldump binary.
        """
        assert (isinstance(server, MySQLServer))

        #Extract the host and the port from the server address.
        host = None
        port = None
        if server.address is not None:
            host, port = _server_utils.split_host_port(
                            server.address,
                            MySQLDump.MYSQL_DEFAULT_PORT
                         )

        #Form the name of the destination .sql file from the name of the
        #server host and the port number that is being backed up.
        destination = "MySQL_{HOST}_{PORT}.sql".format(
                                                    HOST=host,
                                                    PORT=port)

        mysqldump_command = shlex.split(mysqldump_binary)

        #Setup the MYSQLDump command that is used to backup the server.
        #Append the password parameter if the password is not None.
        mysqldump_command.extend([
             "--log-error=" + MySQLDump.MYSQLDUMP_ERROR_WARNING_LOG,
             "--all-databases", "--single-transaction",
            "--triggers", "--routines",
            "--protocol=tcp",
            "-h" + str(host),
            "-P" + str(port),
            "-u" + str(server.user)])

        if server.passwd:
            mysqldump_command.append("-p" + str(server.passwd))

        #Run the backup command
        try:
            with open(destination,"w") as fd_file:
                subprocess.check_call(
                    mysqldump_command,
                    stdout=fd_file,
                    shell=False
                )
        except subprocess.CalledProcessError as error:
            raise _errors.ShardingError(
                "Error while doing backup {ERROR}"
                .format(ERROR=str(error))
            )
        except OSError as error:
            raise _errors.ShardingError(
                "Error while doing backup {ERROR}"
                .format(ERROR=str(error))
            )

        #Return the backup image containing the location of the .sql file.
        return BackupImage(destination)

    @staticmethod
    def restore(server,  image, mysqlclient_binary):
        """Restore the backup from the image to the server.

        In the current implementation the restore works by restoring the
        .sql file created on the FABRIC server. This will be optimized in future
        implementation. But for the current implementation this suffices.

        :param server: The server on which the backup needs to be restored.
        :param image: The image that needs to be restored.
        :param mysqlclient_binary: The fully qualified mysqlclient binary.
        """

        assert (isinstance(server, MySQLServer))
        assert (image is None or isinstance(image, BackupImage))

        #Extract the host and the port from the server address.
        host = None
        port = None
        if server.address is not None:
            host, port = _server_utils.split_host_port(
                                                server.address,
                                                MySQLDump.MYSQL_DEFAULT_PORT
                                                )

        mysqlclient_command = shlex.split(mysqlclient_binary)

        #Use the mysql client for the restore. Append the -p option with
        #the password if there is a non-empty password.
        mysqlclient_command.extend([
            "-h" + str(host),  "-P" + str(port),
            "--protocol=tcp",
            "-u" + str(server.user)])

        if server.passwd:
            mysqlclient_command.append("-p" + str(server.passwd))

        #Fire the mysql client for the restore using the input image as
        #the restore source.
        try:
            with open(image.path,"r") as fd_file:
                subprocess.check_call(mysqlclient_command, stdin=fd_file,
                                      shell=False)
        except subprocess.CalledProcessError as error:
            raise _errors.ShardingError(
                "Error while doing backup {ERROR}"
                .format(ERROR=str(error))
            )
        except OSError as error:
            raise _errors.ShardingError(
                "Error while doing backup {ERROR}"
                .format(ERROR=str(error))
            )

        @abstractmethod
        def copyBackup(image):
            """Currently the MySQLDump based backup method works by backing
            up on the FABRIC server and restoring from there. This method is not
            required for the current implemention of MySQDump based backup.
            """
            pass

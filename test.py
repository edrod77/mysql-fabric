#
# Copyright (c) 2013,2015, Oracle and/or its affiliates. All rights reserved.
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

import getpass
import logging
import os.path
import sys
import xmlrpclib

import uuid as _uuid

from distutils.util import get_platform

from logging import (
    StreamHandler,
    FileHandler,
    )

NUMBER_OF_SERVERS = 6

if sys.version_info[0:2] < (2,7):
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
else:
    from logging import NullHandler

import unittest
from unittest import (
    TestLoader,
    TextTestRunner,
    )

def get_options():
    from optparse import OptionParser
    parser = OptionParser()
    # TODO: Fix option parsing so that -vvv and --verbosity=3 give same effect.
    parser.add_option("-v",
                      action="count", dest="verbosity",
                      help="Verbose mode. Multiple options increase verbosity")

    parser.add_option("--failfast",
                      action="store_true", dest="failfast",
                      help="Stop test suite on first failure")

    parser.add_option("--log-level", action="store", dest="log_level",
                      help="Set loglevel for debug output.")

    parser.add_option("--log-file",
                      action="store", dest="log_file",
                      metavar="FILE",
                      help="Set log file for debug output. "
                      "If not given, logging will be disabled.")

    parser.add_option("--build-dir", action="store", dest="build_dir",
                      help="Directory where mysql modules can be found.")

    # In simple use cases "root" is used as the admin user.
    parser.add_option("--user",
                      action="store", dest="user", default=None,
                      help=("Administrative user. Creates users required"
                            " for Fabric and application simulators."
                            " Needs at least these privileges on global level:"
                            " ALTER, ALTER ROUTINE, CREATE, CREATE ROUTINE,"
                            " CREATE TEMPORARY TABLES, CREATE USER,"
                            " CREATE VIEW, DELETE, DROP, EVENT, EXECUTE,"
                            " GRANT OPTION, INDEX, INSERT, LOCK TABLES,"
                            " PROCESS, RELOAD, REPLICATION CLIENT,"
                            " REPLICATION SLAVE, SELECT, SHOW DATABASES,"
                            " SHOW VIEW, SHUTDOWN, SUPER, TRIGGER, UPDATE."
                            ""
                            ))

    parser.add_option("--password",
                      action="store", dest="password", default=None,
                      help=("Password for the administrative user."))

    parser.add_option("--trial-mode",
                      action="store_true", dest="trial_mode",
                      help=("Use the administrative user for all accounts"
                            " (store_user, server_user,"
                            " backup_user, restore_user)"))

    parser.add_option("--host",
                      action="store", dest="host", default="localhost",
                      help=("Host to use for the backing store. "
                            "Defaults to 'localhost'."))

    parser.add_option("--port",
                      action="store", dest="port", default=32274, type=int,
                      help=("Port to use for the backing store. "
                            "Defaults to 32274."))

    parser.add_option("--servers", action="store", dest="servers",
                      help=("Space-separated list of eight server addresses,"
                            " that can be used for the test. Example:"
                            " localhost:13001 localhost:13002"
                            " localhost:13003 localhost:13004"
                            " localhost:13005 localhost:13006"
                            " localhost:13007 localhost:13008"))

    return parser.parse_args()

def get_config(options, env_options):
    from mysql.fabric import (
        config as _config,
    )
    import copy

    trial_mode = options.trial_mode
    user = options.user
    passwd = options.password

    # Configure parameters.
    params = {
        'protocol.xmlrpc': {
            'address': 'localhost:{0}'.format(env_options["xmlrpc_next_port"]),
            'threads': '5',
            'disable_authentication' : 'yes',
            },
        'protocol.mysql': {
            'address': 'localhost:{0}'.format(env_options["mysqlrpc_next_port"]),
            'threads': '5',
            'disable_authentication' : 'yes',
            },
        'executor': {
            'executors': '5',
            },
        'storage': {
            'address': options.host + ":" + str(options.port),
            'user': user if trial_mode else 'fabric_store',
            'password': passwd if trial_mode else 'storepw',
            'database': 'mysql_fabric',
            'connection_timeout': 'None',
            },
        'servers': {
            'user': user if trial_mode else 'fabric_server',
            'password': passwd if trial_mode else 'serverpw',
            'backup_user': user if trial_mode else 'fabric_backup',
            'backup_password': passwd if trial_mode else 'backuppw',
            'restore_user': user if trial_mode else 'fabric_restore',
            'restore_password': passwd if trial_mode else 'restorepw',
            'unreachable_timeout' : '5',
            },
        'sharding': {
            'mysqldump_program': env_options["mysqldump_path"],
            'mysqlclient_program': env_options["mysqlclient_path"],
            'prune_limit':'10000',
            },
        'failure_tracking': {
            'notifications' : '1',
            'notification_clients' : '1',
            'notification_interval' : '60',
            'failover_interval' : '0',
            'detections' : '3',
            'detection_interval' : '6',
            'detection_timeout' : '1',
            'prune_time' :  '60',
            },
        'connector': {
            'ttl' : '1'
        }
    }
    config = _config.Config(None, params)
    _config.global_config = copy.copy(config)
    config.config_file = ""

    if options.password is None:
        options.password = getpass.getpass("Enter password for the"
                                           " adminitrative user '%s': " %
                                           options.user)

    return config

def configure_path(options):
    # Compute the directory where this script is. We have to do this
    # fandango since the script may be called from another directory than
    # the repository top directory.
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Set up path correctly. We need the build directory in the path
    # and the directory with the tests (which are in 'lib/tests').  We
    # put this first in the path since there can be modules installed
    # under 'mysql' and 'tests'.
    if options.build_dir is None:
        options.build_dir = 'build'
    sys.path[0:1] = [
        os.path.join(
            script_dir, options.build_dir,
            "lib.%s-%s" % (get_platform(), sys.version[0:3]),
            ),
        # Windows installs without platform information in the directory name.
        os.path.join(script_dir, options.build_dir, 'lib'),
        # Find the tests.
        os.path.join(script_dir, 'lib'),
        ]

def configure_servers(options, config):
    """Check if some MySQL's addresses were specified and the number is
    greater than NUMBER_OF_SERVERS.
    """
    import tests.utils as _test_utils
    from mysql.fabric.server import (
        MySQLServer,
        ConnectionManager,
    )
    from mysql.fabric.backup import (
        MySQLDump,
    )
    try:
        servers = _test_utils.MySQLInstances()

        # The administrative user as given in --user and --password options.
        # In simple use cases "root" is used.
        servers.user = options.user
        servers.passwd = options.password

        # Backing store - "fabric_store/storepw".
        servers.state_store_address = config.get("storage", "address")
        servers.store_user = config.get("storage", "user")
        servers.store_passwd = config.get("storage", "password")
        servers.store_db = config.get("storage", "database")

        # Server user - "fabric_server/serverpw".
        servers.server_user = config.get("servers", "user")
        servers.server_passwd = config.get("servers", "password")

        # Backup user - "fabric_backup/backuppw".
        servers.backup_user = config.get("servers", "backup_user")
        servers.backup_passwd = config.get("servers", "backup_password")

        # Restore user - "fabric_restore/restorepw".
        servers.restore_user = config.get("servers", "restore_user")
        servers.restore_passwd = config.get("servers", "restore_password")

        # Set up the backing store.
        from mysql.fabric import persistence
        uuid = MySQLServer.discover_uuid(
            address=servers.state_store_address,
            user=servers.user,
            passwd=servers.passwd
        )
        server = MySQLServer(
            _uuid.UUID(uuid), address=servers.state_store_address,
            user=servers.user, passwd=servers.passwd
        )
        server.connect()
        # Precautionary cleanup.
        server.exec_stmt("DROP DATABASE IF EXISTS %s" % (servers.store_db,))
        # Create store user.
        _test_utils.create_test_user(
            server,
            servers.store_user,
            servers.store_passwd,
            [(persistence.required_privileges(),
              "{db}.*".format(db=servers.store_db))]
        )

        # Set up managed servers.
        if options.servers:
            for address in options.servers.split():
                servers.add_address(address)
                uuid = MySQLServer.discover_uuid(
                    address=address,
                    user=servers.user,
                    passwd=servers.passwd
                )
                server = MySQLServer(
                    _uuid.UUID(uuid),
                    address=address,
                    user=servers.user,
                    passwd=servers.passwd
                )
                server.connect()
                server.set_session_binlog(False)
                server.read_only = False

                # Drop user databases
                server.set_foreign_key_checks(False)
                databases = server.exec_stmt("SHOW DATABASES")
                for database in databases:
                    if database[0] not in MySQLServer.NO_USER_DATABASES:
                        server.exec_stmt("DROP DATABASE IF EXISTS %s" %
                                         (database[0],))
                server.set_foreign_key_checks(True)

                # Create server user.
                _test_utils.create_test_user(
                    server,
                    servers.server_user,
                    servers.server_passwd,
                    [(MySQLServer.SERVER_PRIVILEGES, "*.*"),
                     (MySQLServer.SERVER_PRIVILEGES_DB, "mysql_fabric.*")]
                )

                # Create backup user.
                _test_utils.create_test_user(
                    server,
                    servers.backup_user,
                    servers.backup_passwd,
                    [(MySQLDump.BACKUP_PRIVILEGES, "*.*")]
                )

                # Create restore user.
                _test_utils.create_test_user(
                    server,
                    servers.restore_user,
                    servers.restore_passwd,
                    [(MySQLDump.RESTORE_PRIVILEGES, "*.*")]
                )

                server.set_session_binlog(True)
                server.disconnect()
                ConnectionManager().purge_connections(server)
        if servers.get_number_addresses() < NUMBER_OF_SERVERS:
            print >> sys.stderr, "<<<<<<<<<< Some unit tests need {0} MySQL " \
                "Instances. >>>>>>>>>> ".format(NUMBER_OF_SERVERS)
            return False
    except Exception as error:
        print >> sys.stderr, "Error configuring servers:", error
        import traceback
        traceback.print_exc()
        return False

    return True

def check_connector():
    """Check whether the connector python is installed or not.
    """
    from mysql.fabric import (
        check_connector
    )
    from mysql.fabric.errors import (
        ConfigurationError
    )
    try:
        check_connector()
        return True
    except ConfigurationError as error:
        print >> sys.stderr, error

    return False

def run_tests(pkg, options, args, config):
    # Check whether the connector python is installed or not.
    if not check_connector():
        return None

    # Configure logging.
    configure_logging(options)

    # Configure MySQL Instances that might be used in the tests.
    if not configure_servers(options, config):
        return None

    # Fetch the tests that will be executed.
    import tests
    if len(args) == 0:
        args = tests.__all__

    # Load the test cases and run them.
    suite = TestLoader().loadTestsFromNames(pkg + '.' + mod for mod in args)
    proxy = setup_xmlrpc(options, config)

    if sys.version_info[0:2] >= (2,7):
        # Allow Ctrl-C to end the test suite gracefully.
        unittest.installHandler()
        # Redirect test output to stdout for a better merge with
        # "print" style temporary debug statements.
        # Follow verbosity and failfast options.
        ret = TextTestRunner(stream=sys.stdout,
                             verbosity=options.verbosity,
                             failfast=options.failfast).run(suite)
    else:
        # Redirect test output to stdout for a better merge with
        # "print" style temporary debug statements.
        # Follow verbosity option.
        ret = TextTestRunner(stream=sys.stdout,
                             verbosity=options.verbosity).run(suite)

    teardown_xmlrpc(proxy)
    return ret

def setup_xmlrpc(options, config):
    # Set up the persistence.
    from mysql.fabric import persistence

    # Set up the manager.
    from mysql.fabric.services.manage import (
        _start,
        _configure_connections,
    )

    _configure_connections(config)
    persistence.setup()
    persistence.init_thread()
    _start(options, config)

    # Set up the client.
    url = "http://%s" % (config.get("protocol.xmlrpc", "address"),)
    proxy = xmlrpclib.ServerProxy(url)

    while True:
        try:
            proxy.manage.ping()
            break
        except Exception:
            pass

    return proxy

def teardown_xmlrpc(proxy):
    from mysql.fabric import persistence

    proxy.manage.stop()
    persistence.deinit_thread()
    persistence.teardown()

def configure_logging(options):
    from mysql.fabric.handler import MySQLHandler

    handler = None
    mysql_handler = None

    if options.log_level:
        level = options.log_level.upper()
    else:
        level = "DEBUG"

    formatter = logging.Formatter(
        "[%(levelname)s] %(created)f - %(threadName)s - %(message)s"
    )

    mysql_handler = MySQLHandler()
    if options.log_file:
        # Configuring handler.
        handler = FileHandler(options.log_file, 'w')
        handler.setFormatter(formatter)
    elif options.log_level:
        # If a log-level is given, but no log-file, the assumption is
        # that the user want to see the output, so then we output the
        # log to standard output.
        handler = StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
    else:
        # If neither log file nor log level is given, we assume that
        # the user just want a test report.
        handler = NullHandler()

    # Logging levels.
    logging_levels = {
        "CRITICAL" : logging.CRITICAL,
        "ERROR" : logging.ERROR,
        "WARNING" : logging.WARNING,
        "INFO" : logging.INFO,
        "DEBUG" : logging.DEBUG
    }

    # Setting logging for "mysql.fabric".
    logger = logging.getLogger("mysql.fabric")
    logger.addHandler(handler)
    logger.addHandler(mysql_handler)
    logger.setLevel(logging_levels["DEBUG"])

    # Setting logging for "tests".
    logger = logging.getLogger("tests")
    logger.addHandler(handler)
    logger.addHandler(mysql_handler)
    logger.setLevel(logging_levels["DEBUG"])

    # Setting debugging level.
    mysql_handler.setLevel(logging_levels["DEBUG"])
    try:
        handler.setLevel(logging_levels[level])
    except KeyError:
        handler.setLevel(logging_levels["DEBUG"])

if __name__ == '__main__':
    # Note: do not change the names of the set of variables found below, e.g
    # "options" and "args". They are used in the test modules to pull in user
    # options.
    options, args = get_options()

    # Configure path to mysql.fabric.
    configure_path(options)

    # Configure options.
    xmlrpc_next_port = int(os.getenv("HTTP_PORT", 15500))
    mysqlrpc_next_port = xmlrpc_next_port + 1
    mysqldump_path = os.getenv("MYSQLDUMP", "")
    mysqlclient_path = os.getenv("MYSQLCLIENT", "")

    # Detect missing environment
    assert(mysqldump_path != "")
    assert(mysqlclient_path != "")

    env_options = {
        "xmlrpc_next_port" : xmlrpc_next_port,
        "mysqldump_path" : mysqldump_path,
        "mysqlclient_path" : mysqlclient_path,
        "mysqlrpc_next_port": mysqlrpc_next_port,
    }
    config = get_config(options, env_options)

    # Run tests.
    result = run_tests('tests', options, args, config)
    sys.exit(result is None or not result.wasSuccessful())

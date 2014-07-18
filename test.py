#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
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
    parser.add_option("--log-level", action="store", dest="log_level",
                      help="Set loglevel for debug output.")
    parser.add_option("--log-file",
                      action="store", dest="log_file",
                      metavar="FILE",
                      help="Set log file for debug output. "
                      "If not given, logging will be disabled.")
    parser.add_option("--build-dir", action="store", dest="build_dir",
                      help="Set the directory where mysql modules will be found.")
    parser.add_option("--host",
                      action="store", dest="host",
                      default="localhost",
                      help="Host to use for the state store.")
    parser.add_option("--user",
                      action="store", dest="user",
                      default=getpass.getuser(),
                      help=("User to use to manage MySQL Server instances and "
                            "access the state store. Default to current "
                            "user."))
    parser.add_option("--password",
                      action="store", dest="password", default=None,
                      help=("Password to use to manage MySQL Server instances "
                            "and access the state store. Default to the "
                            "empty string."))
    parser.add_option("--db-user",
                      action="store", dest="db_user", default="mats",
                      help=("User created to accesss the MySQL Server instances"
                            "while running the test cases "))
    parser.add_option("--port",
                      action="store", dest="port", default=32274, type=int,
                      help=("Port to use when connecting to the state store. "
                            "Default to 32274."))
    parser.add_option("--database",
                      action="store", dest="database", default='fabric',
                      help=("Database name to use for the state store."
                            " Default to 'fabric'."))
    parser.add_option("--servers", action="store", dest="servers",
                      help="Set of servers' addresses that can be used.")
    return parser.parse_args()

def get_config(options, env_options):
    from mysql.fabric import (
        config as _config,
    )

    # Configure parameters.
    params = {
        'protocol.xmlrpc': {
            'address': 'localhost:{0}'.format(env_options["xmlrpc_next_port"]),
            'threads': '5',
            'disable_authentication' : 'yes',
            },
        'executor': {
            'executors': '5',
            },
        'storage': {
            'address': options.host + ":" + str(options.port),
            'user': options.user,
            'password': options.password or '',
            'database': 'fabric',
            'connection_timeout': 'None',
            },
        'servers': {
            'user': options.db_user,
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
    config.config_file = ""
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
        os.path.join(script_dir, 'lib'),
        ]

def configure_servers(options):
    """Check if some MySQL's addresses were specified and the number is
    greater than NUMBER_OF_SERVERS.
    """
    import tests.utils as _test_utils
    from mysql.fabric.server import (
        MySQLServer,
        ConnectionPool,
    )
    try:
        servers = _test_utils.MySQLInstances()
        servers.state_store_address = "{host}:{port}".format(
            host=options.host, port=options.port
        )
        servers.user = options.db_user
        servers.passwd = None
        servers.root_user = options.user
        servers.root_passwd = options.password
        if options.servers:
            for address in options.servers.split():
                servers.add_address(address)
                uuid = MySQLServer.discover_uuid(
                    address=address, user=servers.root_user,
                    passwd=servers.root_passwd
                )
                server = MySQLServer(
                    _uuid.UUID(uuid), address=address, user=servers.root_user,
                    passwd=servers.root_passwd
                )
                server.connect()
                server.set_session_binlog(False)
                server.exec_stmt(
                    "GRANT {privileges} ON *.* TO '{user}'@'%%'".format(
                    privileges=", ".join(MySQLServer.ALL_PRIVILEGES),
                    user=servers.user)
                )
                server.exec_stmt("FLUSH PRIVILEGES")
                server.set_session_binlog(True)
                server.disconnect()
                ConnectionPool().purge_connections(server.uuid)
        if servers.get_number_addresses() < NUMBER_OF_SERVERS:
            print "<<<<<<<<<< Some unit tests need %s MySQL Instances. " \
              ">>>>>>>>>> " % (NUMBER_OF_SERVERS, )
            return False
    except Exception as error:
        print "Error configuring servers:", error
        return False

    return True

def check_connector():
    """Check if the connector is properly configured.
    """
    try:
        import mysql.connector
    except Exception as error:
        import mysql
        path = os.path.dirname(mysql.__file__)
        print "Tried to look for mysql.connector at (%s)" % (path, )
        print "Error:", error
        return False
    return True

def run_tests(pkg, options, args, config):
    import tests
    if len(args) == 0:
        args = tests.__all__

    # Find out which MySQL Instances can be used for the tests.
    if not check_connector() or not configure_servers(options):
        return None

    # Load the test cases and run them.
    suite = TestLoader().loadTestsFromNames(pkg + '.' + mod for mod in args)
    proxy = setup_xmlrpc(options, config)
    ret = TextTestRunner(verbosity=options.verbosity).run(suite)
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

def configure_logging(level):
    from mysql.fabric.handler import MySQLHandler

    handler = None
    mysql_handler = None

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
    configure_path(options)
    xmlrpc_next_port = int(os.getenv("HTTP_PORT", 15500))
    mysqldump_path = os.getenv("MYSQLDUMP", "")
    mysqlclient_path = os.getenv("MYSQLCLIENT", "")
    env_options = {
        "xmlrpc_next_port" : xmlrpc_next_port,
        "mysqldump_path" : mysqldump_path,
        "mysqlclient_path" : mysqlclient_path,
    }
    config = get_config(options, env_options)

    if options.password is None:
        options.password = getpass.getpass()

    # Configure logging.
    if options.log_level:
        level = options.log_level.upper()
    else:
        level = "DEBUG"
    configure_logging(level)

    # Run tests.
    result = run_tests('tests', options, args, config)
    sys.exit(result is None or not result.wasSuccessful())

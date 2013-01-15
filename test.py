import getpass
import logging
import os.path
import sys

from distutils.util import get_platform

from logging import (
    StreamHandler,
    FileHandler,
    )

NUMBER_OF_SERVERS = 4

if sys.version_info[0:2] < (2,7):
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
else:
    from logging import NullHandler

# Compute the directory where this script is. We have to do this
# fandango since the script may be called from another directory than
# the repository top directory.
script_dir = os.path.dirname(os.path.realpath(__file__))
# Append the directory where the project is located.
sys.path.append(os.path.join(script_dir, 'lib'))

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
                      help="Host to use for the persistance database.")
    parser.add_option("--user",
                      action="store", dest="user",
                      default=getpass.getuser(),
                      help=("User to use when connecting to the persistance "
                            " database. Default to the current user."))
    parser.add_option("--password",
                      action="store", dest="password", default=None,
                      help=("Password to use when connecting to the"
                            " persistance database. Default to the"
                            " empty string."))
    parser.add_option("--port",
                      action="store", dest="port", default=3306, type=int,
                      help=("Port to use when connecting to persistance"
                            " database. Default to 3306."))
    parser.add_option("--database",
                      action="store", dest="database", default='fabric',
                      help=("Database name to use for persistance database."
                            " Default to 'fabric'."))
    parser.add_option("--servers", action="store", dest="servers",
                      help="Set of servers' addresses that can be used.")
    return parser.parse_args()

def configure_servers(set_of_servers):
    """Check if some MySQL's addresses were specified and the number is
    greater than NUMBER_OF_SERVERS.
    """
    # TODO: Check if the servers exist and are alive.
    import tests.utils as _test_utils
    servers = _test_utils.MySQLInstances()
    if set_of_servers:
        for server in set_of_servers.split():
            servers.add_address(server)
    if servers.get_number_addresses() < NUMBER_OF_SERVERS:
        print "<<<<<<<<<< Some unit tests need %s MySQL Instances. " \
              ">>>>>>>>>> " % (NUMBER_OF_SERVERS, )
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

def run_tests(pkg, options, args):
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
        os.path.join(script_dir, options.build_dir, 'lib'),
        os.path.join(script_dir, 'lib'),
        ]

    import tests
    if len(args) == 0:
        args = tests.__all__

    # Find out which MySQL Instances can be used for the for the
    # tests.
    if not check_connector() or not configure_servers(options.servers):
        return None

    # Load the test cases and run them.
    suite = TestLoader().loadTestsFromNames(pkg + '.' + mod for mod in args)
    return TextTestRunner(verbosity=options.verbosity).run(suite)

if __name__ == '__main__':
    # Note: do not change the names of "options" and "args". They are
    # used in the test modules to pull in user options.
    options, args = get_options()
    xmlrpc_next_port = int(os.getenv("HTTP_PORT", 15500))
    mysqld_next_port = 13000

    handler = None
    formatter = logging.Formatter(
        "[%(levelname)s] %(asctime)s - %(threadName)s - %(message)s")

    if options.password is None:
        options.password = getpass.getpass()

    if options.log_file:
        # Configuring handler.
        handler = FileHandler(options.log_file, 'w')
        handler.setFormatter(formatter)
    elif options.log_level:
        # If a log-level is given, but no log-file, the assumption is
        # that the user want to see the output, so then we output the
        # log to standard output.
        handler = StreamHandler(sys.stdout)
    else:
        # If neither log file nor log level is given, we assume that
        # the user just want a test report.
        handler = NullHandler()

    # Setting logging for "mysql.hub".
    logger = logging.getLogger("mysql.hub")
    logger.setLevel(options.log_level or 'INFO')
    logger.addHandler(handler)

    # Setting logging for "tests".
    logger = logging.getLogger("tests")
    logger.setLevel(options.log_level or 'INFO')
    logger.addHandler(handler)

    result = run_tests('tests', options, args)
    sys.exit(result is None or not result.wasSuccessful())

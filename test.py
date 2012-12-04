import logging
import os.path
import sys

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
    parser.add_option("-v", action="count", dest="verbosity",
                      help="Verbose mode. Multiple options increase verbosity")
    parser.add_option("--log-level", action="store", dest="log_level",
                      default="DEBUG", help="Set loglevel for debug output.")
    parser.add_option("--log-file", action="store", dest="log_file",
                      help="Set log file for debug output. "
                      "If not given, logging will be disabled.", metavar="FILE")
    parser.add_option("--build-dir", action="store", dest="build_dir",
                      help="Set the directory where mysql modules will be "\
                      "found.")
    return parser.parse_args()

def discover_servers(servers):
    # TODO: We need to load this information from a file that is dynamically
    # created by any external tool.
    servers.add_uri("localhost:13000")
    servers.add_uri("localhost:13001")
    servers.add_uri("localhost:13002")
    servers.add_uri("localhost:13003")
    servers.add_uri("localhost:13004")

def run_tests(pkg, opt, args):
    if len(args) == 0:
        import tests
        args = tests.__all__

    # First item is the script directory or the empty string (for example,
    # if running interactively) so we replace the first entry with the
    # library directory.
    build_dir = "lib" if opt.build_dir is None else opt.build_dir
    sys.path[0] = os.path.join(script_dir, build_dir)

    # Find out which MySQL Instances can be used for the for the tests.
    import tests.utils as _test_utils
    servers = _test_utils.MySQLInstances()
    discover_servers(servers)

    # Load the test cases and run them.
    suite = TestLoader().loadTestsFromNames(pkg + '.' + mod for mod in args)
    return TextTestRunner(verbosity=opt.verbosity).run(suite)

if __name__ == '__main__':
    opt, args = get_options()

    handler = None
    if opt.log_file:
        # Configuring handler.
        handler = logging.FileHandler(opt.log_file, 'w')
        formatter = logging.Formatter(
            "[%(levelname)s] %(asctime)s - %(threadName)s - %(message)s")
        handler.setFormatter(formatter)
    else:
        handler = NullHandler()

    # Setting logging for "mysql.hub".
    logger = logging.getLogger("mysql.hub")
    logger.setLevel(opt.log_level)
    logger.addHandler(handler)

    # Setting logging for "tests".
    logger = logging.getLogger("tests")
    logger.setLevel(opt.log_level)
    logger.addHandler(handler)

    result = run_tests('tests', opt, args)
    sys.exit(not result.wasSuccessful())

"""Start the hub daemon.

The daemon is started using ``hub-start`` command-line program.

"""

import logging
import logging.handlers
import os
import sys

import mysql.hub.services as _services
import mysql.hub.events as _events
import mysql.hub.persistence as _persistence
import mysql.hub.failure_detector as _detector

from mysql.hub.config import Config

def _do_fork():
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

def start(config):
    address = config.get('storage', 'address')
    try:
        host, port = address.split(':')
        port = int(port)
    except ValueError:
        host = address
        port = 3306

    user = config.get('storage', 'user')
    password = config.get('storage', 'password')
    database = config.get('storage', 'database')

    # Set up the components
    service_manager = _services.ServiceManager(config, shutdown)

    # Load all services into the service manager
    service_manager.load_services()

    # Initialize the persistence system. This have to be after the
    # services are loaded to ensure that all persistent classes are
    # defined (hence added to the list of persistent classes).
    _persistence.init(host=host, port=port,
                      user=user, password=password,
                      database=database)
    _persistence.init_thread()

    # Start the executor, failure detector and then service manager.
    _events.Handler().start()
    _detector.FailureDetector.register_groups()
    service_manager.start()

def shutdown():
    _detector.FailureDetector.unregister_groups()
    _services.ServiceManager().shutdown()
    _events.Handler().shutdown()
    return True

def main(argv):
    # TODO: Move all option file handling to mysql.hub.options
    from mysql.hub.options import OptionParser
    parser = OptionParser()

    parser.add_option(
        "--daemonize",
        dest="daemonize",
        action="store_true", default=False,
        help="Daemonize the manager")

    # Parse options
    options, _args = parser.parse_args(argv)
    config = Config(options.config_file, options.config_params,
                    options.ignore_site_config)

    # Set up logger
    # We have used a fixed path here, i.e. 'mysql.hub', to make sure
    # that all subsequent calls to getLogger(__name__) finds a
    # properly configured root.  Notice that __name__ is the module's
    # name and that all our modules have the prefix 'mysql.hub'.
    logger = logging.getLogger('mysql.hub')

    # Set up syslog handler, if needed
    if options.daemonize:
        address = config.get('logging.syslog', 'address')
        handler = logging.handlers.SysLogHandler(address)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "[%(levelname)s] %(asctime)s - %(threadName)s"
        " %(thread)d - %(message)s")
    handler.setFormatter(formatter)
    logging_level = config.get('logging', 'level')
    logger.setLevel(logging_level)
    logger.addHandler(handler)

    # Daemonize ourselves, if we should
    if options.daemonize:
        daemonize()

    logger.info("Fabric node starting")
    start(config)
    logger.info("Fabric node stopped")

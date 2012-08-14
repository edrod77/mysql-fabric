"""Start the hub daemon.
"""

import logging
import logging.handlers
import os
import sys
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

import mysql.hub.config as _config
import mysql.hub.core as _core

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

def main(argv):
    # TODO: Move all option file handling to mysql.hub.options
    from optparse import OptionParser
    parser = OptionParser()

    parser.add_option("--config",
                      action="store", dest="config_file", default="hub.cfg",
                      metavar="FILE",
                      help="Read configuration from FILE")
    parser.add_option("--daemonize",
                      dest="daemonize",
                      action="store_true", default=False,
                      help="Daemonize the manager")

    # Parse options
    opt, _args = parser.parse_args(argv)

    # TODO: Move all config file handling to mysql.hub.config
    config = SafeConfigParser(_config.DEFAULTS)

    # Read in basic configuration information
    config.readfp(open(opt.config_file), opt.config_file)

    # TODO: Support configuration files for at least: instance, user, site

    # TODO: Options replace values in config: those should be overwritten

    # Set up logger
    # We have used a fixed path here, i.e. 'mysql.hub', to make sure that all
    # subsequent calls to getLogger(__name__) finds a properly configured root.
    # Notice that __name__ is the module's name and that all our modules have
    # the prefix 'mysql.hub'.
    logger = logging.getLogger('mysql.hub')


    # Set up syslog handler, if needed
    if opt.daemonize:
        try:
            address = config.get('logging.syslog', 'address')
        except (NoSectionError, NoOptionError) as error:
            address = '/dev/log' 
        handler = logging.handlers.SysLogHandler(address)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(threadName)s"\
                                  " %(thread)d - %(message)s")
    handler.setFormatter(formatter)
    try:
        logging_level = config.get('logging', 'level')
    except (NoSectionError, NoOptionError) as error:
        logging_level = logging.DEBUG 
    logger.setLevel(logging_level)
    logger.addHandler(handler)

    # Daemonize ourselves, if we should
    if opt.daemonize:
        daemonize()

    manager = _core.Manager(config)
    manager.start()

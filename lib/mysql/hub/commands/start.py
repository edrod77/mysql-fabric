"""Start the hub daemon.
"""

import logging
import logging.handlers
import mysql.hub.config as _config
import mysql.hub.core as _core
import os
import sys

def _do_fork():
    try:
        if os.fork() > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork failed with errno %d: %s\n" % (e.errno, e.strerror))
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
    sin = file('/dev/null', 'r')
    sout = file('/dev/null', 'a+')
    serr = file('/dev/null', 'a+', 0)
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
    parser.add_option("--syslog",
                      dest="syslog",
                      action="store_true", default=False,
                      help="Write log messages to syslog")
    parser.add_option("--loglevel",
                      action="store", dest="loglevel", default='WARNING',
                      metavar="LEVEL",
                      help="Set logging level to LEVEL")

    # Parse options
    opt, args = parser.parse_args(argv)

    # TODO: Move all config file handling to mysql.hub.config
    from ConfigParser import ConfigParser
    config = ConfigParser(_config.DEFAULTS)

    # Read in basic configuration information
    config.readfp(open(opt.config_file), opt.config_file)

    # TODO: We should support configuration files for at least: instance, user, site

    # TODO: Some options replace values in config, so we should overwrite those

    # Set up logger
    # TODO: Switch to use __name__ ?
    logger = logging.getLogger('mysql.hub')

    # Set up syslog handler, if needed
    if opt.syslog or opt.daemonize:
        handler = logging.handlers.SysLogHandler('/dev/log')
    else:
        handler = logging.StreamHandler()

    level = logging.getLevelName(opt.loglevel)
    if isinstance(level, basestring):
        parser.error("%s is not a level" % (opt.loglevel,))

    logger.setLevel(level)
    logger.addHandler(handler)

    # Daemonize ourselves, if we should
    if opt.daemonize:
        daemonize()

    manager = _core.Manager(logger, config)
    manager.start()


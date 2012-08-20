"""Change logging level for an specific module.

This is handy when one wants to debug the application but does not want to
restart it.
"""

import mysql.hub.config as _config
import sys
import xmlrpclib

def main(argv):
    from optparse import OptionParser
    parser = OptionParser()

    parser.add_option("--config",
                      action="store", dest="config_file", default="hub.cfg",
                      metavar="FILE",
                      help="Read configuration from FILE")
    parser.add_option("--loglevel",
                      action="store", dest="loglevel", default='INFO',
                      metavar="LEVEL",
                      help="Set logging level to LEVEL")

    module = argv[0]
    opt, args = parser.parse_args(argv[1:])

    # TODO: Move all config file handling to mysql.hub.config
    from ConfigParser import ConfigParser
    config = ConfigParser(_config.DEFAULTS)

    # Read in basic configuration information
    config.readfp(open(opt.config_file), opt.config_file)

    # TODO: We should support configuration files for at least: instance, user, site
    port = config.getint("protocol.xmlrpc", "port")
    proxy = xmlrpclib.ServerProxy("http://localhost:%d/" % (port,))
    try:
        proxy.set_logging_level(module, opt.loglevel)
    except xmlrpclib.Fault, err:
        print >> sys.stderr, "Failure (%d): %s" % (err.faultCode, err.faultString)

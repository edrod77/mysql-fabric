"""Stop the hub daemon.
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
    
    opt, args = parser.parse_args(argv)

    # TODO: Move all config file handling to mysql.hub.config
    from ConfigParser import ConfigParser
    config = ConfigParser(_config.DEFAULTS)

    # Read in basic configuration information
    config.readfp(open(opt.config_file), opt.config_file)

    # TODO: We should support configuration files for at least: instance, user, site

    # Connect to the standard server and tell it to shutdown
    # TODO: We need to allow killing the server using signals
    # TODO: We have to make the PID available in a pid-file.
    port = config.getint("protocol.xmlrpc", "port")
    proxy = xmlrpclib.ServerProxy("http://localhost:%d/" % (port,))
    try:
        proxy.shutdown()
    except xmlrpclib.Fault, err:
        print >>sys.stderr, "Failure (%d): %s" % (err.faultCode, err.faultString)

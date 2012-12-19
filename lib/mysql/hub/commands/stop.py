"""Stop the hub daemon.
"""

import mysql.hub.config as _config
import sys
import xmlrpclib

def main(argv):
    """Stop the hub daemon.
    """
    from mysql.hub.options import OptionParser
    parser = OptionParser()

    options, _args = parser.parse_args(argv)
    config = _config.Config(options.config_file, options.config_params,
                            options.ignore_site_config)

    # Connect to the standard server and tell it to shutdown
    # TODO: We need to allow killing the server using signals
    # TODO: We have to make the PID available in a pid-file.
    address = config.get("protocol.xmlrpc", "address")
    host, port = address.split(':')
    proxy = xmlrpclib.ServerProxy("http://%s:%s/" % (host, port))
    try:
        proxy.shutdown()
    except xmlrpclib.Fault, err:
        print >> sys.stderr, "Error (%d): %s" % (err.faultCode, err.faultString)

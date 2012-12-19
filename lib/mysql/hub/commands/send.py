"""Send an event to a Fabric node.
"""

import sys
import xmlrpclib

import mysql.hub.config as _config

def main(argv):
    """Send an event to a Fabric node.
    """
    from mysql.hub.options import OptionParser
    parser = OptionParser()

    options, args = parser.parse_args(argv)
    config = _config.Config(options.config_file, options.config_params,
                            options.ignore_site_config)

    address = config.get("protocol.xmlrpc", "address")
    host, port = address.split(":")
    proxy = xmlrpclib.ServerProxy("http://%s:%s" % (host, port))
    event = args.pop(0)
    try:
        return proxy.event.trigger(event, *args)
    except xmlrpclib.Fault, err:
        msg = "Failure (%d): %s" % (err.faultCode, err.faultString)
        print >> sys.stderr, msg

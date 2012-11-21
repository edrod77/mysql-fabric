"""Change logging level for an specific module.

This is handy when one wants to debug the application but does not want to
restart it.
"""

import sys
import xmlrpclib

import mysql.hub.config as _config

def main(argv):
    from mysql.hub.options import OptionParser
    parser = OptionParser()

    options, args = parser.parse_args(argv)
    config = _config.Config(options.config_file, options.config_params,
                            options.ignore_site_config)

    address = config.get("protocol.xmlrpc", "address")
    host, port = address.split(":")
    proxy = xmlrpclib.ServerProxy("http://%s:%s/" % (host, port))
    module = args.pop()
    try:
        loglevel = config.get('logging', 'level')
        proxy.set_logging_level(module, loglevel)
    except xmlrpclib.Fault, err:
        print >> sys.stderr, "Failure (%d): %s" % \
            (err.faultCode, err.faultString)

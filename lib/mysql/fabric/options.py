import optparse

import mysql.fabric.config as _config

class OptionParser(optparse.OptionParser):
    """Option with default options for all tools.

    This class is used as default option parser and specific commands
    can add their own options.

    There are three options that are provided:

    .. option:: --param <section>.<name>=<value> ...

       This option allow configuration parameters to be overridden by
       providing them on the command line.  The parameters are stored
       in the config_param attribute

    .. option:: --config <filename>

       Name of an extra configuration file to read, in addition to the
       site-wide configuration file. Options given in this file will
       override the options given in the site-wide file.

    .. option:: --ignore_site_config

       Ignore the site-wide configuration file.

    Based on this, the options structure returned can hold the
    following attributes:

    .. attribute:: config_file

       File name for extra configuration file to read, provided by the
       :option:`--config` option. Defaults to ``fabric.cfg``.

    .. attribute:: config_param

       The configuration parameters provided with :option:`--param` as a
       dictionary of dictionaries, for example::

         {
            'protocol.xmlrpc': {
               'address': 'localhost:8080',
            },
            'logging': {
               'level': 'INFO',
            },
         }

    .. attribute:: ignore_site_config

       If True, the site configuration file will be ignored. This can
       be useful when all configuration parameters are given on the
       command line.

    A typical usage can be seen in :mod:`mysql.fabric.config`.

    """
    def __init__(self, *args, **kwrds):
        optparse.OptionParser.__init__(self, *args, **kwrds)

        self.add_option(
            "--param",
            action="callback", callback=_config.parse_param,
            type="string", nargs=1, dest="config_params",
            help="Override a configuration parameter.")
        self.add_option(
            "--config",
            action="store", dest="config_file", default=None,
            metavar="FILE",
            help="Read configuration from FILE.")
        self.add_option(
            "--ignore-site-config",
            action="store_true", dest="ignore_site_config", default=False,
            help="Ignore the site-wide configuration file.")


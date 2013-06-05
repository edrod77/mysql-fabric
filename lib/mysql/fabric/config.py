"""Reading the configuration files and options.

This module implements support needed to read the configuration files
and set up the configuration used both by the Fabric nodes and by the
clients.

Reading the configuration goes through three steps:

1. Read the site-wide configuration file.

2. Read the command-line configuration file (if provided). Settings in
   this file will override any settings read from the site-wide
   configuration file.

3. Some options override settings in both the configuration files.

"""

import ConfigParser
import re

# These are propagated to the importer
from ConfigParser import NoSectionError, NoOptionError

#: Name of site-wide configuration file
SITE_CONFIG = "/etc/fabric/main.cfg"

_VALUE_CRE = re.compile(
    r'(?P<section>\w+(?:\.\w+)*)\.(?P<name>\w+)=(?P<value>.*)')

def parse_param(option, _opt, value, parser):
    """Parser to parse a param option of the form x.y.z=some-value.

    This function is used as a callback to parse param values given on
    the command-line.
    """

    mobj = _VALUE_CRE.match(value)
    if mobj:
        field = getattr(parser.values, option.dest)
        if field is None:
            field = {}
            setattr(parser.values, option.dest, field)
        section = field.setdefault(mobj.group('section'), {})
        section[mobj.group('name')] = mobj.group('value')

class Config(ConfigParser.SafeConfigParser):
    """Fabric configuration file parser and configuration handler.

    This class manages the configuration of Fabric nodes and clients,
    including configuration file locations.

    Sample usage::

       from mysql.fabric.options import OptionParser
       from mysql.fabric.config import Config

       parser = OptionParser()
       ...
       options, args = parser.parse_args()
       config = Config(options.config_file, options.config_params,
                       options.ignore_site_config)

    """

    # Defaults are currently strings and we do some internal parsing
    # to create the correct objects, e.g., logging.INFO instead of
    # 'INFO'. That relieves the configuration class user of some
    # interpretation and ensure that all users interpret it the same.
    _DEFAULTS = {
        'logging': {
            'level': 'INFO',
            },
        'logging.syslog': {
            'address': '/dev/log',
            },
        }

    def __init__(self, config_file, config_params=None,
                 ignore_site_config=False):
        """Create the configuration parser, read the configuration
        files, and set up the configuration from the options.
        """

        ConfigParser.SafeConfigParser.__init__(self)

        # Set default values of options from above
        for section, var_dict in self._DEFAULTS.items():
            self.add_section(section)
            for var, val in var_dict.items():
                self.set(section, var, str(val))

        # Read site-wide configuration file
        if not ignore_site_config:
            self.readfp(open(SITE_CONFIG))

        # Read optional configuration file
        if config_file is not None:
            self.read(config_file)

        # Incorporate options into the configuration. These are read
        # from the mapping above and written into the configuration.
        if config_params is not None:
            for section, var_dict in config_params.items():
                if not self.has_section(section):
                    self.add_section(section)
                for key, val in var_dict.items():
                    self.set(section, key, val)

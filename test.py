import logging
import os.path
import sys

# Compute the directory where this script is. We have to do this
# fandango since the script may be called from another directory than
# the repository top directory.
script_dir = os.path.dirname(os.path.realpath(__file__))

# First item is the script directory or the empty string (for example,
# if running interactively) so we replace the first entry with the
# library directory.
sys.path[0] = os.path.join(script_dir, 'lib')

from unittest import (
    TestLoader,
    TextTestRunner,
    )

def get_options():
    from optparse import OptionParser
    parser = OptionParser()
    # TODO: Fix option parsing so that -vvv and --verbosity=3 give same effect.
    parser.add_option("-v", action="count", dest="verbosity",
                      help="Verbose mode. Multiple options increase verbosity")
    parser.add_option("--loglevel", action="store", dest="loglevel",
                      help="Set loglevel for debug output. "
                      "If not given, logging will be disabled.")
    return parser.parse_args()

def run_tests(pkg, opt, args):
    if len(args) == 0:
        import tests
        args = tests.__all__
    suite = TestLoader().loadTestsFromNames(pkg + '.' + mod for mod in args)
    return TextTestRunner(verbosity=opt.verbosity).run(suite)

if __name__ == '__main__':
    opt, args = get_options()
    if opt.loglevel is not None:
        logger = logging.getLogger("mysql.hub")
        logger.setLevel(opt.loglevel)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(levelname)s] %(asctime)s - %(threadName)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    result = run_tests('tests', opt, args)
    sys.exit(not result.wasSuccessful())

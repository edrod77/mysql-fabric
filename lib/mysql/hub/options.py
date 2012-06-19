import optparse

class DefaultOptionParser(optparse.OptionParser):
    """Option with default options for all tools.

    This class is used as default option parser and specific commands
    can add their own options.
    """
    pass

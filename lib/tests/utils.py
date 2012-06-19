"""Module holding support utilities for tests.
"""

import logging

class DummyManager(object):
    def __init__(self):
        handler = logging.NullHandler()
        self.logger = logging.getLogger('mysql.hub')
        self.logger.addHandler(handler)


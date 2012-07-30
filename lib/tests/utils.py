"""Module holding support utilities for tests.
"""

import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

class DummyManager(object):
    def __init__(self):
        handler = NullHandler()
        self.logger = logging.getLogger('mysql.hub')
        self.logger.addHandler(handler)


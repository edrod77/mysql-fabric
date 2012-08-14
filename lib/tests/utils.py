"""Module holding support utilities for tests.
"""
import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

class DummyManager(object):
    def __init__(self):
        self.executor = None
        self.server = None
        self.resource = None

_LOGGER = logging.getLogger('mysql.hub')
_LOGGER.addHandler(NullHandler())
_LOGGER = logging.getLogger('tests')
_LOGGER.addHandler(NullHandler())

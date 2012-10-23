"""Module holding support utilities for tests.
"""
class DummyManager(object):
    def __init__(self):
        self.executor = None
        self.server = None
        self.resource = None

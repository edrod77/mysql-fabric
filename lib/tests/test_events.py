"""Unit tests for the event handler.
"""

import types
import unittest
import xmlrpclib
import time

import mysql.hub.config as _config
import mysql.hub.core as _core
import mysql.hub.errors as _errors
import mysql.hub.events as _events
import mysql.hub.executor as _executor

import tests.utils as _utils

_TEST1 = None

def test1(job):
    global _TEST1
    _TEST1 = job.args[0]

class Callable(object):
    """A class acting as a callable.
    """
    def __init__(self, n=None):
        self.result = n
        self.__name__ = "Callable(%s)" % (n,)

    def __call__(self, job):
        self.result += job.args[0]


class TestHandler(unittest.TestCase):
    """Test event handler.
    """

    def setUp(self):
        manager = _utils.DummyManager()
        self.executor = _executor.Executor(manager)
        self.handler = _events.Handler(self.executor)
        self.executor.start()

    def tearDown(self):
        self.executor.shutdown()
        self.executor.join()

    def test_events(self):
        "Test creating events."

        self.assertEqual(_events.Event().name, None)
        self.assertEqual(_events.Event("Event").name, "Event")

    def test_register(self):
        "Test event registration functions."
        callables = [Callable(), Callable(), Callable()]

        self.assertFalse(self.handler.is_registered(_events.SERVER_LOST, test1))

        # Check registration of a function
        self.handler.register(_events.SERVER_LOST, test1)
        self.assertTrue(self.handler.is_registered(_events.SERVER_LOST, test1))

        # Check registration of a callable that is not a function
        self.handler.register(_events.SERVER_LOST, callables[0])

        # Check registration of a list of callables
        self.handler.register(_events.SERVER_LOST, callables[1:])

        # Check that all callables are now registered
        for obj in callables:
            self.assertTrue(
                self.handler.is_registered(_events.SERVER_LOST, obj)
                )

        # Check unregistration of a function
        self.handler.unregister(_events.SERVER_LOST, test1)
        self.assertFalse(
            self.handler.is_registered(_events.SERVER_LOST, test1)
            )
        self.assertRaises(
            _errors.UnknownCallableError,
            self.handler.unregister, _events.SERVER_LOST, test1
            )

        # Check unregistration of callables that are not functions
        for obj in callables:
            self.handler.unregister(_events.SERVER_LOST, obj)

        # Check that they are indeed gone
        for obj in callables:
            self.assertFalse(
                self.handler.is_registered(_events.SERVER_LOST, obj)
                )

        # Check that passing a non-event raises an exception
        self.assertRaises(_errors.NotEventError, self.handler.register,
                          list(), test1)
        self.assertRaises(_errors.NotEventError, self.handler.is_registered,
                          list(), test1)
        self.assertRaises(_errors.NotEventError, self.handler.unregister,
                          list(), test1)

        # Check that passing non-callables raise an exception
        self.assertRaises(_errors.NotCallableError, self.handler.register,
                          _events.SERVER_LOST, callables + [5])
        self.assertRaises(_errors.NotCallableError, self.handler.is_registered,
                          _events.SERVER_LOST, callables + [5])
        self.assertRaises(_errors.NotCallableError, self.handler.unregister,
                          _events.SERVER_LOST, callables + [5])

    def test_trigger(self):
        "Test that triggering an event dispatches jobs."

        global _TEST1

        # Register a function that we can check if it was called
        self.handler.register(_events.SERVER_LOST, test1)

        # Register a function and trigger it to see that the executor
        # really executed it.  When triggering the event, a list of
        # the jobs scheduled will be returned, so we iterate over the
        # list and wait until all jobs have been executed.
        _TEST1 = 0
        jobs = self.handler.trigger(_events.SERVER_LOST, 3)
        self.assertEqual(len(jobs), 1)
        for job in jobs:
            job.wait()
        self.assertEqual(_TEST1, 3)

        # Check that triggering an event by name works.
        _TEST1 = 0
        jobs = self.handler.trigger('SERVER_LOST', 4)
        self.assertEqual(len(jobs), 1)
        for job in jobs:
            job.wait()
        self.assertEqual(_TEST1, 4)

        # Check that temporary, unnamed events, also work by
        # registering a bunch of callables with a temporary event
        callables = [ Callable(n) for n in range(0, 2) ]
        my_event = _events.Event("TestEvent")
        self.handler.register(my_event, callables)

        # Trigger the event and wait for all jobs to finish
        jobs = self.handler.trigger(my_event, 3)
        for job in jobs:
            job.wait()

        for idx, obj in enumerate(callables):
            self.assertEqual(obj.result, idx + 3)

#
# Testing the decorator to see that it works
#

_PROMOTED = None
_DEMOTED = None

# Setting the handler used by the on_event decorator. This is a little
# contorted; can we find another way?
_EXECUTOR = _executor.Executor(_utils.DummyManager())
_HANDLER = _events.Handler(_EXECUTOR)

class TestDecorator(unittest.TestCase):
    "Test the decorators related to events"

    def setUp(self):
        _EXECUTOR.start()

    def tearDown(self):
        _EXECUTOR.shutdown()
        _EXECUTOR.join()

    def test_decorator(self):
        global _PROMOTED, _EXECUTOR, _DEMOTED
        _PROMOTED = None

        # Test decorator
        _PROMOTED = None
        jobs = _events._HANDLER.trigger(_events.SERVER_PROMOTED, "Testing")
        for job in jobs:
            job.wait()
        self.assertEqual(_PROMOTED, "Testing")


        # Test undo action for decorator
        _DEMOTED = None
        jobs = _events._HANDLER.trigger(_events.SERVER_DEMOTED, "Executing")
        for job in jobs:
            job.wait()
        self.assertEqual(_DEMOTED, "Undone")

# Testing that on_event decorator works as expected
@_events.on_event(_events.SERVER_PROMOTED)
def my_event(job):
    global _PROMOTED
    _PROMOTED = job.args[0]

# Testing that undo actions are really executed
_DEMOTED = None

@_events.on_event(_events.SERVER_DEMOTED)
def test2(job):
    global _DEMOTED
    _DEMOTED = job.args[0]
    raise NotImplementedError("Just not here")

@test2.undo
def test2_undo(job):
    global _DEMOTED
    _DEMOTED = "Undone"

class TestService(unittest.TestCase):
    "Test the service interface"

    def setUp(self):
        params = {
            'protocol.xmlrpc': {
                'address': 'localhost:13000'
                },
            }
        config = _config.Config(None, params, True)

        # Set up the manager
        self.manager = _core.Manager(config)
        self.manager.start()

        # Set up the client
        url = "http://%s" % (config.get("protocol.xmlrpc", "address"),)
        self.proxy = xmlrpclib.ServerProxy(url)

    def tearDown(self):
        self.manager.shutdown()
        self.manager.wait()

    def test_trigger(self):
        promoted = [None]
        def my_event(job):
            promoted[0] = job.args[0]
        self.manager.handler.register(_events.SERVER_PROMOTED, my_event)
        self.proxy.event.trigger('SERVER_PROMOTED', "my.example.com")
        # Need to wait for the job in the queue to finish, so we
        # access the private attribute directly.
        self.manager.executor._Executor__queue.join()
        self.assertEqual(promoted[0], "my.example.com")
        self.proxy.shutdown()

if __name__ == "__main__":
    unittest.main(argv=sys.argv)

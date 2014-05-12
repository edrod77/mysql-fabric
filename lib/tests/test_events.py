#
# Copyright (c) 2013,2014, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#

"""Unit tests for the event handler.
"""
import unittest
import tests.utils

from mysql.fabric import (
    errors as _errors,
    events as _events,
)

_NEW_SERVER_LOST = _events.Event("_NEW_SERVER_LOST")

_TEST1 = None

def test1(param, ignored):
    """A function acting as a callable.
    """
    global _TEST1
    _TEST1 = param

class Callable(object):
    """A class acting as a callable.
    """
    def __init__(self, n=None):
        """Constructor for Callable class.
        """
        self.result = n
        self.__name__ = "Callable(%s)" % (n,)

    def __call__(self, param, ignored):
        """Define a callable method.
        """
        self.result += param


class TestHandler(unittest.TestCase):
    """Test event handler.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.handler = _events.Handler()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_events(self):
        "Test creating events."
        self.assertEqual(_events.Event().name, None)
        self.assertEqual(_events.Event("Event").name, "Event")

    def test_register(self):
        "Test event registration functions."
        callables = [Callable(), Callable(), Callable()]

        self.assertFalse(self.handler.is_registered(_NEW_SERVER_LOST, test1))

        # Check registration of a function
        self.handler.register(_NEW_SERVER_LOST, test1)
        self.assertTrue(self.handler.is_registered(_NEW_SERVER_LOST, test1))

        # Check registration of a callable that is not a function
        self.handler.register(_NEW_SERVER_LOST, callables[0])

        # Check registration of a list of callables
        self.handler.register(_NEW_SERVER_LOST, callables[1:])

        # Check registration of an object that is not callable or iterable.
        self.assertRaises(
            _errors.NotCallableError,
            self.handler.register, _NEW_SERVER_LOST, None
            )

        # Check that all callables are now registered
        for obj in callables:
            self.assertTrue(
                self.handler.is_registered(_NEW_SERVER_LOST, obj)
                )

        # Check unregistration of a function
        self.handler.unregister(_NEW_SERVER_LOST, test1)
        self.assertFalse(
            self.handler.is_registered(_NEW_SERVER_LOST, test1)
            )
        self.assertRaises(
            _errors.UnknownCallableError,
            self.handler.unregister, _NEW_SERVER_LOST, test1
            )

        # Check unregistration of callables that are not functions
        for obj in callables:
            self.assertRaises(
                _errors.UnknownCallableError,
                self.handler.unregister, _NEW_SERVER_LOST, obj
                )

        # Check that they are indeed gone
        for obj in callables:
            self.assertFalse(
                self.handler.is_registered(_NEW_SERVER_LOST, obj)
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
                          _NEW_SERVER_LOST, callables + [5])
        self.assertRaises(_errors.NotCallableError, self.handler.is_registered,
                          _NEW_SERVER_LOST, callables + [5])
        self.assertRaises(_errors.NotCallableError, self.handler.unregister,
                          _NEW_SERVER_LOST, callables + [5])

    def test_trigger(self):
        "Test that triggering an event dispatches jobs."

        global _TEST1

        # Register a function that we can check if it was called
        self.handler.register(_NEW_SERVER_LOST, test1)

        # Register a function and trigger it to see that the executor
        # really executed it.  When triggering the event, a list of
        # the jobs scheduled will be returned, so we iterate over the
        # list and wait until all jobs have been executed.
        _TEST1 = 0
        jobs = self.handler.trigger(
            False, _NEW_SERVER_LOST, set(["lock"]), 3, ""
        )
        self.assertEqual(len(jobs), 1)
        for job in jobs:
            job.wait()
        self.assertEqual(_TEST1, 3)

        # Check that triggering an event by name works.
        _TEST1 = 0
        jobs = self.handler.trigger(
            False, "_NEW_SERVER_LOST", set(["lock"]), 4, ""
        )
        self.assertEqual(len(jobs), 1)
        for job in jobs:
            job.wait()
        self.assertEqual(_TEST1, 4)

        # Check that temporary, unnamed events, also work by
        # registering a bunch of callables with a temporary event
        callables = [ Callable(n) for n in range(0, 2) ]
        my_event_var = _events.Event("TestEvent")
        self.handler.register(my_event_var, callables)

        # Trigger the event and wait for all jobs to finish
        jobs = self.handler.trigger(
            False, my_event_var, set(["lock"]), 3, ""
        )
        for job in jobs:
            job.wait()

        for idx, obj in enumerate(callables):
            self.assertEqual(obj.result, idx + 3)

        # Try to trigger an unknown event.
        self.assertEqual(
            self.handler.trigger(False, "UNKNOWN_EVENT", None), []
        )

#
# Testing the decorator to see that it works
#
class TestDecorator(unittest.TestCase):
    """Test the decorators related to events.
    """
    handler = _events.Handler()

    def setUp(self):
        """Configure the existing environment
        """
        pass

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_decorator(self):
        """Test decorator related to events.
        """
        global _PROMOTED, _DEMOTED
        _PROMOTED = None

        # Test decorator
        _PROMOTED = None
        jobs = self.handler.trigger(
            False, _NEW_SERVER_PROMOTED, set(["lock"]), "Testing", ""
        )
        for job in jobs:
            job.wait()
        self.assertEqual(_PROMOTED, "Testing")

        # Test undo action for decorator
        _DEMOTED = None
        jobs = self.handler.trigger(
            False, _NEW_SERVER_DEMOTED, set(["lock"]), "Executing", ""
        )
        for job in jobs:
            job.wait()
        self.assertEqual(_DEMOTED, "Undone")


# Testing that on_event decorator works as expected
_PROMOTED = None
_NEW_SERVER_PROMOTED = _events.Event("_NEW_SERVER_PROMOTED")

@_events.on_event(_NEW_SERVER_PROMOTED)
def my_event(param, ignored):
    """Function that is called by a trigger.
    """
    global _PROMOTED
    _PROMOTED = param

# Testing that undo actions are really executed
_DEMOTED = None
_NEW_SERVER_DEMOTED = _events.Event("_NEW_SERVER_DEMOTED")

@_events.on_event(_NEW_SERVER_DEMOTED)
def test2(param, ignored):
    """Function that is called by a trigger.
    """
    global _DEMOTED
    _DEMOTED = param
    raise NotImplementedError("Just not here")

@test2.undo
def test2_undo(param, ignored):
    """Function that is called by a trigger.
    """
    global _DEMOTED
    _DEMOTED = "Undone"

class TestService(unittest.TestCase):
    """Test the service interface.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_trigger(self):
        """Test the trigger interface from the service perspective.
        """
        promoted = [None]
        def _another_my_event(param, ignored):
            """Decorator or Inner function.
            """
            promoted[0] = param
        _events.Handler().register(_NEW_SERVER_PROMOTED, _another_my_event)
        jobs = self.proxy.event.trigger(
            "_NEW_SERVER_PROMOTED", "lock_a, lock_b", "my.example.com", ""
        )
        try:
            self.proxy.event.wait_for_procedures(", ".join(jobs))
            self.assertEqual(promoted[0], "my.example.com")
        except Exception as error:
            if str(error).find("was not found") == -1:
                raise
        _events.Handler().unregister(_NEW_SERVER_PROMOTED, _another_my_event)

    def test_procedures(self):
        """Test the procedure interface from the service perspective.
        """
        try:
            self.proxy.event.wait_for_procedures(
                "e8ca0abe-cfdf-4699-a07d-8cb481f4670b"
                )
            self.assertTrue(False)
        except Exception as error:
            if str(error).find("was not found") == -1:
                raise

if __name__ == "__main__":
    unittest.main()

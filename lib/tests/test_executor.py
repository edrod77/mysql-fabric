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

"""Unit tests for the executor.
"""
import unittest
import logging
import uuid
import tests

from mysql.fabric import (
    executor as _executor,
    errors as _errors,
)

COUNT = []
OTHER = None

_LOGGER = logging.getLogger(__name__)

def test1():
    """Function to be called by a trigger.
    """
    global COUNT
    for cnt in range(10, 1, -1):
        COUNT.append(cnt)

def test2():
    """Function to be called by a trigger.
    """
    global OTHER
    OTHER = 47

class Action(object):
    """Callable class to be called by a trigger.
    """
    def __init__(self, expect):
        """Constructor for Action object.
        """
        self.expect = expect
        self.descr = "{0}({1})".format(self.__class__.__name__, expect)
        self.__name__ = self.descr
        self.result = None

    def __call__(self, param):
        """Callable method.
        """
        self.result = param

    def verify(self, test_case):
        """Function used to verify whether the callable was executed.
        """
        test_case.assertEqual(self.result, self.expect)

class TestExecutor(unittest.TestCase):
    """Test Executor.
    """
    def setUp(self):
        """Configure the existing environment
        """
        tests.utils.cleanup_environment()
        self.executor = _executor.Executor()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_start_executor(self):
        """Test the executor's start.
        """
        self.assertRaises(_errors.ExecutorError, self.executor.start)

    def test_basic(self):
        """Test the executor's and utilities' basic properties.
        """
        # Scheduling actions to be executed.
        proc_1 = self.executor.enqueue_procedure(
            False, test1, "Enqueuing action test1()", set(["lock"])
            )
        proc_1.wait()
        _LOGGER.debug("Procedure 1:"+ str(proc_1))
        proc_2 = self.executor.enqueue_procedure(
            False, test2, "Enqueuing action test2()", set(["lock"])
            )
        proc_2.wait()
        _LOGGER.debug("Procedure 2:"+ str(proc_2))

        # Check information on jobs
        proc = self.executor.get_procedure(proc_1.uuid)
        self.assertTrue(proc_1.uuid == proc.uuid)
        status = proc_1.status[-1]
        self.assertEqual(status["success"], _executor.Job.SUCCESS)
        self.assertEqual(status["state"], _executor.Job.COMPLETE)
        self.assertEqual(status["description"], "Executed action (test1).")

        proc = self.executor.get_procedure(proc_2.uuid)
        self.assertTrue(proc_2.uuid == proc.uuid)
        status = proc_2.status[-1]
        self.assertTrue(status["success"] == _executor.Job.SUCCESS)
        self.assertTrue(status["state"] == _executor.Job.COMPLETE)
        self.assertTrue(status["description"] == "Executed action (test2).")

        # Shutdown the executor and wait until its main thread returns.
        self.executor.shutdown()

        for cnt in range(10, 1, -1):
            self.assertTrue(cnt in COUNT)
        self.assertEqual(OTHER, 47)

        # Start the executor and wait until its main thread returns.
        self.executor.start()

    def test_job_hashable(self):
        """Test job's hasable property.
        """
        def action():
            """Inner function.
            """
            pass
        proc_1 = _executor.Procedure()
        job_1 = _executor.Job(proc_1, action, "Test action.", (), {})
        proc_2 = _executor.Procedure()
        job_2 = _executor.Job(proc_2, action, "Test action.", (), {})
        set_jobs = set()
        set_jobs.add(job_1)
        set_jobs.add(job_2)
        set_jobs.add(job_1)
        set_jobs.add(job_2)
        self.assertEqual(len(set_jobs), 2)
        self.assertEqual(job_1, job_1)
        self.assertNotEqual(job_1, job_2)

    def test_bad_cases(self):
        "Test that error cases are caught."
        # Check what happens when the Executor is not running.
        self.executor.shutdown()
        self.assertRaises(_errors.ExecutorError,
                          self.executor.enqueue_procedure,
                          False, 3, "Enqueue integer", set(["lock"]))

        # Check if the action is callable.
        self.executor.start()
        self.assertRaises(_errors.NotCallableError,
                          self.executor.enqueue_procedure,
                          False, 3, "Enqueue integer", set(["lock"]))

        # Check unknown job.
        proc = self.executor.get_procedure(
            uuid.UUID('{ab75a12a-98d1-414c-96af-9e9d4b179678}'))
        self.assertEqual(proc, None)

    def test_multi_dispatch(self):
        """Test that we can dispatch multiple events without waiting
        for them and then reap them afterwards.
        """
        # Enqueue several jobs at the same time.
        procs = []
        actions = []
        for num in range(1, 10, 2):
            action = Action(num)
            proc = self.executor.enqueue_procedure(
                False, action, action.descr, set(["lock"]),
                action.expect
            )
            procs.append(proc)
            actions.append(action)

        # Wait for all jobs and check that they update the Action
        # object to the correct value.
        for proc in procs:
            proc.wait()
        for action in actions:
            action.verify(self)


if __name__ == "__main__":
    unittest.main()

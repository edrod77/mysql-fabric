"""Unit tests for the executor.
"""
import unittest
import logging
import uuid

from mysql.hub import (
    executor as _executor,
    errors as _errors,
    persistence as _persistence,
    )

count = []
other = None

_LOGGER = logging.getLogger(__name__)

def test1():
    global count
    for cnt in range(10, 1, -1):
        count.append(cnt)

def test2():
    global other
    other = 47

class Action(object):
    def __init__(self, expect):
        self.expect = expect
        self.descr = "{0}({1})".format(self.__class__.__name__, expect)
        self.__name__ = self.descr
        self.result = None

    def __call__(self, param):
        self.result = param

    def verify(self, test_case):
        test_case.assertEqual(self.result, self.expect)

class TestExecutor(unittest.TestCase):
    """Test executor.
    """

    def setUp(self):
        from __main__ import options
        _persistence.init(host=options.host, port=options.port,
                          user=options.user, password=options.password)
        _persistence.setup()
        _persistence.init_thread()
        self.executor = _executor.Executor()

    def tearDown(self):
        _persistence.deinit_thread()
        _persistence.teardown()

    def test_start_executor(self):
        self.executor.start()
        self.assertRaises(_errors.ExecutorError, self.executor.start)
        self.executor.shutdown()

    def test_basic(self):
        self.executor.start()

        # Scheduling actions to be executed.
        proc_1 = self.executor.enqueue_procedure(
            False, test1, "Enqueuing action test1()"
            )
        proc_1.wait()
        _LOGGER.debug("Procedure 1:"+ str(proc_1))
        proc_2 = self.executor.enqueue_procedure(
            False, test2, "Enqueuing action test2()"
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
            self.assertTrue(cnt in count)
        self.assertEqual(other, 47)

    def test_job_hashable(self):
        def action():
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
        self.assertRaises(_errors.ExecutorError,
                          self.executor.enqueue_procedure,
                          False, 3, "Enqueue integer")

        # Check if the action is callable.
        self.executor.start()
        self.assertRaises(_errors.NotCallableError,
                          self.executor.enqueue_procedure,
                          False, 3, "Enqueue integer")
        self.executor.shutdown()

        # Check unknown job.
        proc = self.executor.get_procedure(
            uuid.UUID('{ab75a12a-98d1-414c-96af-9e9d4b179678}'))
        self.assertEqual(proc, None)


    def test_multi_dispatch(self):
        """Test that we can dispatch multiple events without waiting
        for them and then reap them afterwards.
        """
        self.executor.start()

        # Enqueue several jobs at the same time.
        procs = []
        actions = []
        for num in range(1, 10, 2):
            action = Action(num)
            proc = self.executor.enqueue_procedure(False, action, action.descr,
                                                   action.expect)
            procs.append(proc)
            actions.append(action)

        # Wait for all jobs and check that they update the Action
        # object to the correct value.
        for proc in procs:
            proc.wait()
        for action in actions:
            action.verify(self)

        self.executor.shutdown()


if __name__ == "__main__":
    unittest.main()

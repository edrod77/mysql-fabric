"""Unit tests for the executor.
"""
import mysql.hub.executor as _executor
import mysql.hub.errors as _errors
import unittest
import logging

from tests.utils import DummyManager

count = []
other = None
inc = 0

_LOGGER = logging.getLogger(__name__)

def test1(job):
    global count
    for cnt in range(10, 1, -1):
        count.append(cnt)

def test2(job):
    global other
    other = 47

@_executor.primitive
def test1_primitive(job):
    pass

@_executor.primitive
def test2_primitive(job):
    raise Exception("Error...")

@test2_primitive.undo
def test2_primitive_undo(job):
    global inc
    inc += 1

@_executor.primitive
def test3_primitive(job):
    raise Exception("Error...")

@_executor.coordinated
def test1_coordinated(job):
    global inc
    inc += 1

@_executor.coordinated
def test2_coordinated(job):
    global inc
    inc += 1
    raise Exception("Error...")

class TestExecutor(unittest.TestCase):
    """Test executor.
    """

    def setUp(self):
        manager = DummyManager()
        self.executor = _executor.Executor(manager)

    def test_basic(self):
        global other
        global count
        self.executor.start()

        # Scheduling actions to be executed.
        job_1 = self.executor.enqueue_job(test1,
                                          "Enqueuing action test1()", True)
        _LOGGER.debug("Job 1:"+ str(job_1))
        job_2 = self.executor.enqueue_job(test2,
                                          "Enqueuing action test2()", True)
        _LOGGER.debug("Job 2:"+ str(job_2))

        # Check information on jobs
        job = self.executor.get_job(job_1.uuid)
        self.assertTrue(job_1.uuid == job.uuid)
        self.assertTrue(job_1.status[-1]["success"] == \
                        _executor.Job.SUCCESS)
        self.assertTrue(job_1.status[-1]["state"] == \
                        _executor.Job.COMPLETE)
        self.assertTrue(job_1.status[-1]["description"] == \
                        "Executed action (test1).")

        job = self.executor.get_job(job_2.uuid)
        self.assertTrue(job_2.uuid == job.uuid)
        self.assertTrue(job_2.status[-1]["success"] == \
                        _executor.Job.SUCCESS)
        self.assertTrue(job_2.status[-1]["state"] == \
                        _executor.Job.COMPLETE)
        self.assertTrue(job_2.status[-1]["description"] == \
                        "Executed action (test2).")

        # Scheduling decorated actions to be executed.
        job_primitive_1 = self.executor.enqueue_job(test1_primitive,
                                                    "Enqueuing action test1()",
                                                    True)
        _LOGGER.debug("Job 1:"+ str(job_primitive_1))
        job_primitive_2 = self.executor.enqueue_job(test2_primitive,
                                                    "Enqueuing action test2()",
                                                    True)
        _LOGGER.debug("Job 2:"+ str(job_primitive_2))
        job_primitive_3 = self.executor.enqueue_job(test3_primitive,
                                                    "Enqueuing action test3()",
                                                    True)
        _LOGGER.debug("Job 3:"+ str(job_primitive_3))

        # Check information on jobs
        job = self.executor.get_job(job_primitive_1.uuid)
        self.assertTrue(job_primitive_1.uuid == job.uuid)
        self.assertTrue(job_primitive_1.status[-1]["success"] == \
                        _executor.Job.SUCCESS)
        self.assertTrue(job_primitive_1.status[-1]["state"] == \
                        _executor.Job.COMPLETE)
        self.assertTrue(job_primitive_1.status[-1]["description"] == \
                        "Executed action (test1_primitive).")
        job = self.executor.get_job(job_primitive_2.uuid)
        self.assertTrue(job_primitive_2.uuid == job.uuid)
        self.assertTrue(job_primitive_2.status[-1]["success"] == \
                        _executor.Job.ERROR)
        self.assertTrue(job_primitive_2.status[-1]["state"] == \
                        _executor.Job.COMPLETE)
        self.assertTrue(job_primitive_2.status[-1]["description"] == \
                        "Tried to execute action (test2_primitive).")
        job = self.executor.get_job(job_primitive_3.uuid)
        self.assertTrue(job_primitive_3.uuid == job.uuid)
        self.assertTrue(job_primitive_3.status[-1]["success"] == \
                        _executor.Job.ERROR)
        self.assertTrue(job_primitive_3.status[-1]["state"] == \
                        _executor.Job.COMPLETE)
        self.assertTrue(job_primitive_3.status[-1]["description"] == \
                        "Tried to execute action (test3_primitive).")
        self.assertEqual(inc, 1)

        # Scheduling coordinated actions to be executed.
        job_coordinated_1 = self.executor.enqueue_job(test1_coordinated,
                                                      "Enqueuing action test1()",
                                                      True)
        _LOGGER.debug("Job 1:"+ str(job_coordinated_1))
        job_coordinated_2 = self.executor.enqueue_job(test2_coordinated,
                                                      "Enqueuing action test2()",
                                                      True)
        _LOGGER.debug("Job 2:"+ str(job_coordinated_2))

        # Check information on jobs
        job = self.executor.get_job(job_coordinated_1.uuid)
        self.assertTrue(job_coordinated_1.uuid == job.uuid)
        self.assertTrue(job_coordinated_1.status[-1]["success"] == \
                        _executor.Job.SUCCESS)
        self.assertTrue(job_coordinated_1.status[-1]["state"] == \
                        _executor.Job.COMPLETE)
        self.assertTrue(job_coordinated_1.status[-1]["description"] == \
                        "Executed action (test1_coordinated).")
        job = self.executor.get_job(job_coordinated_2.uuid)
        self.assertTrue(job_coordinated_2.uuid == job.uuid)
        self.assertTrue(job_coordinated_2.status[-1]["success"] == \
                        _executor.Job.ERROR)
        self.assertTrue(job_coordinated_2.status[-1]["state"] == \
                        _executor.Job.COMPLETE)
        self.assertTrue(job_coordinated_2.status[-1]["description"] == \
                        "Tried to execute action (test2_coordinated).")
        self.assertEqual(inc, 3)

        # Shutdown the executor and wait until its main thread returns.
        self.executor.shutdown()
        self.executor.join()

        for cnt in range(10, 1, -1):
            self.assertTrue(cnt in count)
        self.assertEqual(other, 47)

if __name__ == "__main__":
    unittest.main()

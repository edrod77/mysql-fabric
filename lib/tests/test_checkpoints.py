"""Unit tests for the checkpoint/recovery.

"""
import unittest
import uuid as _uuid

from mysql.fabric import (
    events as _events,
    executor as _executor,
    persistence as _persistence,
    checkpoint as _checkpoint,
    recovery as _recovery,
    )

import tests.utils

EVENT_CHECK_PROPERTIES_1 = _events.Event("EVENT_CHECK_PROPERTIES_1")
EVENT_CHECK_PROPERTIES_2 = _events.Event("EVENT_CHECK_PROPERTIES_2")
EVENT_CHECK_PROPERTIES_3 = _events.Event("EVENT_CHECK_PROPERTIES_3")
EVENT_CHECK_PROPERTIES_4 = _events.Event("EVENT_CHECK_PROPERTIES_4")
EVENT_CHECK_PROPERTIES_5 = _events.Event("EVENT_CHECK_PROPERTIES_5")
EVENT_CHECK_PROPERTIES_6 = _events.Event("EVENT_CHECK_PROPERTIES_6")

EVENT_CHECK_CHAIN = _events.Event("EVENT_CHECK_CHAIN")

COUNT_1 = 0
COUNT_2 = 0

class MyTransAction(_persistence.Persistable):
    @staticmethod
    def create(persister=None):
        persister.exec_stmt("CREATE TABLE test_temporary(id TEXT)")

    @staticmethod
    def drop(persister=None):
        persister.exec_stmt("DROP TABLE test_temporary")

    @staticmethod
    def insert(text, persister=None):
        persister.exec_stmt("INSERT INTO test_temporary VALUES(%s)",
                            {"params":(text, )})

    @staticmethod
    def count(persister=None):
        return len(persister.exec_stmt("SELECT * FROM test_temporary"))

class TestPropertiesCheckpoint(unittest.TestCase):
    """This test case check basic properties from a checkpoint. There
    are four cases that we need to analyze:

    . proc(x) procedure whose uuid is x
    . job(y)  job whose uuid is y

    1 - Through a service or regular function, triggering an
        independent job.

        proc(1)
        job(n)

    2 - Through a service or regular function, triggering a set
        of independent jobs.

        proc(1), proc(2), ...
        job(n) , job(y) , ...

    3 - Within a job, triggering a simple independent job.

        proc(1)               proc(n)
        job(1)  --(trigger)-> job(n)

        Notice that the procedures are different.

    4 - Within a job, triggering a simple dependent job.

        proc(1)               proc(1)
        job(1)  --(trigger)-> job(n)

        Notice that the procedures are the same, although the
        jobs are different.

    5 - Within a job, triggering a set of independent jobs.

        proc(1)               proc(2), proc(3), ...
        job(1)  --(trigger)-> job(n) , job(y) , ...

    6 - Within a job, triggering a set of dependent jobs.

        proc(1)               proc(1), proc(1), ...
        job(1)  --(trigger)-> job(n) , job(y) , ...

    """
    def setUp(self):
        """Configure the existing environment
        """
        pass

    def tearDown(self):
        """Clean up the existing environment
        """
        _executor.Executor().shutdown()
        tests.utils.cleanup_environment()
        _executor.Executor().start()

    def test_properties_1(self):
        """1 - Through a service or regular function, triggering an
        independent job.
        """
        procedures = _events.trigger(
            EVENT_CHECK_PROPERTIES_1, set(["lock"]), "PARAM 01", "PARAM 02"
            )

        # Get the result (Checkpoint object) from the procedure.
        self.assertEqual(len(procedures), 1)
        result = None
        for procedure in procedures:
            procedure.wait()
            result = procedure.result

            # Fetch and check all the properties.
            self.assertEqual(len(result), 1)
            for checkpoint in result:
                self.assertEqual(checkpoint.param_args, ("PARAM 01", "PARAM 02"))
                self.assertEqual(checkpoint.param_kwargs, {})
                self.assertNotEqual(checkpoint.started, None)
                self.assertEqual(checkpoint.finished, None)
                self.assertEqual(checkpoint.do_action, check_properties_1)

            # There should not be any entry for this procedure.
            self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)

    def test_properties_2(self):
        """2 - Through a service or regular function, triggering a set
        of independent jobs.
        """
        procedures = _events.trigger(
            EVENT_CHECK_PROPERTIES_2, set(["lock"]), "PARAM 01", "PARAM 02"
            )

        # Get the result (Checkpoint object) from the procedure.
        self.assertEqual(len(procedures), 2)
        result = None
        for procedure in procedures:
            procedure.wait()
            result = procedure.result

            # Fetch and check all the properties.
            self.assertEqual(len(result), 1)
            for checkpoint in result:
                self.assertEqual(checkpoint.param_args, ("PARAM 01", "PARAM 02"))
                self.assertEqual(checkpoint.param_kwargs, {})
                self.assertNotEqual(checkpoint.started, None)
                self.assertEqual(checkpoint.finished, None)
                self.assertTrue(
                    checkpoint.do_action == check_properties_2_proc_1 or \
                    checkpoint.do_action == check_properties_2_proc_2)

        # There should not be any entry for this procedure.
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)

    def test_properties_3(self):
        """3 - Within a job, triggering a simple independent job.
        """
        procedures = _events.trigger(
            EVENT_CHECK_PROPERTIES_3, set(["lock"]), "PARAM 01", "PARAM 02"
            )

        # Get the result (Checkpoint object) from the procedure.
        self.assertEqual(len(procedures), 1)
        result = None
        for procedure in procedures:
            procedure.wait()
            result = procedure.result

            # Fetch and check all the properties.
            self.assertEqual(len(result), 1)
            for checkpoint in result:
                self.assertEqual(checkpoint.param_args, ("PARAM 01", "PARAM 02"))
                self.assertEqual(checkpoint.param_kwargs, {})
                self.assertNotEqual(checkpoint.started, None)
                self.assertEqual(checkpoint.finished, None)
                self.assertEqual(checkpoint.do_action, check_properties_3)

        # There should not be any entry for this procedure.
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)

    def test_properties_4(self):
        """4 - Within a job, triggering a simple dependent job.
        """
        procedures = _events.trigger(
            EVENT_CHECK_PROPERTIES_4, set(["lock"]), "PARAM 01", "PARAM 02"
            )

        # Get the result (Checkpoint object) from the procedure.
        self.assertEqual(len(procedures), 1)
        result = None
        for procedure in procedures:
            procedure.wait()
            result = procedure.result

            # Fetch and check all the properties.
            self.assertEqual(len(result), 2)
            for checkpoint in result:
                if checkpoint.do_action == check_properties_4:
                    self.assertEqual(checkpoint.param_args, ("PARAM 01", "PARAM 02"))
                    self.assertEqual(checkpoint.param_kwargs, {})
                    self.assertNotEqual(checkpoint.started, None)
                    self.assertNotEqual(checkpoint.finished, None)

                if checkpoint.do_action == check_properties_1:
                    self.assertEqual(checkpoint.param_args, ("NEW 01", "NEW 02"))
                    self.assertEqual(checkpoint.param_kwargs, {})
                    self.assertNotEqual(checkpoint.started, None)
                    self.assertEqual(checkpoint.finished, None)

        # There should not be any entry for this procedure.
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)

    def test_properties_5(self):
        """5 - Within a job, triggering a set of independent jobs.
        """
        procedures = _events.trigger(
            EVENT_CHECK_PROPERTIES_5, set(["lock"]), "PARAM 01", "PARAM 02"
            )

        # Get the result (Checkpoint object) from the procedure.
        self.assertEqual(len(procedures), 1)
        result = None
        for procedure in procedures:
            procedure.wait()
            result = procedure.result

            # Fetch and check all the properties.
            self.assertEqual(len(result), 1)
            for checkpoint in result:
                self.assertEqual(checkpoint.param_args, ("PARAM 01", "PARAM 02"))
                self.assertEqual(checkpoint.param_kwargs, {})
                self.assertNotEqual(checkpoint.started, None)
                self.assertEqual(checkpoint.finished, None)
                self.assertEqual(checkpoint.do_action, check_properties_5)

        # There should not be any entry for this procedure.
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)

    def test_properties_6(self):
        """6 - Within a job, triggering a set of dependent jobs.
        """
        procedures = _events.trigger(
            EVENT_CHECK_PROPERTIES_6, set(["lock"]), "PARAM 01", "PARAM 02"
            )

        # Get the result (Checkpoint object) from the procedure.
        self.assertEqual(len(procedures), 1)
        result = None
        for procedure in procedures:
            procedure.wait()
            result = procedure.result


            ctrl = False
            # Fetch and check all the properties.
            self.assertEqual(len(result), 3)
            for checkpoint in result:
                if checkpoint.do_action == check_properties_6:
                    self.assertEqual(checkpoint.param_args, ("PARAM 01", "PARAM 02"))
                    self.assertEqual(checkpoint.param_kwargs, {})
                    self.assertNotEqual(checkpoint.started, None)
                    self.assertNotEqual(checkpoint.finished, None)

                if checkpoint.do_action == check_properties_2_proc_1:
                    self.assertEqual(checkpoint.param_args, ("NEW 01", "NEW 02"))
                    self.assertEqual(checkpoint.param_kwargs, {})
                    self.assertNotEqual(checkpoint.started, None)
                    if checkpoint.finished:
                        assert(ctrl == False)

                if checkpoint.do_action == check_properties_2_proc_2:
                    self.assertEqual(checkpoint.param_args, ("NEW 01", "NEW 02"))
                    self.assertEqual(checkpoint.param_kwargs, {})
                    self.assertNotEqual(checkpoint.started, None)
                    if checkpoint.finished:
                        assert(ctrl == False)

        # There should not be any entry for this procedure.
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)


class TestRecoveryCheckpoint(unittest.TestCase):
    """This test case check checkpoint and recovery.

    """
    def setUp(self):
        """Configure the existing environment
        """
        self.persister = _persistence.current_persister()
        assert(self.persister is not None)

    def tearDown(self):
        """Clean up the existing environment
        """
        _executor.Executor().shutdown()
        tests.utils.cleanup_environment()
        _executor.Executor().start()

    def test_recovery_single_job(self):
        """Check checkpoint and recovery with a single job.
        """
        global COUNT_1, COUNT_2
        count_1 = 10
        count_2 = 30
        proc_uuid = _uuid.UUID("9f994e3a-a732-43ba-8aab-f1051f553437")
        lockable_objects = set(["lock"])
        job_uuid = _uuid.UUID("64835080-2114-46de-8fbf-8caba8e8cd90")
        do_action = check_do_action
        do_action_fqn = do_action.__module__ + "." + do_action.__name__
        args = (count_1, count_2)
        kwargs = {}

        # (FAILURE) BEGIN DO FINISH
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, lockable_objects, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.schedule()

        self.assertEqual(COUNT_1, 0)
        self.assertEqual(COUNT_2, 0)

        self.assertEqual(MyTransAction.count(), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 1)
        _checkpoint.Checkpoint.cleanup()
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 1)
        _recovery.recovery()
        executor = _executor.Executor()
        procedure = executor.get_procedure(checkpoint.proc_uuid)
        if procedure is not None:
            procedure.wait()
        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(MyTransAction.count(), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 0)
        executor.remove_procedure(proc_uuid)

        # BEGIN (FAILURE) DO FINISH
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, lockable_objects, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.schedule()
        checkpoint.begin()
        self.persister.begin()
        ####### empty #######
        self.persister.rollback()

        self.assertEqual(COUNT_1, 0)
        self.assertEqual(COUNT_2, 0)
        self.assertEqual(MyTransAction.count(), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 1)
        _checkpoint.Checkpoint.cleanup()
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 1)
        _recovery.recovery()
        executor = _executor.Executor()
        procedure = executor.get_procedure(checkpoint.proc_uuid)
        if procedure is not None:
            procedure.wait()
        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(MyTransAction.count(), 2)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 0)
        executor.remove_procedure(proc_uuid)

        # BEGIN DO (FAILURE) FINISH
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, lockable_objects, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.schedule()
        checkpoint.begin()
        self.persister.begin()
        do_action(10, 30)
        checkpoint.finish()
        self.persister.rollback()

        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(MyTransAction.count(), 2)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 1)
        _checkpoint.Checkpoint.cleanup()
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 1)
        _recovery.recovery()
        executor = _executor.Executor()
        procedure = executor.get_procedure(checkpoint.proc_uuid)
        if procedure is not None:
            procedure.wait()
        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(MyTransAction.count(), 3)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 0)
        executor.remove_procedure(proc_uuid)

        # BEGIN DO FINISH (FAILURE)
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, lockable_objects, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.schedule()
        checkpoint.begin()
        self.persister.begin()
        do_action(10, 30)
        checkpoint.finish()
        self.persister.commit()

        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(MyTransAction.count(), 4)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 1)
        _recovery.recovery()
        executor = _executor.Executor()
        procedure = executor.get_procedure(checkpoint.proc_uuid)
        if procedure is not None:
            procedure.wait()
        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(MyTransAction.count(), 4)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 0)
        executor.remove_procedure(proc_uuid)

    def test_recovery_chain_jobs(self):
        """Check checkpoint and recovery when a job triggers another
        job.
        """
        global COUNT_1, COUNT_2
        count_1 = 10
        count_2 = 30
        proc_uuid = _uuid.UUID("01da10ed-514e-43a4-8388-ab05c04d67e1")
        lockable_objects = set(["lock"])
        job_uuid = _uuid.UUID("e4e1ba17-ff1d-45e6-a83c-5655ea5bb646")
        job_uuid_scheduled_1 = _uuid.UUID("aaa1ba17-ff1d-45e6-a83c-5655ea5bb646")
        job_uuid_scheduled_2 = _uuid.UUID("bbb1ba17-ff1d-45e6-a83c-5655ea5bb646")
        do_action = check_do_action
        do_action_scheduled_1 = check_do_action_scheduled_1
        do_action_scheduled_2 = check_do_action_scheduled_2
        do_action_fqn = do_action.__module__ + "." + do_action.__name__
        do_action_scheduled_1_fqn = \
            do_action_scheduled_1.__module__ + "." + do_action_scheduled_1.__name__
        do_action_scheduled_2_fqn = \
            do_action_scheduled_2.__module__ + "." + do_action_scheduled_2.__name__
        args = (count_1, count_2)
        kwargs = {}

        # BEGIN DO FINISH (FAILURE)
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, lockable_objects, job_uuid, do_action_fqn, args, kwargs
            )
        scheduled_1 = _checkpoint.Checkpoint(
            proc_uuid, lockable_objects, job_uuid_scheduled_1,
            do_action_scheduled_1_fqn, args, kwargs
            )
        scheduled_2 = _checkpoint.Checkpoint(
            proc_uuid, lockable_objects, job_uuid_scheduled_2,
            do_action_scheduled_1_fqn, args, kwargs
            )
        checkpoint.schedule()
        checkpoint.begin()
        self.persister.begin()
        do_action(10, 30)
        checkpoint.finish()
        scheduled_1.schedule()
        scheduled_2.schedule()
        self.persister.commit()

        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(MyTransAction.count(), 1)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 2)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 3)
        _checkpoint.Checkpoint.cleanup()
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 3)
        _recovery.recovery()
        executor = _executor.Executor()
        procedure = executor.get_procedure(checkpoint.proc_uuid)
        if procedure is not None:
            procedure.wait()
        self.assertEqual(COUNT_1, 30)
        self.assertEqual(COUNT_2, 90)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.scheduled()), 0)
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(proc_uuid)), 0)
        executor.remove_procedure(proc_uuid)

@_events.on_event(EVENT_CHECK_PROPERTIES_1)
def check_properties_1(param_01, param_02):
    executor = _executor.Executor()
    job = _executor.ExecutorThread.executor_object().current_job
    checkpoint = _checkpoint.Checkpoint.fetch(job.procedure.uuid)
    return checkpoint

@_events.on_event(EVENT_CHECK_PROPERTIES_2)
def check_properties_2_proc_1(param_01, param_02):
    executor = _executor.Executor()
    job = _executor.ExecutorThread.executor_object().current_job
    checkpoint = _checkpoint.Checkpoint.fetch(job.procedure.uuid)
    return checkpoint

@_events.on_event(EVENT_CHECK_PROPERTIES_2)
def check_properties_2_proc_2(param_01, param_02):
    executor = _executor.Executor()
    job = _executor.ExecutorThread.executor_object().current_job
    checkpoint = _checkpoint.Checkpoint.fetch(job.procedure.uuid)
    return checkpoint

@_events.on_event(EVENT_CHECK_PROPERTIES_3)
def check_properties_3(param_01, param_02):
    _events.trigger(
        EVENT_CHECK_PROPERTIES_1, set(["lock"]), "NEW 01", "NEW 02"
        )

    executor = _executor.Executor()
    job = _executor.ExecutorThread.executor_object().current_job
    checkpoint = _checkpoint.Checkpoint.fetch(job.procedure.uuid)
    return checkpoint

@_events.on_event(EVENT_CHECK_PROPERTIES_4)
def check_properties_4(param_01, param_02):
    _events.trigger_within_procedure(
        EVENT_CHECK_PROPERTIES_1, "NEW 01", "NEW 02"
        )

@_events.on_event(EVENT_CHECK_PROPERTIES_5)
def check_properties_5(param_01, param_02):
    _events.trigger(
        EVENT_CHECK_PROPERTIES_2, set(["lock"]), "NEW 01", "NEW 02"
        )

    executor = _executor.Executor()
    job = _executor.ExecutorThread.executor_object().current_job
    checkpoint = _checkpoint.Checkpoint.fetch(job.procedure.uuid)
    return checkpoint

@_events.on_event(EVENT_CHECK_PROPERTIES_6)
def check_properties_6(param_01, param_02):
    _events.trigger_within_procedure(
        EVENT_CHECK_PROPERTIES_2, "NEW 01", "NEW 02"
        )

def non_trans_do_action(count_1, count_2):
    global COUNT_1, COUNT_2

    COUNT_1 += count_1
    if COUNT_1 != count_1:
        raise Exception("Error")

    COUNT_2 += count_2
    if COUNT_2 != count_2:
        raise Exception("Error")

def non_trans_undo_action():
    global COUNT_1, COUNT_2

    COUNT_1 = 0
    COUNT_2 = 0

def trans_do_action(text):
    MyTransAction.insert(text)

@_events.on_event(EVENT_CHECK_CHAIN)
def check_do_action(count_1, count_2):
    non_trans_do_action(count_1, count_2)
    trans_do_action("check_do_action")

@check_do_action.undo
def check_undo_action(count_1, count_2):
    non_trans_undo_action()

def check_do_action_scheduled_1(count_1, count_2):
    global COUNT_1, COUNT_2

    COUNT_1 += count_1
    COUNT_2 += count_2

def check_do_action_scheduled_2(count_1, count_2):
    global COUNT_1, COUNT_2

    COUNT_1 += count_1
    COUNT_2 += count_2

if __name__ == "__main__":
    unittest.main()

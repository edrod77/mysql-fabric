"""Unit tests for the event handler.
"""
import unittest
import uuid as _uuid

from mysql.hub import (
    events as _events,
    executor as _executor,
    persistence as _persistence,
    checkpoint as _checkpoint,
    recovery as _recovery,
    )

import tests.utils

EVENT_CHECK_PROPERTIES = _events.Event("EVENT_CHECK_PROPERTIES")

EVENT_CHECK_CHAIN_PROPERTIES_01 = _events.Event("EVENT_CHECK_CHAIN_PROPERTIES_01")
EVENT_CHECK_CHAIN_PROPERTIES_02 = _events.Event("EVENT_CHECK_CHAIN_PROPERTIES_02")

EVENT_CHECK_RECOVERY = _events.Event("EVENT_CHECK_RECOVERY")

EVENT_CHECK_UNDO = _events.Event("EVENT_CHECK_UNDO")

COUNT_1 = 0
COUNT_2 = 0

class TestCheckpoint(unittest.TestCase):
    "Test the decorators related to events"

    def setUp(self):
        self.manager, self.proxy = tests.utils.setup_xmlrpc()
        _persistence.init_thread()

    def tearDown(self):
        _persistence.deinit_thread()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_properties_one_job(self):
        # Trigger an event.
        procedures = _events.trigger(
            EVENT_CHECK_PROPERTIES, "PARAM 01", "PARAM 02"
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
            self.assertEqual(checkpoint.do_action, check_properties)

        # There should not be any entry for this procedure.
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)

    def test_properties_two_jobs(self):
        # Trigger an event.
        procedures = _events.trigger(
            EVENT_CHECK_CHAIN_PROPERTIES_01, "PARAM 01", "PARAM 02"
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
            if checkpoint.do_action == check_chain_properties_1:
                self.assertEqual(checkpoint.param_args, ("PARAM 01", "PARAM 02"))
                self.assertEqual(checkpoint.param_kwargs, {})
                self.assertNotEqual(checkpoint.started, None)
                self.assertNotEqual(checkpoint.finished, None)

            if checkpoint.do_action == check_chain_properties_2:
                self.assertEqual(checkpoint.param_args, ("NEW 01", "NEW 02"))
                self.assertEqual(checkpoint.param_kwargs, {})
                self.assertNotEqual(checkpoint.started, None)
                self.assertEqual(checkpoint.finished, None)

        # There should not be any entry for this procedure.
        self.assertEqual(len(_checkpoint.Checkpoint.fetch(procedure.uuid)), 0)

    def test_do_job_recovery(self):
        global COUNT_1, COUNT_2
        count_1 = 10
        count_2 = 30
        proc_uuid = _uuid.UUID("01da10ed-514e-43a4-8388-ab05c04d67e1")
        job_uuid = _uuid.UUID("e4e1ba17-ff1d-45e6-a83c-5655ea5bb646")
        do_action = check_do_action
        do_action_fqn = do_action.__module__ + "." + do_action.__name__
        args = (count_1, count_2)
        kwargs = {}

        # BEGIN (FAILURE) DO FINISH
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.begin()

        self.assertEqual(COUNT_1, 0)
        self.assertEqual(COUNT_2, 0)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 1)
        _recovery.recovery()
        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)

        # BEGIN DO (FAILURE) FINISH
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.begin()
        do_action(count_1, count_2)

        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 1)
        _recovery.recovery()
        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)

        # BEGIN DO FINISH (FAILURE)
        COUNT_1 = 0
        COUNT_2 = 0
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.begin()
        do_action(count_1, count_2)
        checkpoint.finish()

        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 1)
        _recovery.recovery()
        self.assertEqual(COUNT_1, 10)
        self.assertEqual(COUNT_2, 30)
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)

    def test_undo_job_recovery(self):
        global COUNT_1, COUNT_2
        count_1 = 20
        proc_uuid = _uuid.UUID("01da10ed-514e-43a4-8388-ab05c04d67e1")
        job_uuid = _uuid.UUID("e4e1ba17-ff1d-45e6-a83c-5655ea5bb646")
        do_action = do_check_undo
        do_action_fqn = do_action.__module__ + "." + do_action.__name__
        args = (count_1, )
        kwargs = {}

        # BEGIN DO (FAILURE) FINISH
        COUNT_1 = 0
        COUNT_2 = ""
        checkpoint = _checkpoint.Checkpoint(
            proc_uuid, job_uuid, do_action_fqn, args, kwargs
            )
        checkpoint.begin()
        do_action(count_1)

        self.assertEqual(COUNT_1, 20)
        self.assertEqual(COUNT_2, "Executed do")
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 1)
        _recovery.recovery()
        self.assertEqual(COUNT_1, 20)
        self.assertEqual(COUNT_2, "Executed undo, Executed do")
        self.assertEqual(len(_checkpoint.Checkpoint.unfinished()), 0)

@_events.on_event(EVENT_CHECK_PROPERTIES)
def check_properties(param_01, param_02):
    executor = _executor.Executor()
    job = executor.thread.current_job
    checkpoint = _checkpoint.Checkpoint.fetch(job.procedure.uuid)
    return checkpoint

@_events.on_event(EVENT_CHECK_CHAIN_PROPERTIES_01)
def check_chain_properties_1(param_01, param_02):
    _events.trigger_within_procedure(
        EVENT_CHECK_CHAIN_PROPERTIES_02, "NEW 01", "NEW 02"
        )

@_events.on_event(EVENT_CHECK_CHAIN_PROPERTIES_02)
def check_chain_properties_2(param_01, param_02):
    executor = _executor.Executor()
    job = executor.thread.current_job
    checkpoint = _checkpoint.Checkpoint.fetch(job.procedure.uuid)
    return checkpoint

@_events.on_event(EVENT_CHECK_RECOVERY)
def check_do_action(count_1, count_2):
    global COUNT_1, COUNT_2

    if COUNT_1 == 0:
       COUNT_1 += count_1
    elif COUNT_1 != count_1:
        raise Exception("Error")

    if COUNT_2 == 0:
       COUNT_2 += count_2
    elif COUNT_2 != count_2:
        raise Exception("Error")

@_events.on_event(EVENT_CHECK_UNDO)
def do_check_undo(count_1):
    global COUNT_1, COUNT_2

    COUNT_1 += count_1
    COUNT_2 += "Executed do"

@do_check_undo.undo
def undo_check_undo(count_1):
    global COUNT_1, COUNT_2

    COUNT_1 = 0
    COUNT_2 = "Executed undo, "

if __name__ == "__main__":
    unittest.main()

"""This module is responsible for ensuring that the system is in a
consistent state after a crash.
"""
import logging

import mysql.hub.executor as _executor
import mysql.hub.persistence as _persistence

from mysql.hub.checkpoint import (
    Checkpoint,
    )

_LOGGER = logging.getLogger(__name__)

def recovery():
    """Recover after a crash any incomplete procedure.

    It assumes that the executor is already running and that the recovery
    is sequential. In the future, we may consider optimizing this function.

    :return: False, if nothing bad happened while recovering. Otherwise,
             return True.
    """
    Checkpoint.cleanup()

    error = False
    for checkpoint in Checkpoint.unfinished():

        if checkpoint.undo_action:
            procedure = _executor.Executor().enqueue_procedure(
                False, checkpoint.undo_action,
                "Recovering %s." % (checkpoint.undo_action, ),
                *checkpoint.param_args, **checkpoint.param_kwargs
                )
            procedure.wait()
            if procedure.status[-1]['success'] != _executor.Job.SUCCESS:
                _LOGGER.error("Error while recovering %s.",
                (checkpoint.do_action, ))
                error = True

    actions = []
    procedure_uuid = None
    for checkpoint in Checkpoint.scheduled():

        actions.append({
            "job" : checkpoint.job_uuid,
            "action" : (checkpoint.do_action,
            "Recovering %s." % (checkpoint.do_action, ),
            checkpoint.param_args, checkpoint.param_kwargs)}
            )

        if procedure_uuid is not None and \
            procedure_uuid != checkpoint.proc_uuid:
            _executor.Executor().reschedule_procedure(
                procedure_uuid, actions
                )
            procedure_uuid = None
            actions = []

        procedure_uuid = checkpoint.proc_uuid

    if procedure_uuid is not None:
        _executor.Executor().reschedule_procedure(procedure_uuid, actions)

    return error

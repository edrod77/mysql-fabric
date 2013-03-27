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
                continue

        if checkpoint.do_action:
            checkpoint.finish()
            procedure = _executor.Executor().enqueue_procedure(
                checkpoint.proc_uuid, checkpoint.do_action,
                "Recovering %s." % (checkpoint.do_action, ),
                *checkpoint.param_args, **checkpoint.param_kwargs
                )
            procedure.wait()
            if procedure.status[-1]['success'] != _executor.Job.SUCCESS:
                _LOGGER.error("Error while recovering %s.",
                              (checkpoint.do_action, ))
                error = True

    procedures = []
    procedure_uuid = None
    for checkpoint in Checkpoint.scheduled():

        procedures.append({
            "job" : checkpoint.job_uuid,
            "action" : (checkpoint.do_action,
            "Recovering %s." % (checkpoint.do_action, ),
            checkpoint.param_args, checkpoint.param_kwargs)}
            )

        if procedure_uuid is not None and \
            procedure_uuid != checkpoint.proc_uuid:
            _executor.Executor().enqueue_scheduler(
                procedure_uuid, procedures
                )
            procedure_uuid = None
            procedures = []
        
        procedure_uuid = checkpoint.proc_uuid

    if procedure_uuid is not None:
        _executor.Executor().enqueue_scheduler(procedure_uuid, procedures)

    return error

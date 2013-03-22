"""This module is responsible for ensuring that the system is in a
consistent state after a crash.
"""
import logging

import mysql.hub.executor as _executor

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
    unfinished = Checkpoint.unfinished()

    for checkpoint in unfinished:

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
        else:
            error = True
            _LOGGER.warning("It is not possible to recover (%s) which was "
                            "a callable object.")

    return error

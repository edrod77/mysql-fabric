"""Command interface for working with events and procedures. It provides the
necessary means to trigger an event, to get details on a procedure and wait
until procedures finish their execution.
"""
import uuid as _uuid

from mysql.hub import (
    events as _events,
    executor as _executor,
    errors as _errors,
    )

from mysql.hub.command import (
    Command,
    )

class Trigger(Command):
    """Trigger an event.
    """
    command_name = "trigger"

    def execute(self, event, *args, **kwargs):
        """Trigger the execution of an event.

        :param event: Event's identification.
        :type event: String
        :param args: Event's non-keyworded arguments.
        :param kwargs: Event's keyworded arguments.
        :return: List of the procedures' uuids that were created.
        """
        return [ str(proc.uuid) \
                 for proc in _events.trigger(event, *args, **kwargs) ]

class WaitForProcedures(Command):
    """Wait until procedures finish their execution.

    If a procedure which is uniquely identified by a uuid is not found an
    error is returned.
    """
    command_name = "wait_for_procedures"

    def execute(self, proc_uuids):
        """Wait until a set of procedures uniquely identified by their uuids
        finish their execution.

        However, before starting waiting, the function checks if the procedures
        exist. If one of the procedures is not found, the following exception
        is raised :class:`mysql.hub.errors.ProcedureError`.

        :param proc_uuids: Iterable with procedures' uuids.
        """
        procs = []
        for proc_uuid in proc_uuids:
            proc_uuid = _uuid.UUID(proc_uuid)
            procedure = _executor.Executor().get_procedure(proc_uuid)
            if not procedure:
                raise _errors.ProcedureError("Procedure (%s) was not found." %
                                             (proc_uuid, ))
            procs.append(procedure)

        for procedure in procs:
            procedure.wait()

        return True

class WaitForProcedure(Command):
    """Wait until a procedure finishes its execution.

    If the procedure which is uniquely identified by a uuid is not found an
    error is returned.
    """
    command_name = "wait_for_procedure"

    def execute(self, proc_uuid):
        """Wait until a procedure uniquely identified by proc_uuid finishes its
        execution. If the procedure is not found the following exception is
        returned: :class:`mysql.hub.errors.ProcedureError`.

        :param proc_uuid: Procedure's uuid.
        :return: Procedure's status and result.
        """
        proc_uuid = _uuid.UUID(proc_uuid)
        procedure = _executor.Executor().get_procedure(proc_uuid)
        if not procedure:
            raise _errors.ProcedureError("Procedure (%s) was not found." %
                                         (proc_uuid, ))
        procedure.wait()
        return procedure.status, procedure.result

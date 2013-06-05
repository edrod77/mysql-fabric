"""Command interface for working with events and procedures. It provides the
necessary means to trigger an event, to get details on a procedure and wait
until procedures finish their execution.
"""
import uuid as _uuid

from mysql.fabric import (
    events as _events,
    executor as _executor,
    errors as _errors,
    )

from mysql.fabric.command import (
    Command,
    )

class Trigger(Command):
    """Trigger an event.
    """
    group_name = "event"
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
    """Wait until procedures, which are identified through their uuid in a
    list and separated by comma, finish their execution. If a procedure is
    not found an error is returned.
    """
    group_name = "event"
    command_name = "wait_for_procedures"

    def execute(self, proc_uuids):
        """Wait until a set of procedures uniquely identified by their uuids
        finish their execution.

        However, before starting waiting, the function checks if the procedures
        exist. If one of the procedures is not found, the following exception
        is raised :class:`mysql.fabric.errors.ProcedureError`.

        :param proc_uuids: Iterable with procedures' uuids.
        """
        procs = []
        it_proc_uuids = proc_uuids.split(",")
        for proc_uuid in it_proc_uuids:
            proc_uuid = _uuid.UUID(proc_uuid.strip())
            procedure = _executor.Executor().get_procedure(proc_uuid)
            if not procedure:
                raise _errors.ProcedureError("Procedure (%s) was not found." %
                                             (proc_uuid, ))
            procs.append(procedure)

        for procedure in procs:
            procedure.wait()

        return True

"""Command interface for working with events and jobs. It provides the
necessary means to trigger an event, to get details on a job and wait
until jobs finish their execution.
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

    def execute(self, event, *args):
        """Trigger an event.
        """
        return [ str(job.uuid) for job in _events.trigger(event, *args) ]

class WaitForJobs(Command):
    """Wait until jobs finish their execution.

    If a job which is uniquely identified by a uuid is not found an
    error is returned.
    """
    command_name = "wait_for_jobs"

    def execute(self, jobs):
        """Wait until jobs finish their execution.

        If a job is not found the :class:`mysql.hub.errors.JobError`
        exception is returned.

        :param job_uuid: Job's uuid.
        :return: Job's status.
        """
        for job in jobs:
            _wait_for_job(job)
        return False

class WaitForJob(Command):
    """Wait until a job finishes its execution.

    If the job which is uniquely identified by a uuid is not found an
    error is returned.
    """
    command_name = "wait_for_job"

    def execute(self, job_uuid):
        """Wait until a job finishes its execution.

        If the job is not found the :class:`mysql.hub.errors.JobError`
        exception is returned.

        :param job_uuid: Job's uuid.
        :return: Job's status.
        """
        return _wait_for_job(job_uuid)

def _wait_for_job(job_uuid):
    """Wait for a job.
    """
    executor = _executor.Executor()
    job_uuid = _uuid.UUID(job_uuid)
    job = executor.get_job(job_uuid)
    if not job:
        raise _errors.JobError("Job not found.")
    job.wait()
    return job.status

class JobDetails(Command):
    """Get information on a job.

    If the job which is uniquely identified by a uuid is not found an
    error is returned.
    """
    command_name = "job_details"

    def execute(self, job_uuid):
        """Get information on a job.

        If the job is not found the :class:`mysql.hub.errors.JobError`
        exception is returned.

        :param job_uuid: Job's uuid.
        :return: Job's status.
        """
        return _job_details(job_uuid)

def _job_details(job_uuid):
    """Get job's details.
    """
    executor = _executor.Executor()
    job_uuid = _uuid.UUID(job_uuid)
    job = executor.get_job(job_uuid)
    if not job:
        raise _errors.JobError("Job not found.")
    return job.status

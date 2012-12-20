"""Service interface for working with events and jobs. It provides the
necessary means to trigger an event, to get details on a job and wait
until jobs finish their execution.
"""

import uuid as _uuid

from mysql.hub import (
    events as _events,
    executor as _executor,
    errors as _errors,
    )

def trigger(event, *args):
    return [ str(job.uuid) for job in _events.trigger(event, *args) ]

def wait_for(jobs):
    executor = _executor.Executor()
    for job in jobs:
        wait_for_job(job)
    return False

def wait_for_job(job_uuid):
    """Wait until a job uniquely identified by job_uuid finishes its execution.
    If the job is not found the :class:`mysql.hub.errors.JobError` exception
    is returned.

    :param job_uuid: Job's uuid.
    :return: Job's status.
    """
    executor = _executor.Executor()
    job_uuid = _uuid.UUID(job_uuid)
    job = executor.get_job(job_uuid)
    if not job:
        raise _errors.JobError("Job not found.")
    job.wait()
    return job.status

def get_job_details(job_uuid):
    """Get information on job uniquely identified by job_uuid. If the job is
    not found the :class:`mysql.hub.errors.JobError` exception is returned.

    :param job_uuid: Job's uuid.
    :return: Job's status.
    """
    executor = _executor.Executor()
    job_uuid = _uuid.UUID(job_uuid)
    job = executor.get_job(job_uuid)
    if not job:
        raise _errors.JobError("Job not found.")
    return job.status

"""This module provides the necessary means to get details on a job and
wait until a job finishes its execution.
"""
import uuid as _uuid

import mysql.hub.errors as _errors
import mysql.hub.executor as _executor

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

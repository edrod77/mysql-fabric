"""Service interface for working with events.
"""

import uuid

from mysql.hub import (
    events as _events,
    executor as _executor,
    )

def trigger(event, *args):
    try:
        return [ str(job.uuid) for job in _events.trigger(event, *args) ]
    except Exception as error: # pylint: disable=W0703
        return error

def wait_for(jobs):
    executor = _executor.Executor()
    for job in jobs:
        try:
            job_uuid = uuid.UUID(job)
            executor.get_job(job_uuid).wait()
        except Exception as error:
            return error
    return False

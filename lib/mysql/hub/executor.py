import Queue
import threading
import logging
import uuid
import traceback

from weakref import WeakValueDictionary

import mysql.hub.errors as _errors

from mysql.hub.utils import Singleton

_LOGGER = logging.getLogger(__name__)


class Job(object):
    """Class responsible for storing information on a procedure and its
    execution's status.

    The executor process jobs, which has information on which procedures
    shall be executed and its execution's status. Jobs are uniquely
    identified so one can query the executor to figure out its outcome.

    """
    ERROR, SUCCESS = range(1, 3)
    EVENT_OUTCOME = [ERROR, SUCCESS]

    ENQUEUED, PROCESSING, COMPLETE = range(3, 6)
    EVENT_STATE = [ENQUEUED, PROCESSING, COMPLETE]

    def __init__(self, persister, action, description, args):
        """Constructor for the job.

        The action is a callback function and if the caller wants to be blocked
        while the job is being scheduled and than processed the parameter sync
        must be set to true.

        """
        if not callable(action):
            raise _errors.NotCallableError("Callable expected")

        self.__uuid = uuid.uuid4()
        self.__action = action
        self.__lock = threading.Condition()
        self.__complete = False
        self.__status = []
        self.__args = args or []
        self.result = None
        self.jobs = []
        self.__persister = persister
        self.add_status(Job.SUCCESS, Job.ENQUEUED, description)

    def merge_status(self, status):
        """Given a list of statuses, merge it into the current job.

        :param status: List of statuses.
        """
        assert(isinstance(status, list))
        with self.__lock:
            self.__status.extend(status)

    def add_status(self, success, state, description, diagnosis=False):
        """Add a new status to this job.

        A status has the following format::

          status = {
            "success" : success,
            "state" : state,
            "description" : description,
            "diagnosis" : "" if not diagnosis else \\
                          traceback.format_exc()
          }

        :param success: Execution's outcome.
        :param state: Execution's state.
        :param description: Description of the state.
        :type success: Member of Job.EVENT_OUTCOME
        :type state: Member of Job.EVENT_STATE

        """
        try:
            self.__lock.acquire()
            assert(success in Job.EVENT_OUTCOME)
            assert(state in Job.EVENT_STATE)
            status = {
                      "success" : success,
                      "state" : state,
                      "description" : description,
                      "diagnosis" : "" if not diagnosis else \
                                    traceback.format_exc(),
            }
            self.__status.append(status)
        finally:
            self.__lock.release()

    def wait(self):
        """Block the caller until the job is complete.
        """
        _LOGGER.debug("Waiting for %s", self.uuid)
        if self.__complete:
            return
        self.__lock.acquire()
        while not self.__complete:
            self.__lock.wait()
        self.__lock.release()

    def notify(self):
        """Notify blocked caller that the job is complete.
        """
        _LOGGER.debug("Completing job %s", self.uuid)
        self.__lock.acquire()
        self.__complete = True
        self.__lock.notify_all()
        self.__lock.release()

    def __eq__(self,  other):
        """Two jobs are equal if they have the same uuid.
        """
        return isinstance(other, Job) and self.__uuid == other.uuid

    def __hash__(self):
        """A job is hashable through its uuid.
        """
        return hash(self.__uuid)

    def execute(self):
        """Execute the job.
        """
        return self.__action(self)

    @property
    def uuid(self):
        """Return the job's uuid.
        """
        return self.__uuid

    @property
    def complete(self):
        "Is the job complete?"
        return self.__complete

    @property
    def action(self):
        """Return a reference to the callable that is called by the
        executor.
        """
        return self.__action

    @property
    def args(self):
        """Return the arguments passed when enqueing the job.

        The arguments are always an iterable.
        """
        return self.__args

    @property
    def status(self):
        """Return a reference to the dictionary where the job's statuses are
        stored.
        """
        return self.__status

    @property
    def persister(self):
        """Return a reference to the persister object which is used to access
        the state store.
        """
        return self.__persister

    def __str__(self):
        """Return a description on the job: <Job object: uuid=..., status=...>.
        """
        ret = "<Job object: " + \
               "uuid=" + str(self.__uuid) + ", " + \
               "status=" + str(self.__status) + \
               ">"
        return ret


class Executor(Singleton):
    """Class responsible for dispatching execution of procedures.

    Procedures to be executed are queued to the executor, which then
    will execute them in order.
    """
    def __init__(self):
        super(Executor, self).__init__()
        self.__queue = Queue.Queue()
        self.__jobs_lock = threading.RLock()
        self.__jobs = WeakValueDictionary()
        self.__thread_lock = threading.RLock()
        self.__thread = None
        self.persister = None

    def _run(self):
        """Run the executor thread.

        Read callable objects from the queue and call them.
        """
        while True:
            job = self.__queue.get(block=True)
            _LOGGER.debug("Reading next job from queue, found %s.", job)
            if job is None:
                self.__queue.task_done()
                break
            try:
                job.result = False
                if self.persister:
                    self.persister.begin()
                result = job.execute()
                if result is not None:
                    job.result = result
            except Exception as error: # pylint: disable=W0703
                _LOGGER.exception(error)
                job.add_status(job.ERROR, job.COMPLETE,
                "Tried to execute action ({0}).".format(job.action.__name__),
                True)
                if self.persister:
                    self.persister.rollback()
            else:
                job.add_status(job.SUCCESS, job.COMPLETE,
                "Executed action ({0}).".format(job.action.__name__))
                if self.persister:
                    self.persister.commit()
            finally:
                self.__queue.task_done()
                job.notify()

    def start(self):
        """Start the executor.
        """
        with self.__thread_lock:
            if not self.__thread:
                self.__thread = threading.Thread(target=self._run,
                                                 name="Executor")
                self.__thread.start()
            else:
                raise _errors.ExecutorError("Executor is already running.")

    def shutdown(self):
        """Shut down the executor.
        """
        _LOGGER.info("Shutting down Executor.")
        with self.__thread_lock:
            if self.__thread and self.__thread.is_alive():
                self.__queue.put(None)
                self.__thread.join()
            self.__thread = None

    # TODO: args should be *args, **kwargs. ? ? ?
    def enqueue_job(self, action, description, args=None):
        """Schedule a job to be executed.

        :param action: Callable to execute.
        :param description: Description of the job.
        :param args: Arguments to pass to the job.
        :return: Reference to a job that was scheduled.
        :rtype: Job
        """
        with self.__thread_lock:
            if self.__thread and self.__thread.is_alive():
                job = Job(self.persister, action, description, args)
                _LOGGER.debug("Created job (%s) whose description is (%s).",
                              str(job.uuid), description)
                with self.__jobs_lock:
                    self.__jobs[job.uuid] = job
                self.__queue.put(job)
                _LOGGER.debug("Enqueued job (%s).", str(job.uuid))
            else:
                raise _errors.ExecutorError("Executor is not running.")
        return job

    def get_job(self, job_uuid):
        """Retrieve a reference to a job.
        """
        assert(isinstance(job_uuid, uuid.UUID))
        _LOGGER.debug("Checking job (%s).", str(job_uuid))
        try:
            with self.__jobs_lock:
                job = self.__jobs[job_uuid]
        except (KeyError, ValueError) as error:
            _LOGGER.exception(error)
            job = None

        return job

# TODO: Is this the best place to add this function?
# TODO: We need to revisit the format of the information returned
#       after executing a service.
def process_jobs(jobs, synchronous):
    """Wait until a list of jobs complete its execution and
    return information on the latest job in the list.

    :param jobs: List of jobs.
    :param synchronous: Whether should wait until all the
                        jobs finish their execution or not.
    :return: Information on the latest job executed.
    :rtype: str(job.uuid), job.status, job.result.
    """
    if synchronous:
        for job in jobs:
            job.wait()
            if job.jobs:
                ret = process_jobs(job.jobs, synchronous)
                job.merge_status(ret[1])
    return str(jobs[-1].uuid), jobs[-1].status, jobs[-1].result

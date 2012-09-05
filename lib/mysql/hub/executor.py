import Queue
import threading
import logging
import uuid
import traceback

from functools import wraps
from weakref import WeakValueDictionary

import mysql.hub.errors as _errors

_LOGGER = logging.getLogger(__name__)


def primitive(func):
    """Decorator for decorating primitives.

    Primitives are the atomic primitives that make up the execution
    machinery.::

        @primitive
        def write_status(server):
           server.sql("INSERT INTO status VALUES (%d, 'In Progress')",
                      server.id)

        @write_status.undo
        def unwrite_status(server):
           server.sql("DELETE FROM status VALUES (%d, 'Undone')",
           server.id)
    """

    def undo_decorate(undo_func):
        """ This is the undo decorator for the function.

        It is used to define an undo decorator which is used to set a
        function that shall be called if the main execution fails.

        """
        func.compensate = undo_func

    @wraps(func)
    def execute(*args, **kwrd):
        """ This is the function that executes the primitive and
        handles any errors.
        """
        try:
            _LOGGER.debug("Executing %s", func.__name__)
            func(*args, **kwrd)
        except Exception as error:       # pylint: disable=W0703
            _LOGGER.debug("%s failed, executing compensation", func.__name__)
            _LOGGER.exception(error)
            if func.compensate is not None:
                func.compensate(*args, **kwrd)
            raise

    func.compensate = None
    execute.undo = undo_decorate
    return execute


def coordinated(func):
    """Decorator for defining coordinated function execution.

    Coordinated functions will automatically coordinate across several
    instances to ensure that the execution can fail over.::

      @coordinated
      def do_something(server):
         server.whatever()
         server.something_else()

    """
    @wraps(func)
    def coord_and_exec(*args, **kwrd):
        _LOGGER.debug("Begin transactional execution of %s.", func.__name__)
        try:
            commit = False
            func(*args, **kwrd)
            commit = True
        finally:
            if commit:
                _LOGGER.debug("Commit transactional execution of %s.",
                              func.__name__)
            else:
                _LOGGER.debug("Rollback transactional execution of %s.",
                              func.__name__)
        return commit
    return coord_and_exec


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

    def __init__(self, action, description, args):
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
        self.__args = [] if args is None else args
        self.add_status(Job.SUCCESS, Job.ENQUEUED, description)

    def add_status(self, success, state, description, diagnosis=False):
        """Add a new status to this job.

        A status has the following format::
           { "success" : success,
             "state" : state,
             "description" : description,
             "diagnosis" : trace
           }

        """
        try:
            self.__lock.acquire()
            assert(success in Job.EVENT_OUTCOME)
            assert(state in Job.EVENT_STATE)
            if not diagnosis:
                trace = ""
            else:
                trace = traceback.format_exc()
            status = { "success" : success,
                       "state" : state,
                       "description" : description,
                       "diagnosis" : trace
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
        if self.__complete:
           return
        self.__lock.acquire()
        self.__complete = True
        self.__lock.notify_all()
        self.__lock.release()

    def __eq__(self,  other):
        """Define that two jobs are equal if they have the same uuid.
        """
        return self.__uuid == other.uuid

    def __neq__(self,  other):
        """Define that two jobs are not equal if they do not have the same
        uuid.
        """
        return self.__uuid != other.uuid

    def __hash__(self):
        """Define that a job is hashable through the uuid.
        """
        return hash(self.__uuid)

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

    def __str__(self):
        """Return a description on the job: <Job object: uuid=..., status=...>.
        """
        ret = "<Job object: " + \
               "uuid=" + str(self.__uuid) + ", " + \
               "status=" + str(self.__status) + \
               ">"
        return ret


class Executor(threading.Thread):
    """Class responsible for dispatching execution of procedures.

    Procedures to be executed are queued to the executor, which then
    will execute them in order.

    """

    def __init__(self, manager):
        super(Executor, self).__init__(name="Executor")
        self.__queue = Queue.Queue()
        self.__manager = manager
        self.__jobs = WeakValueDictionary()

    def run(self):
        """Run the executor thread.

        Right now, it only read objects from the queue and call them
        (if they are callable).

        """

        while True:
            job = self.__queue.get(block=True)
            _LOGGER.debug("Reading next job from queue, found %s.", job)
            if job is None:
                break
            try:
                job.action(job)
            except Exception as error:
                _LOGGER.exception(error)
                job.add_status(job.ERROR, job.COMPLETE,
                "Tried to execute action ({0}).".format(job.action.__name__),
                True)
            else:
                job.add_status(job.SUCCESS, job.COMPLETE,
                "Executed action ({0}).".format(job.action.__name__))
            finally:
                job.notify()

        # The current shutdown routine is not 100% clean in the sense
        # that jobs may be scheduled after requesting the shutdown.
        # Notice however that the jobs shecduled before the shutdown
        # being requested are processed.
        # Maybe we should define a safe and an immediate shutdown.
        _LOGGER.debug("Checking if there is unprocessed jobs.")
        try:
            while True:
                job = self.__queue.get_nowait()
                _LOGGER.debug("Unprocessed job from queue, found job "
                              "(%s).", job.uuid)
        except Queue.Empty as error:
            pass

    def shutdown(self):
        """Shut down the executor.
        """
        _LOGGER.info("Shutting down executor.")
        self.__queue.put(None)

    def enqueue_job(self, action, description, sync=False, args=None):
        """Schedule a job to be executed.

        The action is a callback function and if the caller wants to
        be blocked while the job is being scheduled and than processed
        the parameter sync must be set to true.

        :param action: Callable to execute.
        :param description: Description of the job.
        :param sync: If True, the caller will be blocked until the job
                     has finished. If False, the function will return
                     immediately.
        :param args: Arguments to pass to the job.
        :return: Reference to job that was scheduled
        :rtype: Job
        """
        #TODO: Check for concurrency issues.
        job = Job(action, description, args)
        _LOGGER.debug("Created job (%s) whose description is (%s).",
                      str(job.uuid), description)
        self.__jobs[job.uuid] = job
        self.__queue.put(job)
        _LOGGER.debug("Enqueued job (%s).", str(job.uuid))
        if sync:
            job.wait()
        return job

    def get_job(self, job_uuid):
        """Retrieve a reference to a job.
        """
        assert(isinstance(job_uuid, uuid.UUID))
        _LOGGER.debug("Checking job (%s).", str(job_uuid))
        #TODO: Check for concurrency issues.
        try:
            job = self.__jobs[job_uuid]
        except (KeyError, ValueError) as error:
            _LOGGER.exception(error)
            job = None

        return job

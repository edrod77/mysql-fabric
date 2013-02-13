import Queue
import threading
import logging
import uuid
import traceback
import time

from weakref import WeakValueDictionary

import mysql.hub.persistence as _persistence
import mysql.hub.errors as _errors

from mysql.hub.utils import Singleton

_LOGGER = logging.getLogger(__name__)

class Procedure(object):
    """Defines the context within which an operation is executed. Explicitly,
    an operation is a code block to be executed and is named a job.

    Any job must belong to a procedure whereas a procedure may have several
    jobs associated to it. When job is created and is about to be scheduled,
    it is added to a set of scheduled jobs. Upon the end of its execution,
    it is moved from the aforementioned set to a list of executed jobs. 
    During the execution of a job, new jobs may be scheduled in the context
    of the current procedure.

    A procedure is marked as finished (i.e. complete) when its last job
    finishes. Specifically, when a job finishes and there is no scheduled
    job on behalf of the procedure.
        
    This class is mainly used to keep track of requests and to provide the
    necessary means to build a synchronous execution.
    """
    def __init__(self):
        """Create a Procedure object.
        """
        self.__uuid = uuid.uuid4()
        self.__lock = threading.Condition()
        self.__complete = False
        self.__result = False
        self.__scheduled_jobs = set()
        self.__executed_jobs = []
        self.__status = []

    def job_scheduled(self, job):
        """Register that a job has been scheduled on behalf of the
        procedeure.

        :param job: Scheduled job.
        """
        with self.__lock:
            assert(not self.__complete)
            assert(job not in self.__scheduled_jobs)
            assert(job not in self.__executed_jobs)
            assert(job.procedure == self)

            self.__scheduled_jobs.add(job) 

    def add_executed_job(self, job):
        """Register that a job has been executed on behalf of the
        procedure.

        :param job: Executed job.
        """
        with self.__lock:
            assert(not self.__complete)
            assert(job in self.__scheduled_jobs)
            assert(job not in self.__executed_jobs)
            assert(job.procedure == self)

            self.__scheduled_jobs.remove(job) 
            self.__executed_jobs.append(job)

            if job.result is not None:
                self.__result = job.result
            self.__status.extend(job.status)

            if not self.__scheduled_jobs:
                self.__complete = True
                self.__lock.notify_all()

    @property
    def uuid(self):
        """Return the procedure's uuid.
        """
        return self.__uuid

    @property
    def status(self):
        """Return the procedure's status which is a list of the
        statuses of all processes executed.
        """
        with self.__lock:
            assert(self.__complete)
            return self.__status

    @property
    def result(self):
        """Return the procedure's result which is the result of the
        last process executed on behalf of the procedure.
        """
        with self.__lock:
            assert(self.__complete)
            return self.__result

    def wait(self):
        """Wait until the procedure finishes its execution.
        """
        self.__lock.acquire()
        while not self.__complete:
            self.__lock.wait()
        self.__lock.release()


class Job(object):
    """Encapuslate a code block and is scheduled through the executor within
    the context of a procedure.
    """
    ERROR, SUCCESS = range(1, 3)
    EVENT_OUTCOME = [ERROR, SUCCESS]

    ENQUEUED, PROCESSING, COMPLETE = range(3, 6)
    EVENT_STATE = [ENQUEUED, PROCESSING, COMPLETE]

    def __init__(self, procedure, action, description, args, kwargs):
        """Create a Job object.
        """
        if not callable(action):
            raise _errors.NotCallableError("Callable expected")

        self.__uuid = uuid.uuid4()
        self.__action = action
        self.__args = args or []
        self.__kwargs = kwargs or {}
        self.__status = []
        self.__result = None
        self.__complete = False
        self.__procedure = procedure

        self._add_status(Job.SUCCESS, Job.ENQUEUED, description)
        self.__procedure.job_scheduled(self)        

    @property
    def uuid(self):
        """Return the job's uuid.
        """
        return self.__uuid

    @property
    def procedure(self):
        """Return a reference to the procedure which the job is
        associated to.
        """
        return self.__procedure

    @property
    def status(self):
        """Return the status of the execution phases (i.e. scheduled,
        processing, completed).

        A status has the following format::

          status = {
            "when": time,
            "state" : state,
            "success" : success,
            "description" : description,
            "diagnosis" : "" if not diagnosis else \\
                          traceback.format_exc()
          }
        """
        assert(self.__complete)
        return self.__status

    @property
    def result(self):
        """Return the job's result.
        """
        assert(self.__complete)
        return self.__result

    def _add_status(self, success, state, description, diagnosis=False):
        """Add a new status to this job.
        """
        assert(success in Job.EVENT_OUTCOME)
        assert(state in Job.EVENT_STATE)
        status = {
            "when" : time.time(),
            "state" : state,
            "success" : success,
            "description" : description,
            "diagnosis" : "" if not diagnosis else traceback.format_exc(),
            }
        self.__status.append(status)

    def execute(self, persister):
        """Execute the job.
        """
        try:
            persister.begin()
            self.__result = self.__action(*self.__args, **self.__kwargs)
        except Exception as error: # pylint: disable=W0703
            # TODO: The rollback and commit cannot fail. Otherwise, there will
            # be problems. This can be broken easily, for example by
            # calling "SELECT * FROM TABLE WHERE name = %s" % (False, )
            # 
            # What does it happen if the connection is idle for a long
            # time?
            #
            # We need to investigate this.
            #
            _LOGGER.exception(error)
            message = "Tried to execute action ({0}).".format(
                self.__action.__name__)
            self._add_status(Job.ERROR, Job.COMPLETE, message, True)
            persister.rollback()
        else:
            message = "Executed action ({0}).".format(self.__action.__name__)
            self._add_status(Job.SUCCESS, Job.COMPLETE, message)
            persister.commit()
        finally:
            self.__complete = True
            self.__procedure.add_executed_job(self)

    def __eq__(self,  other):
        """Two jobs are equal if they have the same uuid.
        """
        return isinstance(other, Job) and self.__uuid == other.uuid

    def __hash__(self):
        """A job is hashable through its uuid.
        """
        return hash(self.__uuid)

    def __str__(self):
        """Return a description on the job: <Job object: uuid=..., status=...>.
        """
        ret = "<Job object: " + \
               "uuid=" + str(self.__uuid) + ", " + \
               "status=" + str(self.__status) + \
               ">"
        return ret


class ExecutorThread(threading.Thread):
    """Class representing an executor thread for executing jobs.

    The thread will repeatedly read from the executor queue and
    execute a job. Note that the job queue is shared between all
    thread instances.

    Each thread will create a persister and register it with the
    persistance system so that objects manipulated as part of the job
    execution can be persisted to the persistent store.

    :param Queue.Queue queue: Queue to read jobs from.
    """
    def __init__(self, queue):
        "Constructor for ExecutorThread."
        super(ExecutorThread, self).__init__(name="Executor")
        self.__queue = queue
        self.__persister = None
        self.__job = None
        self.__current_thread = None

    def is_current_thread(self):
        """Check if the current thread is the same as the executor's thread.
        """
        return self.__current_thread == threading.current_thread()

    @property
    def current_job(self):
        """Return a reference to the current job.
        """
        assert(self.__current_thread == threading.current_thread())
        return self.__job

    def run(self):
        """Run the executor thread.

        This function will repeatedly read jobs from the queue and
        execute them.
        """
        _LOGGER.debug("Initializing Executor thread %s", self.name)
        self.__persister = _persistence.MySQLPersister()
        _persistence.PersistentMeta.init_thread(self.__persister)
        
        self.__current_thread = threading.current_thread()

        while True:
            self.__job = self.__queue.get(block=True)
            _LOGGER.debug("Reading next job from queue, found %s.", self.__job)

            if self.__job is None:
                self.__queue.task_done()
                break

            self.__job.execute(self.__persister)
            self.__queue.task_done()


class Executor(Singleton):
    """Class responsible for dispatching execution of procedures.

    Procedures to be executed are queued to the executor, which then
    will execute them in order.
    """
    def __init__(self):
        super(Executor, self).__init__()
        self.__queue = Queue.Queue()
        self.__procedures_lock = threading.RLock()
        self.__procedures = WeakValueDictionary()
        self.__thread_lock = threading.RLock()
        self.__thread = None

    def start(self):
        """Start the executor.
        """
        with self.__thread_lock:
            _LOGGER.info("Starting Executor")
            if not self.__thread:
                self.__thread = ExecutorThread(self.__queue)
                self.__thread.start()
                _LOGGER.info("Executor started")
            else:
                raise _errors.ExecutorError("Executor is already running.")

    def shutdown(self):
        """Shut down the executor.
        """
        _LOGGER.info("Shutting down Executor.")
        thread = None
        with self.__thread_lock:
            if self.__thread and self.__thread.is_alive():
                self.__queue.put(None)
                thread = self.__thread
            self.__thread = None
        if thread:
            _LOGGER.debug("Waiting until the Executor stops.")
            thread.join()
        _LOGGER.info("Executor has stopped")

    def enqueue_procedure(self, within_procedure, action, description, *args,
                          **kwargs):
        """Schedule a job on behalf of a procedured.

        :within_procedure: Define if a new procedure will be created or not.
        :param action: Callable to execute.
        :param description: Description of the job.
        :param args: Non-keyworded arguments to pass to the job.
        :param kwargs: Keyworded arguments to pass to the job.
        :return: Reference to the procedure.
        :rtype: Procedure

        If the within_procedure parameter is not set, a new procedure is created.
        Otherwise, the job is associated to current job's procedure. It is only
        possible though to schedule jobs within the context of the current job's
        procedure if the request comes from the job's code block. If this does
        not happen, the :class:`mysql.hub.errors.ProgrammingError` exception is
        raised.
        """
        procedure = None
        thread = None
        with self.__thread_lock:
            if self.__thread and self.__thread.is_alive():
                thread = self.__thread
            else:
                raise _errors.ExecutorError("Executor is not running.")
        assert(thread is not None)

        if within_procedure and not thread.is_current_thread():
            raise _errors.ProgrammingError(
                "One can only create a job within the context "
                "of the current procedure from a job that belongs "
                "to this procedure."
                )

        if within_procedure:
            procedure = thread.current_job.procedure
        else:
            procedure = Procedure()
            with self.__procedures_lock:
                self.__procedures[procedure.uuid] = procedure

        assert(procedure is not None)
        job = Job(procedure, action, description, args, kwargs)
        self.__queue.put(job)

        _LOGGER.debug(
            "Enqueued job (%s) in procedure (%s).", str(job.uuid),
            job.procedure.uuid
            )
        return procedure

    def get_procedure(self, proc_uuid):
        """Retrieve a reference to a procedure.
        """
        assert(isinstance(proc_uuid, uuid.UUID))
        _LOGGER.debug("Checking procedure (%s).", str(proc_uuid))
        try:
            with self.__procedures_lock:
                procedure = self.__procedures[proc_uuid]
        except (KeyError, ValueError) as error:
            _LOGGER.exception(error)
            procedure = None

        return procedure

    def wait_for_procedure(self, procedure):
        """Wait until the procedure finishes the execution of all
        its jobs.
        """
        thread = None
        with self.__thread_lock:
            if self.__thread and self.__thread.is_alive():
                thread = self.__thread
            else:
                raise _errors.ExecutorError("Executor is not running.")

        if thread.is_current_thread():
            raise _errors.ProgrammingError(
                "One cannot wait for the execution of a procedure from "
                "a job."
                )

        procedure.wait()

# TODO: Move this function to the service module after the commands
#       are implemented. If you move it now, it will be exported
#       in the service interface and we don't want this.
def wait_for_procedures(procedure_param, synchronous):
    """Wait until a procedure completes its execution and return
    detailed information on it. 

    However, if the parameter synchronous is not set, only the
    procedure's uuid is returned because it is not safe to access
    the procedure's information while it may be executing.

    :param proc_param: Iterable with procedures.
    :param synchronous: Whether should wait until the procedure
                        finishes its execution or not.
    :return: Information on the procedure.
    :rtype: str(procedure.uuid), procedure.status, procedure.result
            or (str(procedure.uuid))
    """
    assert(len(procedure_param) == 1)
    if synchronous:
        executor = Executor()
        for procedure in procedure_param:
            executor.wait_for_procedure(procedure)
        return str(procedure_param[-1].uuid), procedure_param[-1].status, \
               procedure_param[-1].result
    else:
        return str(procedure_param[-1].uuid)

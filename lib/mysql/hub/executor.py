import Queue
import threading
import logging

_LOGGER = logging.getLogger(__name__)

def primitive(func):
    """Decorator for decorating primitives.

    Primitives are the atomic primitives that make up the execution
    machinery.::

        @primitive
        def write_status(server):
           server.sql("INSERT INTO status(id, status) VALUES (%d, 'In Progress')", server.id)

        @write_status.undo
        def unwrite_status(server):
           server.sql("DELETE FROM status(id, status) VALUES (%d, 'Undone')", server.id)
    """

    # This is the undo decorator for the function
    def undo_decorate(func):
        func.compensate = func

    # This is the function that executes the primitive and handles any
    # errors. If a com
    def execute(*args, **kwrd):
        try:
            _LOGGER.debug("Executing %s", func.__name__)
            func(*args, **kwrd)
        except Exception:       # pylint: disable=W0703
            _LOGGER.debug("%s failed, executing compensation", func.__name__)
            if func.compensate is not None:
                func.compensate(*args, **kwrd)

    func.compensate = None
    func.undo = undo_decorate
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
    def coord_and_exec(*args, **kwrd):
        # Here it is possible to call the coordinator
        _LOGGER.debug("Starting execution of %s", func.__name__)
        func(*args, **kwrd)
        _LOGGER.debug("Finishing execution of %s", func.__name__)

    return coord_and_exec

class Executor(threading.Thread):
    """Class responsible for dispatching execution of procedures.

    Procedures to be executed are queued to the executor, which then
    will execute them in order.

    """

    def __init__(self, manager):
        super(Executor, self).__init__(name="Executor")
        self.__queue = Queue.Queue()
        self.__manager = manager

    def run(self):
        """Run the executor thread.

        Right now, it only read objects from the queue and call them
        (if they are callable).
        """

        while True:
            action = self.__queue.get(block=True)
            _LOGGER.debug("Reading next action from queue, found %s.", action)
            if action is None:
                break
            action()
        _LOGGER.debug("Exiting Executor thread.")

    def shutdown(self):
        _LOGGER.info("Shutting down executor.")
        self.__queue.put(None)

    def enqueue(self, action):
        self.__queue.put(action)

"""Module for handling events in the Fabric.

Events represent something that happened either inside or outside the Fabric
node and events can trigger execution of jobs.

Central to the reception and processing of events is the **Event
Handler** (or just **Handler** when it is clear from the context),
which recieves events from an external or intenal source and enqueues
zero or more jobs for each code block that has been registered with
the event.

.. seqdiag::

   diagram {
     Source -> Handler [ label = "trigger(event)" ]
     Handler -> Handler [ label = "lookup(event)" ]
     Handler <-- Handler [ label = "blocks" ]
     Handler -> Executor [ label = "enqueue_job(block)",
                           note = "for block in blocks:\n  executor.enqueue_job(block)" ]
     Handler <-- Executor [ label = "job", note = "jobs.append(job)" ]
     Source <-- Handler [ label = "jobs" ]
   }

"""

import functools
import logging

import mysql.hub.errors as _errors
import mysql.hub.executor as _executor

from mysql.hub.utils import Singleton

_LOGGER = logging.getLogger(__name__)

def on_event(event):
    """Decorator to attach a callable to one event.

    The decorator also defined an ``undo`` decorator for the function,
    which allow abort actions to be executed if the body of the
    function throw an exception.

    Example use::

        @on_event(_events.SLAVE_PROMOTED)
        def changes_status(job):
            ....
        @changes_status.undo
        def changes_status_undo(job):
            ...

    The wrapped function will automatically call the ``undo`` callable
    if the main function raises an exception.

    .. todo::

       There is no connection between the wrapped function and the
       arguments: the ``job`` parameter suddenly appears. Although not
       a big problem, it is probably easier to use if the parameters
       to the callable are of the form event + parameters. For
       example, suppose that the SERVER_LOST event accepts a single
       parameter being the server, then the event processing function
       could be declared as::

           @on_event(_events.SERVER_LOST)
           def remove_server(event, server):
              ...

       This syntax make it very clear where each piece comes from and
       the Job class can be hidden entirely inside the Fabric.

    """

    # TODO: Change it so that decorated function accepts event + argument?

    def register_func(func):
        """Wrapper that registers the function and attaches wrappers
        to the provided function."""

        def undo_decorate(undo_func):
            "Undo decorator."
            func.compensate = undo_func

        @functools.wraps(func)
        def wrapped(job):
            "Wrapper that execute undo function on an exception."
            try:
                _LOGGER.debug("Executing %s", func.__name__)
                func(job)
            except Exception as error:       # pylint: disable=W0703
                _LOGGER.debug("%s failed, executing compensation",
                              func.__name__)
                _LOGGER.exception(error)
                if func.compensate is not None:
                    func.compensate(job)
                raise

        func.compensate = None
        wrapped.undo = undo_decorate
        Handler().register(event, wrapped)
        return wrapped

    return register_func


class Event(object):
    """An event that can be triggered either from an external source or an
    internal source.

    An event might have a name, but it is optional. Only events with
    names can be triggered externally, but any event can be triggered
    internally.

    For an example on how to trigger an event internally, see
    :meth:`Handler.trigger`.

    :param string name: The name of the event instance.
    """
    def __init__(self, name=None):
        self.__name = name

    @property
    def name(self):
        """The name of the event.

        :returns: The name of the event, or None if it does not have a
                  name.
        """
        return self.__name


class Handler(Singleton):
    """An event handler to manage and trigger events in the system.

    The event handler is responsible for keeping track of all events
    and will also keep track of what code blocks should be executed
    when an event is triggered.

    """
    def __init__(self):
        """Constructor for Handler.
        """
        super(Handler, self).__init__()
        self.__executor = _executor.Executor()
        self.__instance_for = {}
        self.__blocks_for = {}

    def start(self):
        """Start a defined executor.
        """
        self.__executor.start()

    def shutdown(self):
        """Shutdown the executor.
        """
        self.__executor.shutdown()

    def register(self, event, blocks):
        """Register code blocks with an event in the event handler.

        This method register the code blocks supplied under the
        event. Each code block is represented as a callable (which
        means either a function or a class supporting the ``__call__``
        method).

        If the event was not previously registered, a new entry will
        be created for it. This involves both registering the name of
        the event (if it has a name) and the event instance.

        :param event: Event to register code blocks for.
        :param blocks: Callable to register for event.
        :type blocks: Callable or sequence of callables

        :except NotEventError: Trying to register code blocks with something
                               that is not an instance of :class:`Event`.
        :except NotCallableError: Trying to register something that is not a
                               callable with an event.
        """

        if not isinstance(event, Event):
            raise _errors.NotEventError(
                "Not possible to register with non-event")

        if callable(blocks):
            blocks = [blocks]

        # Check that all provided blocks are callables.
        try:
            for block in blocks:
                if not callable(block):
                    raise _errors.NotCallableError(
                        "Not possible to register non-callables")
        except TypeError:
            raise _errors.NotCallableError("Expected an iterable")

        _LOGGER.debug("Registering blocks %s for event %s under name %s",
                      blocks, event, event.name)

        # Register the name if not registered
        if event.name is not None and event.name not in self.__instance_for:
            self.__instance_for[event.name] = event

        # Register the callables
        self.__blocks_for.setdefault(event, set()).update(blocks)

    def unregister(self, event, block):
        """Unregister a code block from an event.

        :except NotEventError: Trying to unregister code blocks with something
                               that is not an instance of :class:`Event`.
        :except NotCallableError: Trying to unregister something that is not a
                                  callable.
        :except UnknownCallableError: The callable provided was not known to the
                                      event handler.
        """

        if not isinstance(event, Event):
            raise _errors.NotEventError(
                "Not possible to unregister with non-event")
        if not callable(block):
            raise _errors.NotCallableError(
                "Not possible to unregister a callable")

        _LOGGER.debug("Unregistering %s from event %s", block, event)

        try:
            self.__blocks_for[event].remove(block)
        except KeyError:
            raise _errors.UnknownCallableError(
                "Not possible to unregister a non-existant block")

    def is_registered(self, event, block):
        """Check if a callable is registered with an event.

        Note that the exact callable instance is checked for, so you have to
        pass the instance that you want to check for.

        :param event: Event to check if callable is registered with.
        :param block: Callable to look for.
        :type block: callable

        :return: ``True`` if the callable instance is registered with the event,
                 ``False`` otherwise.
        :rtype: Boolean.
        """

        if not isinstance(event, Event):
            raise _errors.NotEventError(
                "Not possible to check registration for non-event")
        if not callable(block):
            raise _errors.NotCallableError(
                "Not possible to check for non-callable")
        try:
            return block in self.__blocks_for[event]
        except KeyError:
            return False

    def trigger(self, event, *args):
        """Trigger an event.

        This function will trigger an event resulting in zero or more
        blocks being scheduled for execution by creating one
        :class:`Job` for each block. If any arguments are passed to
        this function they are passed to the :class:`Job` instance
        which will pass them to the code block on execution.

        Event can be triggered either by name or by instance. If a
        name is provided as *event*, then the event instance is looked
        up first and the instances triggered.

        If an instance is provided as event, it is looked up
        internally to find all blocks associated with it. The blocks
        are then scheduled (by creating one :class:`Job` for each
        block) with the executor and the function returns. This means
        that the blocks may not have been executed on return from the
        function.

        To be able to wait for the blocks to be completed before
        proceeding after triggering the event, references to the
        created jobs (containing the block) are returned. This means
        that if you want to trigger an event and wait for them to
        finish executing, you can use the following code::

           jobs = handler.trigger(SERVER_LOST, "my.example.com")
           for job in jobs:
              job.wait()

        :param event: Event to trigger.
        :type event: Event name or event instance
        :returns: Jobs that were scheduled as a result of triggering the event.
        :rtype: List of jobs that were scheduled.
        """

        _LOGGER.debug("Triggering event %s", event)

        if isinstance(event, basestring):
            event = self.__instance_for[event]

        # Enqueue the jobs and return a list of the jobs scheduled
        return [
            self.__executor.enqueue_job(block, "Triggered by %s" % (event,),
                                        args)
            for block in self.__blocks_for.get(event, [])
            ]

def trigger(event, *args):
    """Trigger an event by name or instance.

    :param event: The event to trigger.
    :type event: Event name or event instance.
    """
    handler = Handler()
    _LOGGER.debug("Triggering event %s in handler %s",
                  event, handler)
    return handler.trigger(event, *args)

# Some pre-defined events. These are documented directly in the documentation
# and not using autodoc.
SERVER_LOST = Event("SERVER_LOST")
SERVER_PROMOTED = Event("SERVER_PROMOTED")
SERVER_DEMOTED = Event("SERVER_DEMOTED")

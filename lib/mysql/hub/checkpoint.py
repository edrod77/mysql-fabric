"""Define checkpoint routines that are responsible for providing the necessary
means so that one can guarantee consistency in the case of a failure.
"""

import logging
import uuid as _uuid
import time
import pickle as pickle
import sys

import mysql.hub.persistence as _persistence

_LOGGER = logging.getLogger(__name__)

class Checkpoint(_persistence.Persistable):
    """This is responsible for keeping track of a procedure execution
    providing the necessary means to register the jobs that are triggered
    on its behalf.

    While running the recovery procedure, the entry with the greatest
    sequence is retrieved and the appropriate actions executed to put the
    system in a consistent state.

    See in what follows, a figure that briefly depicts what happens:

    .. seqdiag::

      diagram {
      activation = none;
      === Schedule "action" and Create "procedure" ===
      trigger -> executor;
      executor -> procedure [ label = "schedule(action)" ];
      executor <- procedure;
      trigger <- executor;
      === Execute "action" ===
      executor -> checkpoint [ label = "begin(action)" ];
      executor <- checkpoint;
      executor -> action [ label = "execute(action)" ];
      executor <- action;
      executor -> checkpoint [ label = "finish(action)" ];
      executor <- checkpoint;
      }

    :param proc_uuid: Procedure uuid.
    :param job_uuid: Job uuid.
    :param action_fqn: Reference to the main function.
    :type action_fqn: String
    :param param_args: List with non-keyworded arguments to the
                       function(s).
    :param param_kwargs: Dictionary with neyworded arguments to the
                         function(s).
    :param started: Timestamp that identifies when the function
                    has started.
    :param finished: Timestamp that identifies when the function
                     has completed.
    :param sequence: Identify the order of the job within a procedure.
    """
    # TODO: INVESTIGATE WHY started and finished ARE BEING TRUNCATED.
    # SQL Statement for creating the table checkpoint which is used to
    # keep track of execution (i.e. jobs).
    CREATE_CHECKPOINTS = (
        "CREATE TABLE checkpoints (proc_uuid VARCHAR(40) NOT NULL, "
        "job_uuid VARCHAR(40) NOT NULL, sequence INTEGER NOT NULL, "
        "action_fqn TEXT NOT NULL, param_args BLOB NULL, "
        "param_kwargs BLOB NULL, started DOUBLE(16, 6) NULL, "
        "finished DOUBLE(16, 6) NULL, "
        "CONSTRAINT pk_checkpoint PRIMARY KEY (proc_uuid, job_uuid))"
        )

    # SQL Statement for dropping the table checkpoints which is used to keep
    # track of execution.
    DROP_CHECKPOINTS = ("DROP TABLE checkpoints")

    #SQL statement for inserting a new checkpoint into the table.
    INSERT_CHECKPOINT = (
        "INSERT INTO checkpoints(proc_uuid, job_uuid, sequence, action_fqn, "
        "param_args, param_kwargs) "
        "SELECT %s, %s, coalesce(MAX(sequence), 0) + 1, %s, %s, %s FROM "
        "checkpoints WHERE proc_uuid = %s"
        )

    #SQL statement for updating the started time.
    UPDATE_START_CHECKPOINT = ("UPDATE checkpoints set started = %s WHERE "
        "proc_uuid = %s and job_uuid = %s"
        )

    #SQL statement for updating the finished time.
    UPDATE_FINISH_CHECKPOINT = ("UPDATE checkpoints set finished = %s WHERE "
        "proc_uuid = %s and job_uuid = %s"
        )

    #SQL statement for deleting the checkpoint executed on behalf of
    #a procedure.
    DELETE_CHECKPOINTS = ("DELETE FROM checkpoints WHERE proc_uuid = %s")

    #SQL statement for retrieving the checkpoints stored on behalf of a
    #procedure.
    QUERY_CHECKPOINTS = ("SELECT proc_uuid, job_uuid, sequence, action_fqn, "
        "param_args, param_kwargs, started, finished FROM checkpoints WHERE "
        "proc_uuid = %s"
        )

    #SQL statement for retrieving all occurrences in the checkptoint which
    #is used for recovery.
    #TODO: Simplify this statement.
    QUERY_UNFINISHED_CHECKPOINTS = (
        "SELECT chk_info.proc_uuid, chk_info.job_uuid, chk_info.sequence, "
        "chk_info.action_fqn, chk_info.param_args, chk_info.param_kwargs, "
        "chk_info.started, chk_info.finished FROM "
        "(SELECT proc_uuid, max(sequence) as sequence FROM checkpoints "
        "WHERE started is NOT NULL AND finished is NULL GROUP BY proc_uuid) "
        "AS chk_core INNER JOIN "
        "(SELECT proc_uuid, job_uuid, sequence, action_fqn, param_args, "
        "param_kwargs, started, finished FROM checkpoints) AS chk_info ON "
        "chk_info.proc_uuid = chk_core.proc_uuid and "
        "chk_info.sequence = chk_core.sequence"
        )

    QUERY_SCHEDULED_CHECKPOINTS = (
        "SELECT proc_uuid, job_uuid, sequence, action_fqn, param_args, "
        "param_kwargs, started, finished FROM checkpoints WHERE finished "
        "is NULL ORDER BY proc_uuid, sequence"
        )

    QUERY_FINISHED_CHECKPOINTS = (
        "SELECT DISTINCT proc_uuid FROM checkpoints WHERE proc_uuid IN "
        "(SELECT DISTINCT chk_info.proc_uuid FROM checkpoints as chk_info "
        "WHERE chk_info.finished is NOT NULL) and proc_uuid NOT IN "
        "(SELECT DISTINCT chk_info.proc_uuid FROM checkpoints as chk_info "
        "WHERE chk_info.finished is NULL)"
        )

    def __init__(self, proc_uuid, job_uuid, action_fqn, param_args,
                 param_kwargs, started=None, finished=None, sequence=None):
        """Constructor for Checkpoint object.
        """
        super(Checkpoint, self).__init__()
        assert(isinstance(proc_uuid, _uuid.UUID))
        assert(isinstance(job_uuid, _uuid.UUID))
        assert(isinstance(action_fqn, basestring))
        assert(started is None or isinstance(started, float))
        assert(finished is None or isinstance(started, float))
        self.__proc_uuid = proc_uuid
        self.__job_uuid = job_uuid
        self.__action_fqn = action_fqn
        self.__param_args = param_args
        self.__param_kwargs = param_kwargs
        self.__do_action = Checkpoint.get_do_action(action_fqn)
        self.__undo_action = Checkpoint.get_undo_action(action_fqn)
        self.__sequence = sequence
        self.__started = started
        self.__finished = finished

    @property
    def proc_uuid(self):
        """Return proc_uuid.
        """
        return self.__proc_uuid

    @property
    def job_uuid(self):
        """Return job_uuid.
        """
        return self.__job_uuid

    @property
    def param_args(self):
        """Return param_args.
        """
        return self.__param_args

    @property
    def param_kwargs(self):
        """Return param_kwargs.
        """
        return self.__param_kwargs

    @property
    def do_action(self):
        """Return do_action.
        """
        return self.__do_action

    @property
    def undo_action(self):
        """Return undo_action.
        """
        return self.__undo_action

    @property
    def started(self):
        """Return started.
        """
        return self.__started

    @property
    def finished(self):
        """Return finished.
        """
        return self.__finished

    @property
    def sequence(self):
        """Return sequence.
        """
        return self.__sequence

    def schedule(self, persister=None):
        """Register that an action has been scheduled.
        """
        param_args, param_kwargs = \
            Checkpoint.serialize(self.__param_args, self.__param_kwargs)
        persister.exec_stmt(Checkpoint.INSERT_CHECKPOINT,
            {"params":(str(self.__proc_uuid), str(self.__job_uuid),
            self.__action_fqn, param_args, param_kwargs,
            str(self.__proc_uuid))}
            )

    def begin(self, persister=None):
        """Register that an action is about to start.
        """
        started = time.time()
        persister.exec_stmt(Checkpoint.UPDATE_START_CHECKPOINT,
            {"params":(started, str(self.__proc_uuid),
            str(self.__job_uuid))}
            )
        self.__started = started

    def finish(self, persister=None):
        """Register that a job has finished.

        :param job_uuid: Job uuid.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        finished = time.time()
        persister.exec_stmt(Checkpoint.UPDATE_FINISH_CHECKPOINT,
            {"params":(finished, str(self.__proc_uuid),
            str(self.__job_uuid))}
            )
        self.__finished = finished

    @staticmethod
    def _create_object_from_row(row):
        """Create a Checkpoint object from a retrieved row.

        :param row: Checkpoint row.
        :type row: Tuple.
        :return: Return a Checkpoint object.
        """
        (proc_uuid, job_uuid, sequence, action_fqn, param_args, param_kwargs,
        started, finished) = row
        param_args, param_kwargs = \
             Checkpoint.deserialize(param_args, param_kwargs)
        checkpoint = Checkpoint(
            _uuid.UUID(proc_uuid), _uuid.UUID(job_uuid), action_fqn,
            param_args, param_kwargs, started, finished, sequence
            )
        return checkpoint

    @staticmethod
    def unfinished(persister=None):
        """Return unfinished procedures.

        :param persister: The DB server that can be used to access the
                          state store.
        :return: Set of procedures that haven't finished.
        :rtype: set(Checkpoint, ...)
        """
        checkpoints = set()
        rows = persister.exec_stmt(Checkpoint.QUERY_UNFINISHED_CHECKPOINTS,
                                   {"raw": False})
        for row in rows:
            checkpoints.add(Checkpoint._create_object_from_row(row))
        return checkpoints

    @staticmethod
    def scheduled(persister=None):
        """Return scheduled procedures.

        :param persister: The DB server that can be used to access the
                          state store.
        :return: Set of procedures that were scheduled.
        :rtype: set(Checkpoint, ...)
        """
        checkpoints = set()
        rows = persister.exec_stmt(Checkpoint.QUERY_SCHEDULED_CHECKPOINTS,
                                   {"raw": False})
        for row in rows:
            checkpoints.add(Checkpoint._create_object_from_row(row))
        return checkpoints

    @staticmethod
    def fetch(proc_uuid, persister=None):
        """Return the object corresponding to the proc_uuid.

        :param proc_uuid: Procedure uuid.
        :param persister: The DB server that can be used to access the
                          state store.
        :return: Checkpoint object that corresponds to the proc_uuid or
                 None if one is not found.
        """
        checkpoints = set()
        assert(isinstance(proc_uuid, _uuid.UUID))

        rows = persister.exec_stmt(Checkpoint.QUERY_CHECKPOINTS,
            {"raw": False, "params":(str(proc_uuid), )}
            )

        if rows:
            for row in rows:
                checkpoint = Checkpoint._create_object_from_row(row)
                checkpoints.add(checkpoint)
        return checkpoints

    @staticmethod
    def remove(checkpoint, persister=None):
        """Remove the object from the persistent store.

        :param checkpoint: Checkpoint object.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        assert(isinstance(checkpoint, Checkpoint))
        persister.exec_stmt(Checkpoint.DELETE_CHECKPOINTS,
            {"params":(str(checkpoint.proc_uuid), )}
            )

    @staticmethod
    def cleanup(persister=None):
        """Remove all the checkpoints which are related to procedures
        that have finished the execution but did not get the chance
        of calling remove.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        rows = persister.exec_stmt(Checkpoint.QUERY_FINISHED_CHECKPOINTS,
            {"raw": False}
            )
        if rows:
            for row in rows:
                persister.exec_stmt(Checkpoint.DELETE_CHECKPOINTS,
                    {"params":(row[0], )}
                    )

    @staticmethod
    def create(persister=None):
        """Create the objects(tables) that represent Checkpoint information
        in the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Checkpoint.CREATE_CHECKPOINTS)

    @staticmethod
    def drop(persister=None):
        """Drop the objects(tables) that represent Checkpoint information in
        the state store.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Checkpoint.DROP_CHECKPOINTS)

    @staticmethod
    def get_do_action(action_fqn):
        """Get a reference to a main action.

        :param action_fqn: Fully qualified function name, i.e. module.name.
        :return: Reference to an action if there is any.
        :rtype: Callable or None.
        """
        module , name = action_fqn.rsplit(".", 1)
        try:
            return getattr(sys.modules[module], name)
        except (AttributeError, KeyError, NameError):
            return None

    @staticmethod
    def get_undo_action(action_fqn):
        """Get a reference to a main action.

        :param action_fqn: Fully qualified function name, i.e. module.name.
        :return: Reference to an action if there is any.
        :rtype: Callable or None.
        """
        module , name = action_fqn.rsplit(".", 1)
        try:
            return getattr(sys.modules[module], name).undo_function
        except (AttributeError, KeyError, NameError):
            return None

    @staticmethod
    def serialize(param_args, param_kwargs):
        """Serialize the non-keyworded and keyworded parameters using Pickle.
        It is worth noticing that it does not check the type of the objects
        that are being serialize and it is up to the user to do so.

        :param param_args: List with non-keyworded arguments to the
                           function(s).
        :param param_kwargs: Dictionary with neyworded arguments to the
                             function(s).
        :return: Return a tuple with both parameters serialized.
        :rtype: (serialized_args, serialized_kwargs).
        """
        s_param_args = pickle.dumps(param_args)
        s_param_kwargs = pickle.dumps(param_kwargs)
        return s_param_args, s_param_kwargs

    @staticmethod
    def deserialize(param_args, param_kwargs):
        """Deserialize the non-keyworded and keyworded parameters using Pickle.

        :param param_args: Serialized list with non-keyworded arguments to the
                           function(s).
        :param param_kwargs: Serialized dictionary with neyworded arguments to
                             the function(s).
        :return: Return a tuple with both parameters deserialized.
        :rtype: (args, kwargs).
        """
        ds_param_args = pickle.loads(param_args)
        ds_param_kwargs = pickle.loads(param_kwargs)
        return ds_param_args, ds_param_kwargs

    @staticmethod
    def is_recoverable(action):
        """Check if an action is recoverable or not. This means that it is
        possible to get a reference to the action given its module name and
        and its name.

        :param action: Callable.
        :return: True if it is recoverable. Otherwise, return False.
        """
        try:
            action_fqn = action.__module__ + "." + action.__name__
            return Checkpoint.get_do_action(action_fqn) is not None
        except AttributeError:
            return False

    def __eq__(self,  other):
        """Two entries are equal if they have the same proc_uuid and job_uuid.
        """
        return isinstance(other, Checkpoint) and \
               self.__proc_uuid == other.proc_uuid and \
               self.__job_uuid == other.job_uuid

    def __hash__(self):
        """A Checkpoint is hashable through its proc_uuid and job_uuid.
        """
        return hash(self.__proc_uuid) ^ hash(self.__job_uuid)

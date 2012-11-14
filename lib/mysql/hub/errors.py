class Error(Exception):
    """Base exception for all errors in the package.
    """
    pass

class NotCallableError(Error):
    """Exception raised when a callable was expected but not provided.
    """
    pass

class NotEventError(Error):
    """Exception raised when a non-event instance was passed where an
    event instance was expected.
    """
    pass

class UnknownCallableError(Error):
    """Exception raised when trying to use a callable that was not
    known when a known callable was expected.
    """
    pass

class UnknownEventError(Error):
    """Exception raised when trying to use an event that was not known
    in a situation where a known event was expected.
    """
    pass

class ServiceError(Error):
    """Exception raised when the Services fail due to any reason. For example,
    when the wrong parameters to start them up are provided.
    """

class ExecutorError(Error):
    """Exception raised when the one tries to access the executor that is
    not properly configured.
    """

class DatabaseError(Error):
    """Exception raised when something bad happens while accessing a
    database.
    """
    pass

class MismatchUuidError(DatabaseError):
    """Exception raised when server object and server process' uuids
    do not match.
    """
    pass

class ConfigurationError(DatabaseError):
    """Exception raised when access parameters are not properly configured.
    """
    pass

class TimeoutError(DatabaseError):
    """Exception raised when there is a timeout.
    """

class PersistenceError(Error):
    """Raised to indicate exception while accessing the state store.
    """
    pass

class GroupError(Error):
    """Exception raised when one tries to execute an invalid operation on a
    group. For example, it is not possible to create two groups with the
    same id or remove a group that has associated servers.
    """
    pass

class ServerError(Error):
    """Exception raised when one tries to execute an invalid operation on a
    server. For example, it is not possible to create two servers with the
    same uuid.
    """
    pass

class JobError(Error):
    """Exception raised when a job is not found.
    """
    pass

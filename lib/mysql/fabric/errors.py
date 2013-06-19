"""Errors raised within the MySQL Fabric library.
"""

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

class ExecutorError(Error):
    """Exception raised when the one tries to access the executor that is
    not properly configured.
    """

class InvalidGtidError(Error):
    """Exception raised when the one tries to use and make operations with
    invalid GTID(s).
    """

class DatabaseError(Error):
    """Exception raised when something bad happens while accessing a
    database.
    """
    pass

class UuidError(DatabaseError):
    """Exception raised when there are problems with uuids. For example,
    if the expected uuid does not match the server's uuid.
    """
    pass

class ProgrammingError(Error):
    """Exception raised when a developer tries to use the interfaces and
    executes an invalid operation.
    """
    pass

class ConfigurationError(ProgrammingError):
    """Exception raised when access parameters are not properly configured.
    """
    pass

class TimeoutError(ProgrammingError):
    """Exception raised when there is a timeout.
    """

class PersistenceError(Error):
    """Exception raised when something wrong happens while accessing the
    state store.
    """
    pass

class ServiceError(Error):
    """Exception raised when one tries to use the service interface and
    executes an invalid operation.
    """
    pass

class GroupError(ServiceError):
    """Exception raised when one tries to execute an invalid operation on a
    group. For example, it is not possible to create two groups with the
    same id or remove a group that has associated servers.
    """
    pass

class ServerError(ServiceError):
    """Exception raised when one tries to execute an invalid operation on a
    server. For example, it is not possible to create two servers with the
    same uuid.
    """
    pass

class ProcedureError(ServiceError):
    """Exception raised when a procedure is not found.
    """
    pass

class ShardingError(Error):
    """Exception raised when an invalid operation is attempted on the
    sharding system.
    """
    pass

class LockManagerError(Error):
    """Exception raised when an invalid operation is attempted on the
    lock manager.
    """
    pass

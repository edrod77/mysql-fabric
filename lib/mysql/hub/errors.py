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

class Error(Exception):
    """Base exception for all errors in the package.
    """
    pass

class PathError(Error):
    """Exception thrown when a resource path is incorrect.
    """
    pass

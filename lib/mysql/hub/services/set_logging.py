"""Service interface to define logging level per module. It is created in
order to allow users to dynamically set logging level and ease debugging
and eventually make it easier to catch errors.
"""
def set_logging_level(module, level):
    """Set logging level.

    :param module: Module that will have its logging level changed.
    :param level: The logging level that will be set.
    :return: Return True if the logging level is changed. Otherwise,
    return the error's description.
    """
    try:
        __import__(module)
        logger = logging.getLogger(module)
        logger.setLevel(level)
    except Exception as error:
        _LOGGER.exception(error)
        return error
    return True

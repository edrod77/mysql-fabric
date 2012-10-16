import logging

import mysql.hub.executor
import mysql.hub.services
import mysql.hub.events

MANAGER = None
_LOGGER = logging.getLogger(__name__)

class Manager(object):
    """The main manager class.

    This class hold references to all other components of the
    system. It also start all necessary protocol instances and the
    executor.
    """

    def __init__(self, config):
        self.__config = config
        self.__executor = mysql.hub.executor.Executor(self)
        self.__services = mysql.hub.services.ServiceManager(self)
        self.__handler = mysql.hub.events.Handler(self.__executor)

    def start(self):
        _LOGGER.info("Starting Core Services.")
        self.__services.load_services()
        self.__executor.start()
        self.__services.start()
        _LOGGER.info("Core Services started.")

    def wait(self):
        self.__executor.join()

    def shutdown(self):
        _LOGGER.info("Shutting down Core Services.")
        self.__executor.shutdown()
        self.__services.shutdown()

    @property
    def executor(self):
        return self.__executor

    @property
    def handler(self):
        return self.__handler

    @property
    def config(self):
        return self.__config

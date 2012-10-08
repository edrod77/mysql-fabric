import logging

import mysql.hub.executor
import mysql.hub.services

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
        self.__service_manager = mysql.hub.services.ServiceManager(self)
        self.__executor = mysql.hub.executor.Executor(self)

        global MANAGER
        MANAGER = self

    def start(self):
        _LOGGER.info("Starting Core Services.")
        self.__service_manager.load_services()
        self.__executor.start()
        self.__service_manager.start()
        self.__executor.join()

    def shutdown(self):
        _LOGGER.info("Shutting down Core Services.")
        self.__executor.shutdown()
        self.__service_manager.shutdown()

    @property
    def executor(self):
        return self.__executor

    @property
    def config(self):
        return self.__config

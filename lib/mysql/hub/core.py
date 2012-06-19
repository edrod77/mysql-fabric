import logging
import mysql.hub.executor
import mysql.hub.resource
import mysql.hub.services

MANAGER = None

class Manager(object):
    """The main manager class.

    This class hold references to all other components of the
    system. It also start all necessary protocol instances and the
    executor.
    """

    def __init__(self, logger, config):
        self.__logger = logger
        self.__config = config
        self.__executor = mysql.hub.executor.Executor(self)
        self.__services = mysql.hub.services.ServiceManager(self)
        self.__resources = mysql.hub.resource.ResourceManager(self)

    def start(self):
        self.__services.load_services()
        self.__executor.start()
        self.__services.start()
        self.__executor.join()

    def shutdown(self):
        self.__executor.shutdown()
        self.__services.shutdown()

    @property
    def resource(self):
        return self.__resources

    @property
    def executor(self):
        return self.__executor

    @property
    def config(self):
        return self.__config

    @property
    def logger(self):
        return self.__logger

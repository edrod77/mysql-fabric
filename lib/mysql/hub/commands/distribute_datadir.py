"""Utility for recursively copying a MySQL data directory from a source
    to a destination machine.
"""

#TODO: The script needs to be tested on windows

import os
import Queue
import string
import subprocess
import sys
import threading
import time
from optparse import OptionParser

"""The maximum number of times a copy operation between a given pair
    (source, destination) can be tried (re-tried). The operation is aborted once
    the maximum number of retries is reached.
"""
MAX_ALLOWED_RETRIES = 3

"""When doing a single source opy the following constant defines the maximum
    number of simultaneous copy threads that can be started from the same
    source
"""
MAX_CONCURRENT_TRANSFERS = 3

class DataDirCopy(threading.Thread):
    """The class is used to start a thread of copy from a source
    directory to a destination directory, where the source and the
    destination directory are in different systems.
    """

    def __init__(self, source_free, source, destination,
                 max_allowed_retries, quiet, pool_sema):
        """Initialize the parameter for starting a thread of copy.

            :param source_free: The sources that are available for transfer.
            :param source: The source machine containing the data dir.
            :param destination: The destination to which the data dir must be
                                copied.
            :param max_allowed_retries: The maximum number of attempts of
                                                            copying
            :param quiet: The boolean used to indicate if the status needs to
                            be printed
            :param pool_sema: The thread semaphore
        """
        super(DataDirCopy, self).__init__()
        self.__source = source
        self.__destination = destination
        self.__max_allowed_retries = max_allowed_retries
        self.__source_free = source_free
        self.__quiet = quiet
        self.__pool_sema = pool_sema

    def run(self):
        """The method is invoked to start a thread of copy from the source
            to the destination.
        """
        transfer_attempts = 0

        #scp to destination. Upon failure retry a max_allowed_retries
        #number of times
        while transfer_attempts < self.__max_allowed_retries:
            try:
                #If check_call is successful it does not throw an exception,
                #otherwise it throws an exception
                subprocess.check_call(["scp",  "-r",  "-o",
                            "StrictHostKeyChecking=no",
                            os.path.join(self.__source,  "."),
                            self.__destination])
                #If the code comes here it means that scp was successful.
                #Hence we do not need to retry anymore.
                break
            except subprocess.CalledProcessErrors as e:
                print_error(e.output,  self.__quiet)
            transfer_attempts += 1

        if self.__source_free is not None:
            #Upon successful transfer from the source to the destination
            #both the source and the destination can be used as sources.
            #If the transfer_attempts reaches maximum allowed attempts
            #it indicates that the transfer failed.
            if transfer_attempts == self.__max_allowed_retries:
                print_error("Error while transferring from %s -> %s" %
                            (self.__source,  self.__destination),  self.__quiet)
                self.__source_free.put(self.__source)
            else:
                #In this case both the source and the destination can be used
                #as a source for transfer.
                self.__source_free.put(self.__source)
                self.__source_free.put(self.__destination)
        self.__pool_sema.release()

def single_source_copy_initate(source, destinations,
                               max_allowed_retries,
                               max_concurrent_transfers,
                               quiet):
    """Initiates multiple threads of copy from the given source to the
    destinations.

    :param source <host>:<dir> source containing the dir to be copied.
    :param destinations <host>:<dir> list of destinations
    :param max_allowed_retries: The number of times a copy will be
                                attempted before giving up.
    :param max_concurrent_transfers: The maximum number of simultaneous
                                copy threads that can be started.
    :param quiet: Indicates whether the status and error messages should
                be printed.
    """
    print_msg("Initiating Transfers\n",  quiet)
    source = string.replace(source, " ", "\ ")
    dir_copy_threads = []
    pool_sema = threading.BoundedSemaphore(max_concurrent_transfers)
    for dst in destinations:
        pool_sema.acquire(True)
        dst = string.replace(dst, " ", "\ ")
        print_msg("Initiating copy from %s -> %s\n" %
                  (source,  dst),  quiet)
        ssc = DataDirCopy(None, source, dst, max_allowed_retries, quiet,
                               pool_sema)
        dir_copy_threads.append(ssc)
        ssc.start()
    #Wait until all the copy threads complete before exiting.
    for thread in dir_copy_threads:
        thread.join()

def data_dir_copy_initiate(source, destinations, max_allowed_retries,
                               max_concurrent_transfers, quiet):
    """Initiaties a recursive secure copy from a given source to
        a set of destinations. There is one thread of copy from a
        given host to a destination.

        For example,

        If there are four hosts

        h0, h1, h2 and h3 where h0 is the source

        A thread of copy is started from h0 to h1. Once this completes,
        a thread of copy is started from h0 to h1 and from h1 to h2.

        h0
        |
        |
        \/
        h1

        h0   h1
        |    |
        |    |
        \/   \/
        h2   h4

        The above algorithm of copying will become more efficient
        when the granularity of copying changes from directory to
        file.

        :param source <host>:<dir> source containing the dir to be copied.
        :param destinations <host>:<dir> list of destinations
        :param max_allowed_retries: The number of times a copy will be
                                    attempted before giving up.
        :param max_concurrent_transfers: The maximum number of simultaneous
                                    copy threads that can be started.
        :param quiet: Indicates whether the status and error messages should
                    be printed.
    """

    #Stores those <host>:<dir> pairs that can act as a source for
    #transferring. At anytime if we have not transferred (or initiated
    #transfers) to all destinations we can pick up a source from here
    #to participate in a transfer to a particular destination.
    source_free = Queue.Queue()

    pool_sema = threading.BoundedSemaphore(max_concurrent_transfers)

    source_free.put(source)
    #Maintain the list of copy threads started in the list below
    dir_copy_threads = []
    print_msg("Initiating Transfers\n",  quiet)
    for dst in destinations:
        pool_sema.acquire(True)
        dst = string.replace(dst, " ", "\ ")
        src = source_free.get(True)
        src = string.replace(src, " ", "\ ")
        print_msg("Initiating copy from %s -> %s\n" %
                  (src,  dst),  quiet)
        ddc = DataDirCopy(source_free, src, dst, max_allowed_retries, quiet,
                          pool_sema)
        dir_copy_threads.append(ddc)
        ddc.start()

    #Wait until all the copy threads complete before exiting.
    for thread in dir_copy_threads:
        thread.join()

    print_msg("Transfers Completed.\n",  quiet)

def print_msg(msg,  quiet):
    """If the quiet flag is not set print the output in stdout

    :param msg The output message to be printed.
    """
    if not quiet:
        sys.stdout.write(msg)

def print_error(error_msg,  quiet):
    """If the quiet flag is not set print the error in stderror

    :param error_msg The error message to be printed.
    """
    if not quiet:
        sys.stderr.write(error_msg)

def main():
    """Parse the parameters and initiate the copy of the data directory
    """
    usage = "usage: %prog [options] source destination"
    #Parse the arguments from the command line
    parser = OptionParser(usage)
    parser.add_option("--multiple_source_transfer", default=False,
                        action="store_true",
                        help="Disable output from the transfer command")
    parser.add_option("--quiet", default=False,
                        action="store_true",
                        help="Disable output from the transfer command")
    parser.add_option("--user", help="The user name to be prefixed to URLs")
    parser.add_option("--max_allowed_retries", default=MAX_ALLOWED_RETRIES,
                        type=int, help="Maximum retries allowed to a system")
    parser.add_option("--max_concurrent_transfers",
                        default=MAX_CONCURRENT_TRANSFERS,
                        type=int, help="Maximum Concurrent copy threads")

    (options,  args) = parser.parse_args()

    if len(args) != 2:
        print_error("Invalid Number of arguments\n",  options.quiet)
        sys.exit("Invalid Invocation\n")

    destinations = args[1].split(",")

    #Form the source and the destinations to be passed to scp
    scp_destinations = []

    if options.user is not None:
      scp_destinations = [options.user + "@" + destination
                          for destination in destinations]
    else:
      scp_destinations = destinations

    #Initiate the remote copy
    if options.user is not None:
        scp_source = options.user + "@" + args[0]
    else:
        scp_source = args[0]

    if not options.multiple_source_transfer:
        single_source_copy_initate(scp_source, scp_destinations,
                               int(options.max_allowed_retries),
                               int(options.max_concurrent_transfers),
                               options.quiet)
    else:
        data_dir_copy_initiate(scp_source, scp_destinations,
                               options.max_allowed_retries,
                               int(options.max_concurrent_transfers),
                               options.quiet)

if __name__ == "__main__":
    main()

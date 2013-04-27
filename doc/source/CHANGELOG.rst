####################
ChangeLog for Fabric
####################


Release 0.1.2 (released April 18, 2013)
---------------------------------------

* HAM-52: Mismatch between service and logging.
* HAM-58: start_slave() may block due to errors during startup.
* HAM-74: Add version checking
* HAM-100: Fixed documentation issues in the README and README.devel.
* HAM-102: MySQL Fabric manage stop hangs when we interrupt in the fabric start page
* HAM-103: Fabric manage setup hangs when the corresponding server is not started.
* HAM-108: Starting a failure detector re-register events.
* HAM-109: Replication topology fails after a switchover/promote.
* HAM-112: Remove "duplicate" commands from the interface.
* HAM-113: Promote fails after demote.
* HAM-114: Promote fails after removing the previous master from the group.
* HAM-120: Incorrect error message while promoting a server again in a group.
* HAM-136: logger.setLevel("INFO") does not work with python 2.6

Release 0.1.1 (released February 28, 2013)
------------------------------------------

* HAM-34: Revisit the Server's Pool
* HAM-42: Command-Line Interface Module.
* HAM-53: Variables that support None & Columns that support NULL
* HAM-54: Revisit the Event Driven Interface
* HAM-56: Concurrency issue in the executor
* HAM-69: Fix basic design issues in Server class
* HAM-70: Created commands for master group management
* HAM-80: Documentation is not being generated.
* HAM-82: Adding support for Shard IDs
* HAM-83: Adding commands for database sharding.
* HAM-85: Problems with --daemonize.
* HAM-86: Create command "fabric manage setup/teardown"
* HAM-87: Present results reported by a command in a user-friendly way
* HAM-88: setup.py is not installing the configuration file "main.cfg"
* HAM-90: Creating the fabric list mapping definitions command

Release 0.1.0 (released January 23, 2013)
-----------------------------------------

* HAM-1: State Store
* HAM-4: Configuration
* HAM-7: Shard key configuration
* HAM-8: Master Groups
* HAM-9: Logging
* HAM-12: High Availability Interfaces
* HAM-17: Basic Executor
* HAM-18: Persister Management
* HAM-22: Offline Sharding Utility
* HAM-30: Implement event processing
* HAM-31: Shard key mapping interface
* HAM-40: Remove deprecated decorators
* HAM-43: Removal of the core module and centralized Manager

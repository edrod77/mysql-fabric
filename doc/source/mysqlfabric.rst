The `mysqlfabric` utility
=========================

:Release: |version|

MySQL Fabric is a system for managing farms of MySQL servers. The MySQL Fabric
utility to keep track of the servers in the farm, is the platform for execution
of procedures that work with the servers of the farm, and provide information
about the farm through a set of web interfaces.

The MySQL Fabric utility can be used to start a Fabric daemon that is
responsible for providing information about the farm, process requests to add or
remove servers, and execute procedures such as handling fail-over and
switch-over.


Command Overview
----------------

The commands to the MySQL Fabric utility is organized into groups each serving a
different purpose.

`mysqlfabric manage setup`
  Perform setup of the MySQL Fabric daemon by creating the necessary tables in
  the backing store. It is assumed that you have a valid Fabric configuration
  file containing the address of a MySQL server that will be used as a backing
  store for all the information about the farm.

  The command will create all the necessary tables in the backing
  store. Normally, the user in the configuration file will be used to create the
  tables, but you can provide a user (for example, the root user) to ensure that
  you have enough privileges to create the database and the necessary tables and
  still keep only update privileges for the user in the configuration file.

`mysqlfabric manage teardown`
  Tear down the system by removing tables that were created by the `setup`
  command.

`mysqlfabric manage start [--daemonize]`

  Start a new MySQL Fabric daemon. If `--daemonize` is given, the process will
  be started in the background and will be disassociated from the terminal
  session (so that it keeps running even if you close the terminal session).

  If started without `--daemonize`, log output will go to the terminal. If
  started with `--daemonize`, log output will be written to the log file
  according to the configuration file.

`mysqlfabric manage stop`
  Stop the MySQL Fabric daemon.


Configuration File
------------------

Both the Fabric daemon and the Fabric utility use the same configuration file
but they read and use different sections. If a section is not necessary it will
not be read, so you can safely remove sections related to the Fabric daemon on
machines that only need to use the Fabric utility.


Section ``storage``
~~~~~~~~~~~~~~~~~~~

This section contains information about how to contact the backing store and is
only necessary for running the Fabric daemon. The backing store is a normal
MySQL server; there are currently no requirements on the version of the server
but it is assumed that InnoDB is installed because that is the storage engine
used for the tables.

``address``
  The address of the backing store in *host:port* notation.

``user``
  The user to use when logging into the backing store. Note that the user need
  to have write permissions to the database given below.

``password``
  The password to use when logging into the backing store.

``database``
  The database to store the tables in.

``connection_timeout``
  The connection timeout to use for the connection to the backing store.


Section ``protocol.xmlrpc``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section contains configuration information for the XML-RPC protocol used by
both the Fabric daemon and the Fabric utility.

``address``
  Address for the XML-RPC server in *host:port* notation. On the server side,
  the *host* is ignored and the server will listen on all connections on the
  given port. On the client side, the *host* is the machine where the request
  will be sent.

``threads``
  The number of threads used to process incoming requests.


Section ``executor``
~~~~~~~~~~~~~~~~~~~~

This section contains configuration information for the executor, which is part
of the Fabric daemon.

``executors``
  The number of executor threads to use for executing procedures.


Section ``logging``
~~~~~~~~~~~~~~~~~~~

This section contains configuration information for logging.

``level``
  The log level to use for the Fabric daemon.

``url``
  This is a URL for the log files. The "file" protocol implements a rotating
  file handler, while "syslog" log using syslog.


Section ``sharding``
~~~~~~~~~~~~~~~~~~~~

This section contains configuration information used by the sharding subsystem.

``mysqldump_program``
  Path to the mysqldump program used when migrating shards.

``mysqlclient_program``
  Path to the mysql client program used when migrating shards.


Section ``connector``
~~~~~~~~~~~~~~~~~~~~~

This section contains information related to the information dumped to the
connectors.

``TTL``
  Time-to-live provided with the dumped information. Connectors need to reload
  the information after the TTL has passed.


Files
-----

``/etc/mysql/fabric.cfg``
  Default location of the configuration file.


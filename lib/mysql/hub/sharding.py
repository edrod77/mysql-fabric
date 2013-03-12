"""This module contains the plumbing necessary for creating, modifying and
querying the sharding information from the state stores.
"""
import mysql.hub.errors as _errors
import mysql.hub.persistence as _persistence

from mysql.hub.server import MySQLServer, Group

class ShardMapping(_persistence.Persistable):
    """Represents the mapping between the sharding scheme and the table
    being sharded. The class encapsulates the operations required to
    manage the information in the state store related to this mapping.

    NOTE: A sharding scheme contains the details of the type name (i.e.) RANGE,
    HASH, List used for sharding and the parameters for the same. For example,
    a range sharding would contain the Lower Bound and the Upper Bound for
    the sharding definition.

    The information in the state store, for a sharding scheme, in the case
    where the state store is a relational store takes the following form:

        +--------------+--------------------------+---------------------+
        | shard_map_id |      table_name          |    column_name      |
        +==============+==========================+=====================+
        |1             |Employee                  |ID                   |
        +--------------+--------------------------+---------------------+
        |2             |Salary                    |EmpID                |
        +--------------+--------------------------+---------------------+

    The columns are explained as follows,
    * shard_mapping_id - The unique identification for a shard mapping.
    * table_name - The tables associated with this shard mapping.
    * column_name - The column name in the table that is used to shard this
    table.

        +--------------+--------------------------+---------------------+
        | shard_map_id |         type_name        |     global          |
        +==============+==========================+=====================+
        |1             |RANGE                     |GROUPIDX             |
        +--------------+--------------------------+---------------------+

    The columns are explained as follows

    * shard_mapping_id - The unique identification for a shard mapping.
    * type_name - The type name of the sharding scheme- RANGE, HASH, LIST etc
    * global_group - Every shard mapping is associated with a Global Group
                        that stores the global updates and the schema changes
                        for this shard mapping and dissipates these to the
                        shards.
    """

    #Create the schema for the tables used to store the shard specification
    #mapping information
    CREATE_SHARD_MAPPING = ("CREATE TABLE shard_mapping"
                            "(shard_mapping_id INT NOT NULL, "
                            "table_name VARCHAR(64) NOT NULL, "
                            "column_name VARCHAR(64) NOT NULL, "
                            "PRIMARY KEY (table_name, column_name), "
                            "INDEX(table_name))")
    CREATE_SHARD_MAPPING_DEFN = ("CREATE TABLE shard_mapping_defn"
                                 "(shard_mapping_id INT AUTO_INCREMENT "
                                 "NOT NULL PRIMARY KEY, "
                                 "type_name VARCHAR(64) NOT NULL, "
                                 "global_group VARCHAR(64))")

    #Drop the schema for the tables used to store the shard specification
    #mapping information
    DROP_SHARD_MAPPING = ("DROP TABLE shard_mapping")
    DROP_SHARD_MAPPING_DEFN = ("DROP TABLE shard_mapping_defn")

    #Create the referential integrity constraint with the shard_mapping_defn
    #table.
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                ("ALTER TABLE shard_mapping "
                                  "ADD CONSTRAINT fk_shard_mapping_id "
                                  "FOREIGN KEY(shard_mapping_id) REFERENCES "
                                  "shard_mapping_defn(shard_mapping_id)")

    #Drop the referential integrity constraint with the shard_mapping_defn
    #table.
    DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                    ("ALTER TABLE shard_mapping DROP "
                                     "FOREIGN KEY fk_shard_mapping_id")

    #Define a shard mapping.
    DEFINE_SHARD_MAPPING = ("INSERT INTO "
                            "shard_mapping_defn(type_name, global_group) "
                            "VALUES(%s, %s)")
    ADD_SHARD_MAPPING = ("INSERT INTO shard_mapping"
                         "(shard_mapping_id, table_name, column_name) "
                         "VALUES(%s, %s, %s)")

    #select the shard specification mapping information for a table
    SELECT_SHARD_MAPPING = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_mapping as sm, "
                            "shard_mapping_defn as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND table_name = %s")

    #Select all the shard specifications of a particular sharding type name.
    LIST_SHARD_MAPPINGS = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_mapping as sm, "
                            "shard_mapping_defn as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND type_name = %s")

    #Select the shard mapping for a given shard mapping ID.
    SELECT_SHARD_MAPPING_DEFN = ("SELECT shard_mapping_id, type_name, "
                                 "global_group FROM shard_mapping_defn "
                                 "WHERE shard_mapping_id = %s")

    #Select all the shard mapping definitions
    LIST_SHARD_MAPPING_DEFN = ("SELECT shard_mapping_id, type_name, "
                               "global_group FROM shard_mapping_defn")

    #Delete the sharding specification to table mapping for a given table.
    DELETE_SHARD_MAPPING = ("DELETE FROM shard_mapping "
                            "WHERE shard_mapping_id = %s")


    def __init__(self, shard_mapping_id, table_name, column_name, type_name,
                 global_group):
        """Initialize the Shard Specification Mapping for a given table.

        :param shard_mapping_id: The unique identifier for this shard mapping.
        :param table_name: The table for which the mapping is being created.
        :param column_name: The column name on which the mapping is being
                            defined. The column is useful when resharding
                            comes into the picture.
        :param type_name: The type of sharding defined on the given table.
                            E.g. RANGE, HASH etc.
        :param global_group: Every shard mapping is associated with a Global
                                Group that stores the global updates and the
                                schema changes for this shard mapping and
                                dissipates these to the shards.
        """
        super(ShardMapping, self).__init__()
        self.__shard_mapping_id = shard_mapping_id
        self.__table_name = table_name
        self.__column_name = column_name
        self.__type_name = type_name
        self.__global_group = global_group

    @property
    def shard_mapping_id(self):
        """Return the shard mapping id for this shard mapping.
        """
        return self.__shard_mapping_id

    @property
    def table_name(self):
        """Return the table on which the shard mapping is defined.
        """
        return self.__table_name

    @property
    def column_name(self):
        """Return the column in the table on which the sharding is defined.
        """
        return self.__column_name

    @property
    def type_name(self):
        """Return the type of the sharding specification defined on the table.
        """
        return self.__type_name

    @property
    def global_group(self):
        """Return the global group for the sharding specification.
        """
        return self.__global_group

    @staticmethod
    def list(sharding_type, persister=None):
        """The method returns all the shard mappings (names) of a
        particular sharding_type. For example if the method is called
        with 'range' it returns all the sharding specifications that exist
        of type range.

        :param sharding_type: The sharding type for which the sharding
                              specification needs to be returned.
        :param persister: A valid handle to the state store.

        :return: A list of sharding specification names that are of the
                  sharding type.
        :raises: Sharding Error if Sharding type is not found.
        """

        shard_mappings = []
        cur = persister.exec_stmt(ShardMapping.LIST_SHARD_MAPPINGS,
                                  {"raw" : False,
                                  "fetch" : False,
                                  "params" : (sharding_type,)})
        rows = cur.fetchall()

        #TODO: Better to put the query with the code, since this is the only
        #TODO: place it is used. That makes it clear what each number refers
        #TODO: to. If you're concerned about row lengths, you can always
        #TODO: define a local variable.
        #TODO: An alternative is to "unpack" the row into local variables that
        #TODO: make sense. E.g., "id, table_name, ... = row".
        for row in rows:
            shard_mappings.append(ShardMapping(row[0], row[1], row[2],
                                               row[3], row[4]))
        if shard_mappings == []:
            raise _errors.ShardingError("Unknown Sharding Type")
        return shard_mappings

    # TODO: FOLLOW THE SAME PATTERN ADOPTED IN MySQLServer.
    def remove(self, persister=None):
        """Remove the shard mapping represented by the Shard Mapping object.
        The method basically results in removing the association between a
        table and the sharding specification used to shard this table.

        :param persister: Represents a valid handle to the state
                          store.
        :return: True if the remove succeeded.
                False if the query failed.
        """

        persister.exec_stmt(
            ShardMapping.DELETE_SHARD_MAPPING,
            {"params":(self.__shard_mapping_id,)})
        return True

    @staticmethod
    def create(persister=None):
        """Create the schema to store the mapping between the table and
        the sharding specification.

        :param persister: A valid handle to the state store.
        :return: True if the query succeeded but there is no result set
                False if the query failed
        """

        persister.exec_stmt(ShardMapping.CREATE_SHARD_MAPPING)
        persister.exec_stmt(ShardMapping.CREATE_SHARD_MAPPING_DEFN)
        return True

    @staticmethod
    def drop(persister=None):
        """Drop the schema for the table used to store the mapping between
        the table and the sharding specificaton.

        :param persister: A valid handle to the state store.
        :return: True if the query succeeded but there is no result set
                False if the query failed
        """

        persister.exec_stmt(ShardMapping.DROP_SHARD_MAPPING_DEFN)
        persister.exec_stmt(ShardMapping.DROP_SHARD_MAPPING)
        return True

    @staticmethod
    def add_constraints(persister=None):
        """Add the Foreign key contraints for the Shard Mapping tables.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(
                    ShardMapping.ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID)
        return True

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the Foreign key contraints for the Shard Mapping tables.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(
                    ShardMapping.DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID)
        return True

    @staticmethod
    def fetch(table_name, persister=None):
        """Fetch the shard specification mapping for the given table

        :param table_name: The name of the table for which the sharding
                            specification is being queried.
        :param persister: A valid handle to the state store.
        :return: The ShardMapping object that encapsulates the shard mapping
                    information for the given table.
        :raises: Sharding Error if the table name is not found.
        """
        cur = persister.exec_stmt(
                                  ShardMapping.SELECT_SHARD_MAPPING,
                                  {"raw" : False,
                                  "fetch" : False,
                                  "params" : (table_name,)})
        row = cur.fetchone()
        if row is None:
            raise _errors.ShardingError("Table Name Not found")
        if row:
            return ShardMapping(row[0], row[1], row[2], row[3], row[4])

    @staticmethod
    def fetch_shard_mapping_defn(shard_mapping_id, persister=None):
        """Fetch the shard mapping definition corresponding to the
        shard mapping id.

        :param shard_mapping_id: The id of the shard mapping definition that
                                    needs to be fetched.
        :param persister: A valid handle to the state store.
        :return: A list containing the shard mapping definition parameters.
        """
        row = persister.exec_stmt(ShardMapping.SELECT_SHARD_MAPPING_DEFN,
                                  {"raw" : False,
                                  "fetch" : True,
                                  "params" : (shard_mapping_id,)})
        if row is None:
            raise _errors.ShardingError("Table Name Not found")
        if row:
            return row[0]

    @staticmethod
    def list_shard_mapping_defn(persister=None):
        """Fetch all the shard mapping definitions.
            :param persister: A valid handle to the state store.
            :return: A list containing the shard mapping definitions.
        """
        rows = persister.exec_stmt(ShardMapping.LIST_SHARD_MAPPING_DEFN,
                                  {"raw" : False,
                                  "fetch" : True})
        if rows is None:
            raise _errors.ShardingError("No shard mapping definitions found")
        return rows

    @staticmethod
    def define(type_name, global_group_id, persister=None):
        """Define a shard mapping.

        :param type_name: The type of sharding scheme - RANGE, HASH, LIST etc
        :param global_group: Every shard mapping is associated with a
                            Global Group that stores the global updates
                            and the schema changes for this shard mapping
                            and dissipates these to the shards.
        :param persister: A valid handle to the state store.
        :return: The shard_mapping_id generated for the shard mapping.
        """
        persister.exec_stmt(
            ShardMapping.DEFINE_SHARD_MAPPING,
            {"params":(type_name, global_group_id)})
        row = persister.exec_stmt("SELECT LAST_INSERT_ID()")
        return int(row[0][0])


    @staticmethod
    def add(shard_mapping_id, table_name, column_name, persister=None):
        """Add a table to a shard mapping.

        :param shard_mapping_id: The shard mapping id to which the input
                                    table is attached.
        :param table_name: The table being sharded.
        :param column_name: The column whose value is used in the sharding
                            scheme being applied
        :param persister: A valid handle to the state store.

        :return: The ShardMapping object for the mapping created.
                 None if the insert failed.

        """
        persister.exec_stmt(
            ShardMapping.ADD_SHARD_MAPPING,
            {"params":(shard_mapping_id, table_name, column_name)})
        return ShardMapping.fetch(table_name)

class Shards(_persistence.Persistable):
    """Contains the mapping between the Shard ID and the Group ID

    A typical mapping between the shards and their location looks like
    the following

        +--------------------+--------------------+
        |     shard_id       |      group_id      |
        +====================+====================+
        |1                   |GroupID1            |
        +--------------------+--------------------+

    The columns are explained as follows,

    shard_id - Unique identifier for the shard of a particular table
    group_id - The Server location for the given partition of the table.
    """

    #Create the schema for storing the shard to groups mapping
    CREATE_SHARDS = ("CREATE TABLE "
                    "Shards (shard_id INT AUTO_INCREMENT "
                    "NOT NULL PRIMARY KEY, "
                    "group_id VARCHAR(64) UNIQUE NOT NULL)")

    #Create the referential integrity constraint with the groups table.
    ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID = \
                                ("ALTER TABLE Shards "
                                  "ADD CONSTRAINT fk_shards_group_id "
                                  "FOREIGN KEY(group_id) REFERENCES "
                                  "groups(group_id)")

    #Drop the referential integrity constraint with the groups table.
    DROP_FOREIGN_KEY_CONSTRAINT_GROUP_ID = \
                                    ("ALTER TABLE Shards DROP "
                                     "FOREIGN KEY fk_shards_group_id")

    #Drop the schema for storing the shard to group mapping.
    DROP_SHARDS = ("DROP TABLE Shards")

    #Insert the Range to Shard mapping into the table.
    INSERT_SHARD = ("INSERT INTO Shards(group_id) VALUES(%s)")

    #Update the group_id for a shard.
    UPDATE_SHARD = ("UPDATE Shards SET group_id=%s WHERE shard_id=%s")

    #Delete a given shard to group mapping.
    DELETE_SHARD = ("DELETE FROM Shards WHERE shard_id = %s")

    #Select the group to which a shard ID maps to.
    SELECT_SHARD_GROUP = ("SELECT group_id FROM Shards "
                                  "WHERE shard_id = %s")

    def __init__(self, shard_id, group_id):
        """Initialize the Shards object with the shard to group mapping.

        :param shard_id: An unique identification, a logical representation for a
                    shard of a particular table.
        :param group_id: The group ID to which the shard maps to.
        """
        super(Shards, self).__init__()
        self.__shard_id = shard_id
        self.__group_id = group_id

    @staticmethod
    def create(persister=None):
        """Create the schema to store the current Shard to Group mapping.

        :param persister: A valid handle to the state store.

        :return: True if the query succeeded but there is no result set
                False if the query failed
        """
        persister.exec_stmt(Shards.CREATE_SHARDS)
        return True

    @staticmethod
    def drop(persister=None):
        """Drop the schema to store the current Shard to Group mapping.

        :param persister: A valid handle to the state store.

        :return: True if the query succeeded but there is no result set
                False if the query failed
        """
        persister.exec_stmt(Shards.DROP_SHARDS)
        return True

    @staticmethod
    def add(group_id, persister=None):
        """Add a Group that will store a shard. A shard ID is automatically
        generated for a given added Group.

        :param group_id: The Group that is being added to store a shard.
        :param persister: A valid handle to the state store.

        :return: The Shards object containing a mapping between the shard
                    and the group.
        """
        persister.exec_stmt(Shards.INSERT_SHARD, {"params":(group_id,)})
        row = persister.exec_stmt("SELECT LAST_INSERT_ID()")
        return Shards(int(row[0][0]), group_id)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the Shards tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID)
        return True

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the constraints on the Shards tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.DROP_FOREIGN_KEY_CONSTRAINT_GROUP_ID)
        return True

    def remove(self, persister=None):
        """Remove the Shard to Group mapping.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.DELETE_SHARD, \
                            {"params":(self.__shard_id,)})
        return True

    @staticmethod
    def fetch(shard_id, persister=None):
        """Fetch the Shards object containing the group_id for the input
        shard ID.

        :param shard_id: That shard ID whose mapping needs to be fetched.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        row = persister.exec_stmt(Shards.SELECT_SHARD_GROUP, \
                                  {"params":(shard_id,)})
        #TODO: exec_stmt can return a list as well as a cursor, but
        #TODO: They are done in a consistent manner. It should either
        #TODO: return a list of a cursor everywhere in the sharding
        #TODO: code.
        return Shards(shard_id, row[0][0])

    @property
    def shard_id(self):
        """Return the shard ID for the Shard to Group mapping.
        """
        return self.__shard_id

    @property
    def group_id(self):
        """Return the Group ID for the Shard to Group mapping.
        """
        return self.__group_id

class RangeShardingSpecification(_persistence.Persistable):
    """Represents a RANGE sharding specification. The class helps encapsulate
    the representation of a typical RANGE sharding implementation in the
    state store.

    A typical RANGE sharding representation looks like the following,

        +--------------+---------+--------+-----------+-----------+
        | shard_map_id |   LB    |  UB    |  shard_id |   state   |
        +==============+=========+========+===========+===========+
        |1             |10000    |20000   |1          |ENABLED    |
        +--------------+---------+--------+-----------+-----------+

    The columns in the above table are explained as follows,

    * shard_mapping_id - The unique identification for a shard mapping.
    * LB -The lower bound of the given RANGE sharding scheme instance
    * UB - The upper bound of the given RANGE sharding scheme instance
    * shard_id - An unique identification, a logical representation for a
                    shard of a particular table.
    * state - Indicates whether a given shard is ENABLED or DISABLED
    """

#TODO: Create Forgein keys and Indexes for the table.
#TODO: verify if state should be string or if a enum or bool is possible
#TODO: MySQL does not support CHECK contraint. If it does restrict State
#TODO: to store only 'ENABLED' and 'DISABLED'
    #Create the schema for storing the RANGE sharding specificaton.
    CREATE_RANGE_SPECIFICATION = ("CREATE TABLE "
                                "range_sharding_specification "
                                "(shard_mapping_id INT NOT NULL, "
                                "INDEX(shard_mapping_id), "
                                "lower_bound INT NOT NULL, "
                                "upper_bound INT NOT NULL, "
                                "INDEX(lower_bound, upper_bound), "
                                "shard_id INT NOT NULL, "
                                "state ENUM('DISABLED', 'ENABLED') NOT NULL)")

    #Create the referential integrity constraint with the shard_mapping_defn
    #table
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                ("ALTER TABLE range_sharding_specification "
                                  "ADD CONSTRAINT "
                                  "fk_shard_mapping_id_sharding_spec "
                                  "FOREIGN KEY(shard_mapping_id) REFERENCES "
                                  "shard_mapping_defn(shard_mapping_id)")

    #Drop the referential integrity constraint with the shard_mapping_defn
    #table
    DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                    ("ALTER TABLE range_sharding_specification "
                                       "DROP FOREIGN KEY "
                                       "fk_shard_mapping_id_sharding_spec")

#TODO: Verify if the table name and the column name can be he
#TODO: same value.
    #Create the referential integrity constraint with the shard_id
    #table
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_ID = \
                                ("ALTER TABLE range_sharding_specification "
                                  "ADD CONSTRAINT fk_shard_id_sharding_spec "
                                  "FOREIGN KEY(shard_id) REFERENCES "
                                  "Shards(shard_id)")

    #Drop the referential integrity constraint with the shard_id
    #table
    DROP_FOREIGN_KEY_CONSTRAINT_SHARD_ID = \
                                    ("ALTER TABLE range_sharding_specification "
                                       "DROP FOREIGN KEY "
                                       "fk_shard_id_sharding_spec")

    #Drop the schema for storing the RANGE sharding specification.
    DROP_RANGE_SPECIFICATION = ("DROP TABLE "
                                "range_sharding_specification")

    #Insert a RANGE of keys and the server to which they belong.
    INSERT_RANGE_SPECIFICATION = ("INSERT INTO "
                                  "range_sharding_specification "
                                  "VALUES(%s, %s, %s, %s, %s)")

    #Update the state of a shard
    UPDATE_RANGE_SPECIFICATION_STATUS = ("UPDATE range_sharding_specification "
                                         "SET state=%s "
                                         "where shard_id=%s")

    #Delete a given RANGE specification instance.
    DELETE_RANGE_SPECIFICATION = ("DELETE FROM "
                                  "range_sharding_specification "
                                  "WHERE "
                                  "shard_id = %s")

    #Given a Shard ID select the RANGE Scheme that it defines.
    SELECT_RANGE_SPECIFICATION = ("SELECT shard_mapping_id, lower_bound, "
                                  "upper_bound, "
                                  "shard_id, "
                                  "state "
                                  "FROM range_sharding_specification "
                                  "WHERE shard_id = %s")

    #Given a Shard Mapping ID select all the RANGE mappings that it
    #defines.
    LIST_RANGE_SPECIFICATION = ("SELECT shard_mapping_id, lower_bound, "
                                  "upper_bound, "
                                  "shard_id, "
                                  "state "
                                  "FROM range_sharding_specification "
                                  "WHERE shard_mapping_id = %s")

    #Select the server corresponding to the RANGE to which a given key
    #belongs
    LOOKUP_KEY = ("SELECT shard_mapping_id, lower_bound, upper_bound, "
                  "shard_id, state "
                  "FROM range_sharding_specification "
                  "WHERE %s >= lower_bound AND %s <= upper_bound AND "
                  "shard_mapping_id = %s")

    def __init__(self, shard_mapping_id, lower_bound, upper_bound,
                  shard_id, state="DISABLED"):
        """Initialize a given RANGE sharding mapping specification.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given RANGE sharding defn
        :param upper_bound: The upper bound of the given RANGE sharding defn
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.
        :param state: Indicates whether a given shard is ENABLED or DISABLED
        """
        super(RangeShardingSpecification, self).__init__()
        self.__shard_mapping_id = shard_mapping_id
        self.__lower_bound = lower_bound
        self.__upper_bound = upper_bound
        self.__shard_id = shard_id
        self.__state = state

    @property
    def shard_mapping_id(self):
        """Return the shard mapping to which this RANGE definition belongs.
        """
        return self.__shard_mapping_id

    @property
    def lower_bound(self):
        """Return the lower bound of this RANGE specification.
        """
        return self.__lower_bound

    @property
    def upper_bound(self):
        """Return the upper bound of this RANGE specification.
        """
        return self.__upper_bound

    @property
    def shard_id(self):
        """Return the Shard ID of the shard for this RANGE sharding
        definition
        """
        return self.__shard_id

    @property
    def state(self):
        """Return whether the state is ENABLED or DISABLED.
        """
        return self.__state

    def enable(self, persister=None):
        """Set the state of the shard to ENABLED.
        """
        persister.exec_stmt(
          RangeShardingSpecification.UPDATE_RANGE_SPECIFICATION_STATUS,
                             {"params":('ENABLED', self.__shard_id)})
        return True

    def disable(self, persister=None):
        """Set the state of the shard to DISABLED.
        """
        persister.exec_stmt(
          RangeShardingSpecification.UPDATE_RANGE_SPECIFICATION_STATUS,
                             {"params":('DISABLED', self.__shard_id)})
        return True

    def remove(self, persister=None):
        """Remove the RANGE specification mapping represented by the current
        RANGE shard specification object.

        :param persister: Represents a valid handle to the state store.
        :return: True if the remove succeeded
                False if the query failed
        """
#TODO: Throw an exception if the shard is not disabled.
        if self.__state == "DISABLED":
            persister.exec_stmt(
                RangeShardingSpecification.DELETE_RANGE_SPECIFICATION,
                {"params":(self.__shard_id,)})
            return True
        else:
            return False

    @staticmethod
    def add(shard_mapping_id, lower_bound, upper_bound,
                  shard_id, state="DISABLED", persister=None):
        """Add the RANGE shard specification. This represents a single instance
        of a shard specification that maps a key RANGE to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given RANGE sharding defn
        :param upper_bound: The upper bound of the given RANGE sharding defn
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.
        :param state: Indicates whether a given shard is ENABLED or DISABLED

        :return: A RangeShardSpecification object representing the current
                Range specification.
                None if the insert into the state store failed
        """
        persister.exec_stmt(
                  RangeShardingSpecification.INSERT_RANGE_SPECIFICATION,
                                     {"params":(shard_mapping_id,
                                                lower_bound,
                                                upper_bound,
                                                shard_id,
                                                state)})
        return RangeShardingSpecification(shard_mapping_id, lower_bound,
                                          upper_bound, shard_id, state)

    @staticmethod
    def create(persister=None):
        """Create the schema to store the current RANGE sharding specification.

        :param persister: A valid handle to the state store.

        :return: True if the query succeeded but there is no result set
                False if the query failed
        """

        persister.exec_stmt(
                    RangeShardingSpecification.CREATE_RANGE_SPECIFICATION)
        return True

    @staticmethod
    def drop(persister=None):
        """Drop the Range shard specification schema.

        :param persister: A valid handle to the state store.

        :return: True if the query succeeded but there is no result set
                False if the query failed
        """
        persister.exec_stmt(
                        RangeShardingSpecification.DROP_RANGE_SPECIFICATION)
        return True

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the sharding tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.\
            ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID
            )
        persister.exec_stmt(
            RangeShardingSpecification.ADD_FOREIGN_KEY_CONSTRAINT_SHARD_ID
            )
        return True

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the constraints on the sharding tables.

        :param persister: The DB server that can be used to access the
                  state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.DROP_FOREIGN_KEY_CONSTRAINT_SHARD_ID
            )
        persister.exec_stmt(
            RangeShardingSpecification.\
            DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID
            )
        return True

#TODO: Should fetch accept a shard_mapping_id or a shard_id ?
#TODO: Should there be a method that accepts shard_mapping_id like list?
#TODO: Changing fetch to list, and adding a fetch that accepts a shard_id.
#TODO: Verify this is correct.
    @staticmethod
    def list(shard_mapping_id, persister=None):
        """Return the RangeShardingSpecification objects corresponding to the
        given sharding scheme.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: A  list of RangeShardingSpecification objects which belong to
                to this shard mapping.
                None if the shard mapping is not found
        :raises: Sharding Error if the Sharding Specification is not found.
        """
        range_sharding_specifications = []
        cur = persister.exec_stmt(
                    RangeShardingSpecification.LIST_RANGE_SPECIFICATION,
                        {"raw" : False,
                        "fetch" : False,
                        "params" : (shard_mapping_id,)})
        rows = cur.fetchall()
        for row in rows:
            range_sharding_specifications.append(RangeShardingSpecification
                                            (row[0], row[1], row[2],
                                             row[3], row[4]))
        if range_sharding_specifications == []:
            raise _errors.ShardingError("Sharding specification not found")
        return  range_sharding_specifications

    @staticmethod
    def fetch(shard_id, persister=None):
        """Return the RangeShardingSpecification object corresponding to the
        given sharding ID.

        :param shard_id: The unique identification for a shard.
        :param persister: A valid handle to the state store.

        :return: A list that represents the information in the
                    RangeShardingSpecification.
                    An Empty list otherwise.
        :raises: Sharding Error if the Sharding Specification is not found.
        """
        cur = persister.exec_stmt(
                    RangeShardingSpecification.SELECT_RANGE_SPECIFICATION,
                        {"raw" : False,
                        "fetch" : False,
                        "params" : (shard_id,)})
        row = cur.fetchone()
        range_sharding_specification = RangeShardingSpecification(
                                                 row[0], row[1], row[2],
                                                 row[3], row[4])
        if range_sharding_specification is None:
            raise _errors.ShardingError("Sharding specification not found")
        return  range_sharding_specification

#TODO: Should a lookup fail if a shard is DISABLED ?
    @staticmethod
    def lookup(key, shard_mapping_id, persister=None):
        """Return the Range sharding specification in whose key range the input
            key falls.

        :param key: The key which needs to be checked to see which range it
                    falls into
        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: The Range Sharding Specification that contains the range in
                which the key belongs.
        :raises: Sharding Error if the sharding specification is not found
        """
        cur = persister.exec_stmt(
                        RangeShardingSpecification.LOOKUP_KEY,
                        {"raw" : False,
                        "fetch" : False,
                        "params" : (key, key, shard_mapping_id)})
        row = cur.fetchone()
        if row is None:
            raise _errors.ShardingError("Sharding specification not found")
        if row:
            return RangeShardingSpecification(row[0], row[1], row[2],
                                              row[3], row[4])

    @staticmethod
    def delete_from_shard_db(table_name):
        """Delete the data from the copied data directories based on the
        sharding configuration uploaded in the sharding tables of the state
        store. The basic logic consists of

        * Querying the shard mapping ID corresponding to the sharding
          table.
        * Using the shard mapping ID to find the type of shard scheme and hence
            the sharding scheme table to query in.
        * Querying the sharding key range using the shard mapping ID.
        * Deleting the sharding keys that fall outside the range for a given
          server.

        :param table_name: The table being sharded.

        :return: False If the delete fails
                True if the delete succeeds.
        """

        #Get the shard mapping for the table from the state store.
        shard_mapping = ShardMapping.fetch(table_name)

        #Get the shard mapping ID
        shard_mapping_id = shard_mapping.shard_mapping_id

        #Query the range sharding specifications defined for the
        #shard mapping id
        range_sharding_specs = RangeShardingSpecification.list \
                                                (shard_mapping_id)

        #Use the information in each of the range sharding specs to prune the
        #tables.
        for range_sharding_spec in range_sharding_specs:
            #Form the delete query using the shard mapping and the shard spec
            #information
            delete_query = \
                ("DELETE FROM %s WHERE %s NOT BETWEEN %s and %s")% \
                                                (table_name,
                                                shard_mapping.column_name,
                                                range_sharding_spec.lower_bound,
                                                range_sharding_spec.upper_bound)

            #Fetch the shard information using the shard_id
            shard = Shards.fetch(range_sharding_spec.shard_id)

            #Fetch the Group object using the group id in the shard information
            group = Group.fetch(shard.group_id)

            #Fetch the master of the group
            master = MySQLServer.fetch(group.master)

            #Get a connection
            master.connect()

            #Fire the DELETE query
            master.exec_stmt(delete_query)
        return True

def lookup_servers(table_name, key):
    """Given a table name and a key return the server where the shard of this
    table can be found

    :param table_name: The table whose sharding specification needs to be
                        looked up.
    :param key: The key value that needs to be looked up

    :return: The master UUID of the Group that contains the range in which the
            key belongs None if lookup fails.
    :raises: Sharding Error if the table name does not map to a Group.
    """
    group = None
    shard_mapping = ShardMapping.fetch(table_name)
    if shard_mapping.type_name == "RANGE":
        range_sharding_specification = RangeShardingSpecification.lookup \
                                        (key,
                                         shard_mapping.shard_mapping_id)
        shard = Shards.fetch(str(range_sharding_specification.shard_id))
        # TODO: This should reuse whatever is available in services.group.py
        # instead of copying the code from there.
        group = Group.fetch(shard.group_id)
    ret = []
    for server in group.servers():
        ret.append([str(server.uuid), server.address,
                   group.master == server.uuid])
    return ret

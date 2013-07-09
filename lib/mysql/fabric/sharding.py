"""This module contains the plumbing necessary for creating, modifying and
querying the sharding information from the state stores.

"""

import mysql.fabric.errors as _errors
import mysql.fabric.persistence as _persistence

from mysql.fabric.server import MySQLServer, Group

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
    CREATE_SHARD_MAPPING = ("CREATE TABLE shard_tables"
                            "(shard_mapping_id INT NOT NULL, "
                            "table_name VARCHAR(64) NOT NULL, "
                            "column_name VARCHAR(64) NOT NULL, "
                            "PRIMARY KEY (table_name, column_name), "
                            "INDEX(shard_mapping_id))")
    CREATE_SHARD_MAPPING_DEFN = ("CREATE TABLE shard_maps"
                                 "(shard_mapping_id INT AUTO_INCREMENT "
                                 "NOT NULL PRIMARY KEY, "
                                 "type_name ENUM('RANGE', 'HASH') NOT NULL, "
                                 "global_group VARCHAR(64))")

    #Drop the schema for the tables used to store the shard specification
    #mapping information
    DROP_SHARD_MAPPING = ("DROP TABLE shard_tables")
    DROP_SHARD_MAPPING_DEFN = ("DROP TABLE shard_maps")

    #Create the referential integrity constraint with the shard_maps
    #table.
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                ("ALTER TABLE shard_tables "
                                  "ADD CONSTRAINT fk_shard_mapping_id "
                                  "FOREIGN KEY(shard_mapping_id) REFERENCES "
                                  "shard_maps(shard_mapping_id)")

    #Drop the referential integrity constraint with the shard_maps
    #table.
    DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                    ("ALTER TABLE shard_tables DROP "
                                     "FOREIGN KEY fk_shard_mapping_id")

    #Create the referential integrity constraint with the groups table.
    ADD_FOREIGN_KEY_CONSTRAINT_GLOBAL_GROUP = \
                            ("ALTER TABLE shard_maps "
                            "ADD CONSTRAINT fk_shard_mapping_global_group "
                            "FOREIGN KEY(global_group) REFERENCES "
                            "groups(group_id)")

    #Drop the referential integrity constraint with the groups table.
    DROP_FOREIGN_KEY_CONSTRAINT_GLOBAL_GROUP = \
                                ("ALTER TABLE shard_maps DROP "
                                "FOREIGN KEY fk_shard_mapping_global_group")

    #Define a shard mapping.
    DEFINE_SHARD_MAPPING = ("INSERT INTO "
                            "shard_maps(type_name, global_group) "
                            "VALUES(%s, %s)")
    ADD_SHARD_MAPPING = ("INSERT INTO shard_tables"
                         "(shard_mapping_id, table_name, column_name) "
                         "VALUES(%s, %s, %s)")

    #select the shard specification mapping information for a table
    SELECT_SHARD_MAPPING = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_tables as sm, "
                            "shard_maps as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND table_name = %s")

    #Select the shard mapping for a given shard mapping ID.
    SELECT_SHARD_MAPPING_BY_ID = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_tables as sm, "
                            "shard_maps as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND sm.shard_mapping_id = %s")

    #Select all the shard specifications of a particular sharding type name.
    LIST_SHARD_MAPPINGS = ("SELECT sm.shard_mapping_id, table_name, "
                            "column_name, type_name, "
                            "global_group "
                            "FROM shard_tables as sm, "
                            "shard_maps as smd "
                            "WHERE sm.shard_mapping_id = smd.shard_mapping_id "
                            "AND type_name = %s")

    #Select the shard mapping for a given shard mapping ID.
    SELECT_SHARD_MAPPING_DEFN = ("SELECT shard_mapping_id, type_name, "
                                 "global_group FROM shard_maps "
                                 "WHERE shard_mapping_id = %s")

    #Select all the shard mapping definitions
    LIST_SHARD_MAPPING_DEFN = ("SELECT shard_mapping_id, type_name, "
                               "global_group FROM shard_maps")

    #Delete the sharding specification to table mapping for a given table.
    DELETE_SHARD_MAPPING = ("DELETE FROM shard_tables "
                            "WHERE shard_mapping_id = %s")

    #Delete the shard mapping definition
    DELETE_SHARD_MAPPING_DEFN = ("DELETE FROM shard_maps "
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
        """

        sharding_type = sharding_type.upper()
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
        return [ ShardMapping(*row[0:5]) for row in rows ]

    # TODO: FOLLOW THE SAME PATTERN ADOPTED IN MySQLServer.
    def remove(self, persister=None):
        """Remove the shard mapping represented by the Shard Mapping object.
        The method basically results in removing the association between a
        table and the sharding specification used to shard this table.

        :param persister: Represents a valid handle to the state
                          store.
        """
        #Remove the shard mapping
        persister.exec_stmt(
            ShardMapping.DELETE_SHARD_MAPPING,
            {"params":(self.__shard_mapping_id,)})

        #Remove the shard mapping definition.
        persister.exec_stmt(
            ShardMapping.DELETE_SHARD_MAPPING_DEFN,
            {"params":(self.__shard_mapping_id,)})

    @staticmethod
    def create(persister=None):
        """Create the schema to store the mapping between the table and
        the sharding specification.

        :param persister: A valid handle to the state store.
        """

        persister.exec_stmt(ShardMapping.CREATE_SHARD_MAPPING)
        persister.exec_stmt(ShardMapping.CREATE_SHARD_MAPPING_DEFN)

    @staticmethod
    def drop(persister=None):
        """Drop the schema for the table used to store the mapping between
        the table and the sharding specificaton.

        :param persister: A valid handle to the state store.
        """

        persister.exec_stmt(ShardMapping.DROP_SHARD_MAPPING_DEFN)
        persister.exec_stmt(ShardMapping.DROP_SHARD_MAPPING)

    @staticmethod
    def add_constraints(persister=None):
        """Add the Foreign key contraints for the Shard Mapping tables.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(
                    ShardMapping.ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID)
        persister.exec_stmt(
                    ShardMapping.ADD_FOREIGN_KEY_CONSTRAINT_GLOBAL_GROUP)

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the Foreign key contraints for the Shard Mapping tables.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(
                    ShardMapping.DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID)
        persister.exec_stmt(
                    ShardMapping.DROP_FOREIGN_KEY_CONSTRAINT_GLOBAL_GROUP)

    @staticmethod
    def fetch(table_name, persister=None):
        """Fetch the shard specification mapping for the given table

        :param table_name: The name of the table for which the sharding
                            specification is being queried.
        :param persister: A valid handle to the state store.
        :return: The ShardMapping object that encapsulates the shard mapping
                    information for the given table.
        """
        cur = persister.exec_stmt(
                                  ShardMapping.SELECT_SHARD_MAPPING,
                                  {"raw" : False,
                                  "fetch" : False,
                                  "params" : (table_name,)})
        row = cur.fetchone()
        if row:
            return ShardMapping(row[0], row[1], row[2], row[3], row[4])

        return None

    @staticmethod
    def fetch_by_id(shard_mapping_id, persister=None):
        """Fetch the shard specification mapping for the given shard mapping ID.

        :param shard_mapping_id: The shard mapping id for which the sharding
                            specification is being queried.
        :param persister: A valid handle to the state store.
        :return: The ShardMapping object that encapsulates the shard mapping
                    information for the given shard mapping ID.
        """
        cur = persister.exec_stmt(
                                  ShardMapping.SELECT_SHARD_MAPPING_BY_ID,
                                  {"raw" : False,
                                  "fetch" : False,
                                  "params" : (shard_mapping_id,)})
        row = cur.fetchone()
        if row:
            return ShardMapping(row[0], row[1], row[2], row[3], row[4])

        return None

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
        if row is not None:
            #There is no abstraction for a shard mapping definition. A
            #shard mapping definition is just a triplet of
            #(shard_id, sharding_type, global_group)
            return row[0]

        return None

    @staticmethod
    def list_shard_mapping_defn(persister=None):
        """Fetch all the shard mapping definitions.
            :param persister: A valid handle to the state store.
            :return: A list containing the shard mapping definitions.
        """
        rows = persister.exec_stmt(ShardMapping.LIST_SHARD_MAPPING_DEFN,
                                  {"raw" : False,
                                  "fetch" : True})
        if rows is not None:
            return rows

        return []

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

+--------------------+--------------------+--------------------+
|     shard_id       |      group_id      |     state          | 
+====================+====================+====================+
|1                   |GroupID1            |ENABLED             |
+--------------------+--------------------+--------------------+

    The columns are explained as follows,

    shard_id - Unique identifier for the shard of a particular table.
    group_id - The Server location for the given partition of the table.
    state - Indicates whether a given shard is ENABLED or DISABLED.
    """

    #Tuple stores the list of valid shard mapping types.
    VALID_SHARDING_TYPES = ('RANGE', 'HASH')

    #Valid states of the shards
    VALID_SHARD_STATES = ("ENABLED", "DISABLED")

    #Create the schema for storing the shard to groups mapping
    CREATE_SHARDS = ("CREATE TABLE shards ("
                    "shard_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, "
                    "group_id VARCHAR(64) UNIQUE NOT NULL, "
                    "state ENUM('DISABLED', 'ENABLED') NOT NULL)")

    #Create the referential integrity constraint with the groups table.
    ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID = \
                                ("ALTER TABLE shards "
                                  "ADD CONSTRAINT fk_shards_group_id "
                                  "FOREIGN KEY(group_id) REFERENCES "
                                  "groups(group_id)")

    #Drop the referential integrity constraint with the groups table.
    DROP_FOREIGN_KEY_CONSTRAINT_GROUP_ID = \
                                    ("ALTER TABLE shards DROP "
                                     "FOREIGN KEY fk_shards_group_id")

    #Create the referential integrity constraint with the shard_mapping_defn
    #table
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                ("ALTER TABLE shards "
                                  "ADD CONSTRAINT "
                                  "fk_shard_mapping_id_sharding_spec "
                                  "FOREIGN KEY(shard_mapping_id) REFERENCES "
                                  "shard_maps(shard_mapping_id)")

    #Drop the referential integrity constraint with the shard_maps
    #table
    DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = \
                                    ("ALTER TABLE shards "
                                       "DROP FOREIGN KEY "
                                       "fk_shard_mapping_id_sharding_spec")

    #Drop the schema for storing the shard to group mapping.
    DROP_SHARDS = ("DROP TABLE shards")

    #Insert the Range to Shard mapping into the table.
    INSERT_SHARD = ("INSERT INTO shards(group_id, state) VALUES(%s, %s)")

    #Update the group_id for a shard.
    UPDATE_SHARD = ("UPDATE shards SET group_id=%s WHERE shard_id=%s")

    #Delete a given shard to group mapping.
    DELETE_SHARD = ("DELETE FROM shards WHERE shard_id = %s")

    #Select the group to which a shard ID maps to.
    SELECT_SHARD = ("SELECT shard_id, group_id, state "
                                    "FROM shards WHERE shard_id = %s")

    #Select the shard that belongs to a given group.
    SELECT_GROUP_FOR_SHARD = ("SELECT shard_id FROM shards WHERE group_id = %s")

    #Update the state of a shard
    UPDATE_SHARD_STATE = ("UPDATE shards SET state=%s where shard_id=%s")

    def __init__(self, shard_id, group_id,  state="DISABLED"):
        """Initialize the Shards object with the shard to group mapping.

        :param shard_id: An unique identification, a logical representation for a
                    shard of a particular table.
        :param group_id: The group ID to which the shard maps to.
        :param state: Indicates whether a given shard is ENABLED or DISABLED
        """
        super(Shards, self).__init__()
        self.__shard_id = shard_id
        self.__group_id = group_id
        self.__state = state

    @staticmethod
    def create(persister=None):
        """Create the schema to store the current Shard to Group mapping.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(Shards.CREATE_SHARDS)

    @staticmethod
    def drop(persister=None):
        """Drop the schema to store the current Shard to Group mapping.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(Shards.DROP_SHARDS)

    @staticmethod
    def add(group_id, state="DISABLED", persister=None):
        """Add a Group that will store a shard. A shard ID is automatically
        generated for a given added Group.

        :param group_id: The Group that is being added to store a shard.
        :param persister: A valid handle to the state store.
        :param state: Indicates whether a given shard is ENABLED or DISABLED

        :return: The Shards object containing a mapping between the shard
                    and the group.
        """
        persister.exec_stmt(Shards.INSERT_SHARD, {"params":(group_id, state)})
        row = persister.exec_stmt("SELECT LAST_INSERT_ID()")
        return Shards(int(row[0][0]), group_id, state)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the Shards tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.ADD_FOREIGN_KEY_CONSTRAINT_GROUP_ID)

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the constraints on the Shards tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.DROP_FOREIGN_KEY_CONSTRAINT_GROUP_ID)

    def remove(self, persister=None):
        """Remove the Shard to Group mapping.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(Shards.DELETE_SHARD, \
                            {"params":(self.__shard_id,)})

    @staticmethod
    def fetch(shard_id, persister=None):
        """Fetch the Shards object containing the group_id for the input
        shard ID.

        :param shard_id: That shard ID whose mapping needs to be fetched.
        :param persister: The DB server that can be used to access the
                          state store.
        """
        row = persister.exec_stmt(Shards.SELECT_SHARD, \
                                  {"params":(shard_id,)})
        #TODO: exec_stmt can return a list as well as a cursor, but
        #TODO: They are done in a consistent manner. It should either
        #TODO: return a list of a cursor everywhere in the sharding
        #TODO: code.

        if row is None:
            return None

        return Shards(row[0][0], row[0][1], row[0][2],)

    def enable(self, persister=None):
        """Set the state of the shard to ENABLED.
        """
        persister.exec_stmt(
          Shards.UPDATE_SHARD_STATE,
                             {"params":('ENABLED', self.__shard_id)})

    def disable(self, persister=None):
        """Set the state of the shard to DISABLED.
        """
        persister.exec_stmt(
          Shards.UPDATE_SHARD_STATE,
                             {"params":('DISABLED', self.__shard_id)})

    @staticmethod
    def lookup_shard_id(group_id,  persister=None):
        """Fetch the shard ID for the given Group.

        :param group_id: The Group that is being looked up.
        :param persister: A valid handle to the state store.

        :return: The shard_id contained in the given group_id.
        """
        row = persister.exec_stmt(Shards.SELECT_GROUP_FOR_SHARD, \
                                  {"params":(group_id,)})
        if row:
            return row[0][0]

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

    @group_id.setter
    def group_id(self,  group_id,  persister=None):
        """Set the group_id for the Shard.

        :param group_id: The Group that is being added to store a shard.
        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(Shards.UPDATE_SHARD,
                                        {"params":(group_id, self.__shard_id)})
        self.__group_id = group_id

    @property
    def state(self):
        """Return whether the state is ENABLED or DISABLED.
        """
        return self.__state

class RangeShardingSpecification(_persistence.Persistable):
    """Represents a RANGE sharding specification. The class helps encapsulate
    the representation of a typical RANGE sharding implementation in the
    state store.

    A typical RANGE sharding representation looks like the following,

        +--------------+---------+-----------+
        | shard_map_id |   LB    |  shard_id |
        +==============+=========+===========+
        |1             |10000    |1          |
        +--------------+---------+-----------+

    The columns in the above table are explained as follows,

    * shard_mapping_id - The unique identification for a shard mapping.
    * LB -The lower bound of the given RANGE sharding scheme instance
    * shard_id - An unique identification, a logical representation for a
                    shard of a particular table.
    """

    #Create the schema for storing the RANGE sharding specificaton.
    CREATE_RANGE_SPECIFICATION = ("CREATE TABLE "
                                "shard_ranges "
                                "(shard_mapping_id INT NOT NULL, "
                                "INDEX(shard_mapping_id), "
                                "lower_bound VARBINARY(16) NOT NULL, "
                                "INDEX(lower_bound), "
                                "UNIQUE(shard_mapping_id, lower_bound), "
                                "shard_id INT NOT NULL)")

    #Create the referential integrity constraint with the shard_mapping_defn
    #table
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = (
        "ALTER TABLE shard_ranges "
        "ADD CONSTRAINT "
        "fk_shard_mapping_id_sharding_spec "
        "FOREIGN KEY(shard_mapping_id) REFERENCES "
        "shard_mapping_defn(shard_mapping_id)"
    )

    #Drop the referential integrity constraint with the shard_mapping_defn
    #table
    DROP_FOREIGN_KEY_CONSTRAINT_SHARD_MAPPING_ID = (
        "ALTER TABLE shard_ranges "
        "DROP FOREIGN KEY "
        "fk_shard_mapping_id_sharding_spec"
    )

    #Create the referential integrity constraint with the shard_id
    #table
    ADD_FOREIGN_KEY_CONSTRAINT_SHARD_ID = \
                                ("ALTER TABLE shard_ranges "
                                  "ADD CONSTRAINT fk_shard_id_sharding_spec "
                                  "FOREIGN KEY(shard_id) REFERENCES "
                                  "shards(shard_id)")

    #Drop the referential integrity constraint with the shard_id
    #table
    DROP_FOREIGN_KEY_CONSTRAINT_SHARD_ID = \
                                    ("ALTER TABLE shard_ranges "
                                       "DROP FOREIGN KEY "
                                       "fk_shard_id_sharding_spec")

    #Drop the schema for storing the RANGE sharding specification.
    DROP_RANGE_SPECIFICATION = ("DROP TABLE shard_ranges")

    #Insert a RANGE of keys and the server to which they belong.
    INSERT_RANGE_SPECIFICATION = ("INSERT INTO shard_ranges"
        "(shard_mapping_id, lower_bound, shard_id) VALUES(%s, %s, %s)")

    #Delete a given RANGE specification instance.
    DELETE_RANGE_SPECIFICATION = ("DELETE FROM shard_ranges "
                                  "WHERE "
                                  "shard_id = %s")

    #Given a Shard ID select the RANGE Scheme that it defines.
    SELECT_RANGE_SPECIFICATION = (
        "SELECT shard_mapping_id, lower_bound, "
        "shard_id "
        "FROM shard_ranges "
        "WHERE shard_id = %s"
    )

    #Given a Shard Mapping ID select all the RANGE mappings that it
    #defines.
    LIST_RANGE_SPECIFICATION = (
        "SELECT "
        "shard_mapping_id, "
        "lower_bound, "
        "shard_id "
        "FROM shard_ranges "
        "WHERE shard_mapping_id = %s"
    )

    #Select the server corresponding to the RANGE to which a given key
    #belongs. The query either selects the least lower_bound that is larger
    #than a given key or selects the largest lower_bound and insert the key
    #in that shard.
    LOOKUP_KEY = (
        "SELECT "
        "sr.shard_mapping_id, "
        "sr.lower_bound, "
        "s.shard_id "
        "FROM "
        "shard_ranges AS sr, shards AS s "
        "WHERE %s >= sr.lower_bound "
        "AND sr.shard_mapping_id = %s "
        "AND s.shard_id = sr.shard_id "
        "ORDER BY sr.lower_bound DESC "
        "LIMIT 1"
    )

    #Select the UPPER BOUND for a given LOWER BOUND value.
    SELECT_UPPER_BOUND = (
        "SELECT lower_bound FROM "
        "shard_ranges JOIN shards USING (shard_id) "
        "WHERE lower_bound > %s AND shard_mapping_id = %s "
        "ORDER BY lower_bound ASC LIMIT 1"
    )

    #Update the Range for a particular shard. The updation needs to happen
    #for the upper bound and the lower bound simultaneously.
    UPDATE_RANGE = (
        "UPDATE shard_ranges SET lower_bound = %s "
        " WHERE shard_id = %s"
    )

    def __init__(self, shard_mapping_id, lower_bound, shard_id):
        """Initialize a given RANGE sharding mapping specification.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given RANGE sharding defn
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.
        """
        super(RangeShardingSpecification, self).__init__()
        self.__shard_mapping_id = shard_mapping_id
        self.__lower_bound = lower_bound
        self.__shard_id = shard_id

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
    def shard_id(self):
        """Return the Shard ID of the shard for this RANGE sharding
        definition
        """
        return self.__shard_id

    def remove(self, persister=None):
        """Remove the RANGE specification mapping represented by the current
        RANGE shard specification object.

        :param persister: Represents a valid handle to the state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.DELETE_RANGE_SPECIFICATION,
            {"params":(self.__shard_id,)})

    @staticmethod
    def add(shard_mapping_id, lower_bound, shard_id, persister=None):
        """Add the RANGE shard specification. This represents a single instance
        of a shard specification that maps a key RANGE to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given RANGE sharding defn
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.

        :return: A RangeShardSpecification object representing the current
                Range specification.
                None if the insert into the state store failed
        """
        persister.exec_stmt(
            RangeShardingSpecification.INSERT_RANGE_SPECIFICATION, {
                "params":(
                    shard_mapping_id,
                    lower_bound,
                    shard_id
                )
            }
        )
        return RangeShardingSpecification(
            shard_mapping_id,
            lower_bound,
            shard_id
        )

    @staticmethod
    def create(persister=None):
        """Create the schema to store the current RANGE sharding specification.

        :param persister: A valid handle to the state store.
        """

        persister.exec_stmt(
                    RangeShardingSpecification.CREATE_RANGE_SPECIFICATION)

    @staticmethod
    def list(shard_mapping_id, persister=None):
        """Return the RangeShardingSpecification objects corresponding to the
        given sharding scheme.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: A  list of RangeShardingSpecification objects which belong to
                to this shard mapping.
                None if the shard mapping is not found
        """

        cur = persister.exec_stmt(
                    RangeShardingSpecification.LIST_RANGE_SPECIFICATION,
                        {"raw" : False,
                        "fetch" : False,
                        "params" : (shard_mapping_id,)})
        rows = cur.fetchall()
        return [ RangeShardingSpecification(*row[0:5]) for row in rows ]

    @staticmethod
    def drop(persister=None):
        """Drop the Range shard specification schema.

        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(
                        RangeShardingSpecification.DROP_RANGE_SPECIFICATION)

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the sharding tables.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.ADD_FOREIGN_KEY_CONSTRAINT_SHARD_ID
            )

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the constraints on the sharding tables.

        :param persister: The DB server that can be used to access the
                  state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.DROP_FOREIGN_KEY_CONSTRAINT_SHARD_ID
            )

    @staticmethod
    def fetch(shard_id, persister=None):
        """Return the RangeShardingSpecification object corresponding to the
        given sharding ID.

        :param shard_id: The unique identification for a shard.
        :param persister: A valid handle to the state store.

        :return: A list that represents the information in the
                    RangeShardingSpecification.
                    An Empty list otherwise.
        """
        cur = persister.exec_stmt(
                    RangeShardingSpecification.SELECT_RANGE_SPECIFICATION,
                        {"raw" : False,
                        "fetch" : False,
                        "params" : (shard_id,)})
        row = cur.fetchone()
        if row is None:
            return None
        return RangeShardingSpecification(row[0], row[1],  row[2])

    @staticmethod
    def update_shard(shard_id, lower_bound, persister=None):
        """Update the range for a given shard_id.

        :param shard_id: The ID of the shard whose range needs to be updated.
        :param lower_bound: The new lower bound for the shard.
        :param persister: A valid handle to the state store.
        """
        persister.exec_stmt(
            RangeShardingSpecification.UPDATE_RANGE,
            {"params" : (lower_bound, shard_id)}
        )
    
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
        """
        cur = persister.exec_stmt(
                        RangeShardingSpecification.LOOKUP_KEY,
                        {"raw" : False,
                        "fetch" : False,
                        "params" : (key, shard_mapping_id)})

        row = cur.fetchone()

        if row is None:
            return None
        return RangeShardingSpecification(row[0], row[1],  row[2])

    @staticmethod
    def get_upper_bound(lower_bound, shard_mapping_id, persister=None):
        """Return the next value in range for a given lower_bound value.
        This basically helps to form a (lower_bound, upper_bound) pair
        that can be used during a prune.

        :param lower_bound: The lower_bound value whose next range needs to
                            be retrieved.
        :param shard_mapping_id: The shard_mapping_id whose shards should be
                                searched for the given lower_bound.
        :param persister: A valid handle to the state store.

        :return: The next value in the range for the given lower_bound.
        """
        #TODO: Even though a function like this seems practical, it
        #TODO: encourages a bad usage since there really are no upper
        #TODO: bounds any more, just lower bounds. It would probably
        #TODO: be better to re-write the prune method to be based on
        #TODO: just lower bounds.
        cur = persister.exec_stmt(
                        RangeShardingSpecification.SELECT_UPPER_BOUND,
                        {"raw" : False,
                        "fetch" : False,
                        "params" : (lower_bound, shard_mapping_id)})

        row = cur.fetchone()

        if row is None:
            return None

        return row[0]

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
        """

        shard_mapping = ShardMapping.fetch(table_name)
        if shard_mapping is None:
            raise _errors.ShardingError("Shard Mapping not found.")

        shard_mapping_id = shard_mapping.shard_mapping_id

        shards = RangeShardingSpecification.list(shard_mapping_id)
        if not shards:
            raise _errors.ShardingError("No shards associated with this"
                                                         " shard mapping ID.")

        for shard in shards:
            RangeShardingSpecification.prune_shard_id(shard.shard_id)

    #TODO: Narayanan: Explore if the errors below can be handled at the service
    #TODO: Narayanan: layer.
    @staticmethod
    def prune_shard_id(shard_id):
        """Remove the rows in the shard that do not match the metadata
        in the shard_range tables.

        :param shard_id: The ID of the shard that needs to be pruned.
        """
        range_sharding_spec = RangeShardingSpecification.fetch(shard_id)
        if range_sharding_spec is None:
            raise _errors.ShardingError("No shards associated with this"
                                                         " shard mapping ID.")

        shard = Shards.fetch(shard_id)

        upper_bound = RangeShardingSpecification.get_upper_bound(
                            range_sharding_spec.lower_bound,
                            range_sharding_spec.shard_mapping_id
                        )

        shard_mapping = ShardMapping.fetch_by_id(range_sharding_spec.shard_mapping_id)
        if shard_mapping is None:
            raise _errors.ShardingError("Shard Mapping not found.")

        table_name = shard_mapping.table_name

        if upper_bound is not None:
            #TODO: OR is notoriously bad for the optimizer. Better to turn it into
            #TODO: two separate queries handling all below and all above respectively.
            delete_query = ("DELETE FROM %s WHERE %s < %s OR %s >= %s") % (
                table_name,
                shard_mapping.column_name,
                range_sharding_spec.lower_bound,
                shard_mapping.column_name,
                upper_bound
            )
        else:
            delete_query = ("DELETE FROM %s WHERE %s < %s") % (
                table_name,
                shard_mapping.column_name,
                range_sharding_spec.lower_bound
            )

        shard = Shards.fetch(range_sharding_spec.shard_id)
        if shard is None:
            raise _errors.ShardingError(
                "Shard not found (%s)" %
                (range_sharding_spec.shard_id, )
            )

        group = Group.fetch(shard.group_id)
        if group is None:
            raise _errors.ShardingError(
                "Group not found (%s)" %
                (shard.group_id, )
            )

        master = MySQLServer.fetch(group.master)
        if master is None:
            raise _errors.ShardingError(
                "Group Master not found (%s)" %
                (str(group.master))
            )

        master.connect()

        master.exec_stmt(delete_query)

class HashShardingSpecification(RangeShardingSpecification):
    #Insert a HASH of keys and the server to which they belong.
    INSERT_HASH_SPECIFICATION = ("INSERT INTO shard_ranges"
        "(shard_mapping_id, lower_bound, shard_id) VALUES(%s, UNHEX(MD5(%s)), %s)")

    #Fetch the shard ID corresponding to the input key.
    LOOKUP_KEY = (
                "("
                "SELECT "
                "sr.shard_mapping_id, "
                "sr.lower_bound AS lower_bound, "
                "s.shard_id "
                "FROM shard_ranges AS sr, shards AS s "
                "WHERE UNHEX(MD5(%s)) >= sr.lower_bound "
                "AND sr.shard_mapping_id = %s "
                "AND s.shard_id = sr.shard_id "
                "ORDER BY sr.lower_bound DESC "
                "LIMIT 1"
                ") "
                "UNION ALL "
                "("
                "SELECT "
                "sr.shard_mapping_id, "
                "sr.lower_bound AS lower_bound, "
                "sr.shard_id "
                "FROM shard_ranges AS sr, shards AS s "
                "WHERE sr.shard_mapping_id = %s "
                "AND s.shard_id = sr.shard_id "
                "ORDER BY sr.lower_bound DESC "
                "LIMIT 1"
                ") "
                "ORDER BY lower_bound ASC "
                "LIMIT 1"
                )

    def __init__(self, shard_mapping_id, lower_bound, shard_id):
        """Initialize a given HASH sharding mapping specification.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param lower_bound: The lower bound of the given HASH sharding
                            definition.
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.
        """
        super(HashShardingSpecification, self).__init__(
            shard_mapping_id,
            lower_bound,
            shard_id
        )

    @staticmethod
    def add(shard_mapping_id, shard_id, persister=None):
        """Add the HASH shard specification. This represents a single instance
        of a shard specification that maps a key HASH to a server.

        :param shard_mapping_id: The unique identification for a shard mapping.
        :param shard_id: An unique identification, a logical representation
                        for a shard of a particular table.

        :return: A RangeShardSpecification object representing the current
                Range specification.
                None if the insert into the state store failed
        """
        shard = Shards.fetch(shard_id)
        persister.exec_stmt(
            HashShardingSpecification.INSERT_HASH_SPECIFICATION, {
                "params":(
                    shard_mapping_id,
                    shard.group_id,
                    shard_id
                )
            }
        )
        #TODO: Note that we do not return a HashShardingSpecification instance.
        #TODO: This behaviour is different from the RangeShardingSpecification.
        #TODO: What should be done in this case ?

    @staticmethod
    def lookup(key, shard_mapping_id, persister=None):
        """Return the Hash sharding specification in whose hashed key range
        the input key falls.

        :param key: The key which needs to be checked to see which range it
                    falls into
        :param shard_mapping_id: The unique identification for a shard mapping.
        :param persister: A valid handle to the state store.

        :return: The Hash Sharding Specification that contains the range in
                which the key belongs.
        """
        cur = persister.exec_stmt(
                    HashShardingSpecification.LOOKUP_KEY, {
                        "raw" : False,
                        "fetch" : False,
                        "params" : (
                            key,
                            shard_mapping_id,
                            shard_mapping_id
                        )
                    }
                )

        row = cur.fetchone()
        if row is None:
            return None
        return RangeShardingSpecification(row[0], row[1],  row[2])

    @staticmethod
    def create(persister=None):
        """We use the relations used to store the RANGE sharding data. Hence
        this method is a dummy here. We need a dummy implementation since
        the framework invokes this method to create the schemas necessary for
        storing relevant data, and we have no schemas.

        :param persister: A valid handle to the state store.
        """
        pass

    @staticmethod
    def drop(persister=None):
        """Drop the Range shard specification schema. We use the relations used
        to store the RANGE sharding data. Hence this method is a dummy here.
        We need a dummy implementation since the framework invokes this method
        to drop the schemas.

        :param persister: A valid handle to the state store.
        """
        pass

    @staticmethod
    def add_constraints(persister=None):
        """Add the constraints on the sharding tables. We use a dummy implementation
        here.

        :param persister: The DB server that can be used to access the
                          state store.
        """
        pass

    @staticmethod
    def drop_constraints(persister=None):
        """Drop the constraints on the sharding tables. We use a dummy
        implementation here.

        :param persister: The DB server that can be used to access the
                  state store.
        """
        pass

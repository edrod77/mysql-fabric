#
# Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#

from __future__ import print_function

import getpass
import hashlib
import logging
import re

from mysql.fabric import (
    config as _config,
    errors as _errors,
    persistence as _persistence,
    executor as _executor,
    command as _command,
)

_LOGGER = logging.getLogger(__name__)

FABRIC_REALM_XMLRPC = 'MySQL Fabric'

FABRIC_DEFAULT_PROTOCOL = 'xmlrpc'
FABRIC_PROTOCOL_DEFAULTS = {
    'protocol.xmlrpc': {
        'realm': FABRIC_REALM_XMLRPC,
    },
}

# This sequence is used when creating an dropping tables
_SQL_TABLES = ('users', 'roles', 'permissions', 'role_permissions',
               'user_roles')

_SQL_CREATE = dict()
_SQL_CREATE['users'] = """
CREATE TABLE `users` (
  `user_id` int unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(100) NOT NULL,
  `protocol` varchar(200) NOT NULL DEFAULT 'xmlrpc',
  `password` varchar(128) DEFAULT NULL,
  PRIMARY KEY (user_id),
  UNIQUE KEY (`username`, `protocol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

_SQL_CREATE['roles'] = """
CREATE TABLE `roles` (
  `role_id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(80) NOT NULL,
  `description` varchar(1000),
  PRIMARY KEY (role_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

_SQL_CREATE['permissions'] = """
CREATE TABLE `permissions` (
  `permission_id` int unsigned NOT NULL AUTO_INCREMENT,
  `subsystem` varchar(60) NOT NULL,
  `component` varchar(60) DEFAULT NULL,
  `function` varchar(60) DEFAULT NULL,
  `description` varchar(1000),
  PRIMARY KEY (permission_id),
  UNIQUE INDEX (`subsystem`, `component`, `function`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

_SQL_CREATE['role_permissions'] = """
CREATE TABLE `role_permissions` (
  `role_id` int unsigned NOT NULL,
  `permission_id` int unsigned NOT NULL,
  PRIMARY KEY (`role_id`, `permission_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

_SQL_CREATE['user_roles'] = """
CREATE TABLE `user_roles` (
  `user_id` int unsigned NOT NULL,
  `role_id` int unsigned NOT NULL,
  PRIMARY KEY (`user_id`, `role_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

_SQL_CONSTRAINTS = [
    "ALTER TABLE user_roles ADD CONSTRAINT fk_user_roles_user_id "
    "FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE, "
    "ADD CONSTRAINT fk_user_roles_role_id FOREIGN KEY (role_id) "
    "REFERENCES roles (role_id) ON DELETE CASCADE",

    "ALTER TABLE role_permissions ADD CONSTRAINT fk_role_permissions_role_id "
    "FOREIGN KEY (role_id) REFERENCES roles (role_id), ADD CONSTRAINT "
    "fk_role_permissions_permission_id FOREIGN KEY (permission_id) "
    "REFERENCES permissions (permission_id)",
]

_USERS = [
    # user_id, username, password, protocol
    (1, 'admin', None, 'xmlrpc'),
]

_PERMISSIONS = [
    (1, 'core', None, None, 'Full access to all core Fabric functionality'),
    (2, 'core', 'dump', None, 'Access to dump commands'),
    (3, 'core', 'user', None, 'User administration'),
    (4, 'core', 'role', None, 'Role administration'),
    (5, 'core', 'threat', None, 'Reporting to Fabric'),
]

_ROLES = [
    (1, 'superadmin', 'Role for Administrative users'),
    (2, 'useradmin', 'Role for users dealing with user administration'),
    (3, 'connector', 'Role for MySQL Connectors'),
]

_ROLE_PERMISSIONS = {
    1: (1,),  # superadmin permissions
    3: (2, 5),  # connector permissions
    2: (3, 4),  # useradmin permissions
}

_USER_ROLES = [
    (1, 1),  # admin in superadmin role
]

_SQL_FETCH_USER = """
SELECT username, protocol, password
FROM users WHERE username = %s AND protocol = %s
"""

_SQL_FETCH_USER_CHECK_PASSWORD = """
SELECT username, protocol, password
FROM users WHERE username = %s AND protocol = %s AND password = %s
"""

_SQL_FETCH_USER_PERMISSIONS = """
SELECT p.subsystem, p.component, p.function
FROM users AS u LEFT JOIN user_roles AS ur USING (user_id)
    LEFT JOIN role_permissions AS rp USING (role_id)
    LEFT JOIN permissions AS p USING (permission_id)
WHERE
    u.username = %s AND protocol = %s
"""


class FabricCredential(_persistence.Persistable):
    """Class defining a user connecting to a Fabric instance
    """

    def __init__(self, username, protocol, password_hash):
        self._username = username
        self._protocol = protocol
        self._password_hash = password_hash
        self._permissions = FabricCredential.fetch_permissions(username,
                                                               protocol)

    @property
    def username(self):
        return self._username

    @property
    def protocol(self):
        return self._protocol

    @property
    def password_hash(self):
        return self._password_hash

    @staticmethod
    def add_user(username, password, protocol, user_id=None,
                 config=None, persister=None):
        stmt = (
            "INSERT INTO users (user_id, username, password, protocol) "
            "VALUES (%(user_id)s, %(username)s, %(password)s, %(protocol)s)"
        )

        hashed_password = _hash_password(username, password, protocol, config)

        options = {
            "raw": False,
            "fetch": False,
            "params": {
                'user_id': user_id,
                'username': username,
                'protocol': protocol,
                'password': hashed_password,
            },
        }
        cur = persister.exec_stmt(stmt, options)
        return cur.lastrowid

    @staticmethod
    def add_permission(subsystem, component=None, function=None,
                       description=None, permission_id=None, persister=None):
        stmt = (
            "INSERT INTO permissions (permission_id, subsystem, "
            "component, function, description) "
            "VALUES (%(permission_id)s, %(subsystem)s, %(component)s, "
            "%(function)s, %(description)s)"
        )

        options = {
            "raw": False,
            "fetch": False,
            "params": {
                'permission_id': permission_id,
                'subsystem': subsystem,
                'function': function,
                'component': component,
                'description': description,
            },
        }
        persister.exec_stmt(stmt, options)

    @staticmethod
    def add_role(name, description, role_id=None, persister=None):
        stmt = (
            "INSERT INTO roles (role_id, name, description) "
            "VALUES (%(role_id)s, %(name)s, %(description)s)"
        )

        options = {
            "raw": False,
            "fetch": False,
            "params": {
                'role_id': role_id,
                'name': name,
                'description': description,
            },
        }
        persister.exec_stmt(stmt, options)

    @staticmethod
    def add_role_permission(role_id, permission_id, persister=None):
        stmt = (
            "INSERT INTO role_permissions (role_id, permission_id) "
            "VALUES (%(role_id)s, %(permission_id)s)"
        )

        options = {
            "raw": False,
            "fetch": False,
            "params": {
                'role_id': role_id,
                'permission_id': permission_id,
            },
        }
        persister.exec_stmt(stmt, options)

    @staticmethod
    def add_user_role(user_id, role_id, persister=None):
        stmt = (
            "INSERT INTO user_roles (user_id, role_id) "
            "VALUES (%(user_id)s, %(role_id)s)"
        )

        options = {
            "raw": False,
            "fetch": False,
            "params": {
                'role_id': role_id,
                'user_id': user_id,
            },
        }
        persister.exec_stmt(stmt, options)

    @staticmethod
    def delete_user(user_id=None, username=None, protocol=None, persister=None):
        """Delete a Fabric user"""
        if bool(user_id) != bool(username):
            raise AttributeError("Either user_id or username can be given, "
                                 "not both")

        if username and not protocol:
            raise AttributeError(
                "protocol is required when username is provided")

        options = {
            "raw": False,
            "fetch": False,
            "params": {},
        }
        if user_id:
            options['params']['user_id'] = user_id
            sql = "DELETE FROM users WHERE user_id = %(user_id)s"
        else:
            options['params']['username'] = username
            options['params']['protocol'] = protocol
            sql = ("DELETE FROM users WHERE username = %(username)s "
                   "AND protocol = %(protocol)s")

        persister.exec_stmt(sql, options)

    @staticmethod
    def create(persister=None, config=None):
        """Create table for FabricUser

        :param persister: A valid handle to the state store.
        """
        for table_name in _SQL_TABLES:
            persister.exec_stmt(_SQL_CREATE[table_name])

        for statement in _SQL_CONSTRAINTS:
            persister.exec_stmt(statement)

        for user_id, username, password, protocol in _USERS:
            FabricCredential.add_user(username, password, protocol,
                                      user_id=user_id,
                                      persister=persister,
                                      config=config)

        for permission_id, subsystem, component, function, description in \
                _PERMISSIONS:
            FabricCredential.add_permission(
                subsystem, component, function, description,
                permission_id=permission_id, persister=persister)

        for role_id, name, description in _ROLES:
            FabricCredential.add_role(name, description, role_id=role_id,
                                      persister=persister)

        for role_id, permissions in _ROLE_PERMISSIONS.items():
            for permission_id in permissions:
                FabricCredential.add_role_permission(
                    role_id, permission_id, persister=persister)

        for user_id, role_id in _USER_ROLES:
            FabricCredential.add_user_role(user_id, role_id,
                                           persister=persister)

    @staticmethod
    def drop(persister=None):
        """Drop tables for FabricCredential

        :param persister: A valid handle to the state store.
        """
        for table_name in reversed(_SQL_TABLES):
            drop_table = "DROP TABLE IF EXISTS {0}".format(table_name)
            persister.exec_stmt(drop_table)

    @staticmethod
    def fetch_user(username, protocol, config=None, password=None, realm=None,
                   persister=None):
        """Fetch the information about a user using particular protocol

        :param username: Username being queried.
        :param protocol: Protocol with which the user connect.
        :param password: Password of the user.
        :param persister: A valid handle to the state store.
        :return: Returns user information including password hash.
        """
        if not protocol:
            protocol = FABRIC_DEFAULT_PROTOCOL

        options = {
            "raw": False,
            "fetch": False,
            "params": [username, protocol],
            "columns": True,
        }

        if password:
            sql = _SQL_FETCH_USER_CHECK_PASSWORD
            options['params'].append(_hash_password(username, password,
                                                    protocol, config, realm))
        else:
            sql = _SQL_FETCH_USER

        cur = persister.exec_stmt(sql, options)
        row = cur.fetchone()
        if row:
            return FabricCredential(username=row.username,
                                    protocol=row.protocol,
                                    password_hash=row.password)

        return None

    @staticmethod
    def fetch_permissions(username, protocol, persister=None):
        """Fetch permissions for given user and protocol

        :param username: Username being queried.
        :param protocol: Protocol in which the user lives
        :param persister: A valid handle to the state store.
        :return: Returns user permissions as a list of tuples
        """
        if not protocol:
            protocol = FABRIC_DEFAULT_PROTOCOL

        options = {
            "raw": False,
            "fetch": False,
            "params": (username, protocol),
        }

        cur = persister.exec_stmt(_SQL_FETCH_USER_PERMISSIONS, options)
        rows = cur.fetchall()
        if rows:
            return rows

        return None

    def has_permission(self, subsystem, component=None, function=None):
        """Check if user has permission"""
        needles = [
            (subsystem, None, None),  # full access to subsystem
            (subsystem, component, None),  # access to all component's functions
            (subsystem, component, function)  # access to function
        ]

        for needle in needles:
            if needle in self._permissions:
                return True

        return False


def _hash_password(username, password, protocol, config, realm=None):
    """Hash password base on protocol

    :raises errors.CredentialError: if protocol is not implemented
    """
    if not password:
        return
    if not realm and config:
        section = 'protocol.' + protocol
        realm = config.get(section, 'realm',
                           vars=FABRIC_PROTOCOL_DEFAULTS[section])

    if protocol == 'xmlrpc':
        return hashlib.md5('{user}:{realm}:{secret}'.format(
            user=username, realm=FABRIC_REALM_XMLRPC,
            secret=password)).hexdigest()

    elif protocol == 'console':
        return hashlib.sha256(username + password).hexdigest()

    raise _errors.CredentialError(
        "Password hasing for protocol '{0}' is not implemented.".format(
            protocol
        ))


def get_user(username, protocol, persister=None):
    """Get user information from data store

    :param username: User to query for.
    :param protocol: Protocol used to connect.
    :param persister: A valid handle to the state store.
    :return: namedtuple with password, username, protocol and user_id
    :rtype: namedtuple
    """
    if not persister:
        persister = _persistence.current_persister()

    options = {
        "raw": False,
        "fetch": False,
        "params": (username, protocol),
        "columns": True,
    }

    cur = persister.exec_stmt(
        "SELECT password, username, protocol, user_id "
        "FROM users WHERE username = %s "
        "AND protocol = %s", options)
    row = cur.fetchone()
    if row:
        return row
    return None


def _get_password(prompt=None):
    """Get password from using STDIN

    :param prompt: String describing what input is required.
    :returns: Password as string.
    :rtype: str
    :raises CredentialError: if passwords did not match
    """
    if not prompt:
        prompt = 'Password: '
    password = getpass.getpass(prompt).strip()
    repeat = getpass.getpass('Repeat Password: ').strip()
    if password != repeat:
        raise _errors.CredentialError("Passwords did not match!")
    return password


def check_initial_setup(config, persister):
    """Check if admin user has password and if not sets it

    :param persister: A valid handle to the state store.
    """
    protocols = []
    for key in FABRIC_PROTOCOL_DEFAULTS.keys():
        if key.startswith('protocol.'):
            tmp = key.split('.', 2)[1]
            if tmp not in protocols:
                user = get_user('admin', tmp, persister)
                if not user.password:
                    protocols.append(tmp)

    if not protocols:
        # Passwords are set
        return

    print("Finishing initial setup")
    print("=======================")
    print("Password for admin user is not yet set.")
    password = None

    while True:
        password = _get_password("Password for {user}: ".format(user='admin'))
        if password:
            break

    if password:
        options = {
            "raw": False,
            "fetch": False,
            "params": [],
            "columns": True,
        }
        update = ("UPDATE users SET password = %s WHERE username = %s "
                  "AND protocol = %s")

        for protocol in protocols:
            hashed = _hash_password('admin', password, protocol, config)
            options['params'] = (hashed, 'admin', protocol)
            persister.exec_stmt(update, options)

    else:
        # Making sure password is set and there is an error message
        raise _errors.CredentialError(
            "Password for admin can not be empty.")

    persister.commit()


def _configure_connections(config):
    """Configure information on database connection and remote servers.

    :param config: Configuration read from configuration file.
    """
    # Configure the number of concurrent executors.
    executor = _executor.Executor()
    executor.set_number_executors(1)

    # Fetch options to configure the state store.
    address = config.get('storage', 'address')

    try:
        host, port = address.split(':')
        port = int(port)
    except ValueError:
        host = address
        port = 3306

    user = config.get('storage', 'user')
    database = config.get('storage', 'database')

    try:
        password = config.get('storage', 'password')
    except _config.NoOptionError:
        password = _get_password("Storage password: ")

    try:
        connection_timeout = config.get("storage", "connection_timeout")
        connection_timeout = float(connection_timeout)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        connection_timeout = None

    try:
        connection_attempts = config.get("storage", "connection_attempts")
        connection_attempts = int(connection_attempts)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        connection_attempts = None

    try:
        connection_delay = config.get("storage", "connection_delay")
        connection_delay = int(connection_delay)
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        connection_delay = None

    try:
        auth_plugin = config.get("storage", "auth_plugin")
    except (_config.NoOptionError, _config.NoSectionError, ValueError):
        auth_plugin = None

    # Define state store configuration.
    _persistence.init(
        host=host, port=port, user=user, password=password, database=database,
        connection_timeout=connection_timeout,
        connection_attempts=connection_attempts,
        connection_delay=connection_delay,
        auth_plugin=auth_plugin
    )


def confirm(message, default='y'):
    """Prompt confirming using Yes or No

    :param message: Message to be shown.
    :param default: The default answer, either 'y' or 'n'.
    :returns: True if answer was 'y', False if 'n'.
    :rtype: bool
    :raises ValueError: if invalid choice has been given.
    """
    if default:
        default = default[0].lower()

    yes = 'Y' if default == 'y' else 'y'
    no = 'N' if default == 'n' else 'n'

    msg = message + " [{0}/{1}] ".format(yes, no)
    try:
        choice = raw_input(msg)[0].lower()
    except IndexError:
        choice = default

    if choice == 'y':
        return True
    elif choice == 'n':
        return False

    raise ValueError("Invalid choice")


def _role_listing(selected=None, marker=None, persister=None):
    """Display list of roles

    :param selected: Sequence of selected roles.
    :param marker: Marker used for showing selected roles.

    The selected argument should be a list of role IDs. The marker will be
    shown in front of the role ID when it is in the selected sequence.
    """
    if not persister:
        persister = _persistence.current_persister()

    if not selected:
        selected = []

    if marker:
        marker = marker[0]
    else:
        marker = 'X'

    options = {
        "raw": False,
        "fetch": False,
        "columns": True,
    }

    roleperms = (
        "SELECT r.role_id, r.name, r.description AS role_desc, p.permission_id, "
        "p.description AS perm_desc, p.subsystem, "
        "p.component, p.function "
        "FROM roles AS r LEFT JOIN role_permissions USING (role_id) "
        "LEFT JOIN permissions AS p USING (permission_id) ORDER BY r.role_id"
    )
    cur = persister.exec_stmt(roleperms, options)
    roles = {}
    max_name_len = 0
    max_rowid = 0
    for row in cur:
        if len(row.name) > max_name_len:
            max_name_len = len(row.name)
        if row.role_id > max_rowid:
            max_rowid = row.role_id

        if row.role_id not in roles:
            roles[row.role_id] = [row.name, row.role_desc, [row.perm_desc]]
        else:
            roles[row.role_id][2].append(row.perm_desc)

    # Minimum sizes
    max_rowid_len = max(2, len(str(max_rowid)))
    max_name_len = max(9, max_name_len)

    sel = ' '

    role_fmt = ("{{sel}} {{role_id:{idlen}}}  {{name:{namelen}}}"
                "  {{desc}}".format(idlen=max_rowid_len, namelen=max_name_len))

    label_desc = "Description and Permissions"
    header = role_fmt.format(role_id="ID", name="Role Name", desc=label_desc,
                             sel=sel)
    print(header)
    print(role_fmt.format(role_id='-' * max_rowid_len, name='-' * max_name_len,
                          desc='-' * len(label_desc), sel=sel))

    fmt_perm = '{0}+ {{perm}}'.format(
        (2 + max_rowid_len + 2 + max_name_len + 2) * ' ')

    for role_id in sorted(roles.keys()):
        if role_id in selected:
            sel = marker
        else:
            sel = ' '
        name, role_desc, permissions = roles[role_id]
        print(role_fmt.format(role_id=role_id, name=name, desc=role_desc,
                              sel=sel))
        for perm in permissions:
            print(fmt_perm.format(perm=perm))


def _role_selection(message=None, persister=None):
    """Offers user to select roles on the console

    :param persister: A valid handle to the state store.
    :param message: Message shown just before prompt.
    :return: List of role IDs or role names.
    :rtype: list
    :raises errors.CredentialError: if invalid role was given
    """
    if not persister:
        persister = _persistence.current_persister()

    if not message:
        message = "\nEnter comma separated list of role IDs or names: "

    choice = raw_input(message)
    if not choice.strip():
        return []

    choices = choice.split(',')

    options = {
        "raw": False,
        "fetch": False,
        "columns": True,
    }
    cur = persister.exec_stmt("SELECT role_id, name FROM roles", options)

    valid_role_ids = []
    roles = {}
    for row in cur:
        roles[row.name] = row.role_id
        if str(row.role_id) not in valid_role_ids:
            valid_role_ids.extend([str(row.role_id), row.name])

    try:
        if not all(rid.strip() in valid_role_ids for rid in choices):
            raise ValueError
    except ValueError:
        raise _errors.CredentialError("Invalid role in choice.")

    # Only return role IDs
    result = []
    for rid in choices:
        try:
            result.append(int(rid))
        except ValueError:
            # We got name
            result.append(roles[rid.strip()])

    return result

def validate_username(username, allow_empty=False):
    """Validates a username

    :param username: The username we need to check.
    :param allow_empty: Whether to allow empty username.
    :returns: Stripped username or None when username was empty
    :rtype: str
    :raises ValueError: if username is not acceptable.
    """
    username = username.strip()
    if not username and allow_empty:
        return None
    valid = r"[a-zA-Z0-9_@.-]"
    match = re.match(r"^" + valid + "+$", username)
    if not match:
        raise _errors.CredentialError("Invalid username (was not {0})".format(
            valid
        ))

    return username


def validate_protocol(protocol, allow_empty=False):
    """Validates a protocol

    :param protocol: The protocol we need to check.
    :param allow_empty: Whether to allow empty protocol.
    :returns: Stripped protocol or None when protocol was empty
    :rtype: str
    :raises ValueError: if protocol is not acceptable.
    """
    protocol = protocol.strip()
    if not protocol and allow_empty:
        return None
    valid = r"[a-zA-Z0-9_@.-]"
    match = re.match(r"^" + valid + "+$", protocol)
    if not match:
        raise _errors.CredentialError("Invalid protocol (was not {0})".format(
            valid
        ))

    return protocol


class UserCommand(_command.Command):

    """Base class for all user commands"""

    group_name = 'user'
    command_name = ''
    description = ''
    persister = None

    def dispatch(self, *args):
        """Setup Fabric Storage System.
        """
        # Configure connections.
        _configure_connections(self.config)
        _persistence.init_thread()
        self.persister = _persistence.current_persister()
        self.execute(*args)

    def execute(self, *args):
        raise NotImplemented

    def _ask_credentials(self, username, ask_password=True, title=None):
        """Ask for credentials

        :param username: Username will not be asked if given.
        :param ask_password: Whether to ask for password or not.
        :param title: The title to show.
        :return: Tuple containing username, password and protocol
        :rtype: tuple
        """
        if not title:
            title = self.description

        print(title)
        print("=" * len(title))

        if username:
            print("Username: {0}".format(username))
        else:
            username = validate_username(raw_input("Enter username: "))

        if self.options.protocol:
            protocol = self.options.protocol
            print("Protocol: {0}".format(protocol))
        else:
            prompt = "Protocol (default {0}): ".format(FABRIC_DEFAULT_PROTOCOL)
            protocol = validate_protocol(raw_input(prompt),
                                              allow_empty=True)
            if not protocol:
                protocol = FABRIC_DEFAULT_PROTOCOL

        if ask_password:
            password = _get_password()
        else:
            password = None

        return username, password, protocol


class UserAdd(UserCommand):

    """Add a new Fabric user"""

    command_name = 'add'
    description = 'Add a new Fabric user'

    def execute(self, username, protocol=None, role=None):
        """Add a new Fabric user"""
        username, password, protocol = self._ask_credentials(
            username, ask_password=False)

        # Check if user exists
        if get_user(username, protocol, persister=self.persister):
            raise _errors.CredentialError(
                "User {user}/{protocol} already exists".format(
                    user=username, protocol=protocol))

        password = _get_password('Password:')

        if not password:
            raise _errors.CredentialError("Can not set empty password")

        try:
            self.persister.begin()
            user_id = FabricCredential.add_user(username, password, protocol)
            print("\nSelect role(s) for new user")
            _role_listing()
            roles = _role_selection(persister=self.persister)
            if not roles:
                print("You can always assign roles later using the "
                      "'user addrole' command")
            else:
                for role_id in roles:
                    FabricCredential.add_user_role(user_id, int(role_id))
        except Exception:
            # Whatever happens, we rollback and re-raise
            self.persister.rollback()
            print("Adding user cancelled.")
            raise

        self.persister.commit()
        print("Fabric user added.")


class UserDelete(UserCommand):

    """Delete a Fabric user"""

    command_name = 'delete'
    description = 'Delete a Fabric user'

    def execute(self, username, protocol=None, force=False):
        """Delete a Fabric user
        """
        username, password, protocol = self._ask_credentials(
            username, ask_password=False)

        # Check if user exists
        if not get_user(username, protocol, persister=self.persister):
            raise _errors.CredentialError(
                "No user {user}/{protocol}".format(
                    user=username, protocol=protocol))

        if not self.options.force:
            result = confirm(
                "Really remove user {user}/{protocol}?".format(
                    user=username, protocol=protocol), default='n')
            if result is not True:
                return

        try:
            self.persister.begin()
            FabricCredential.delete_user(username=username, protocol=protocol)
        except Exception:
            # We rollback and re-raise
            self.persister.rollback()
            print("Removing user cancelled.")
            raise

        self.persister.commit()
        print("Fabric user deleted.")


class UserPassword(UserCommand):

    """Change password of a Fabric user"""

    command_name = 'password'
    description = 'Change password a Fabric user'

    def execute(self, username, protocol=None):
        """Change password a Fabric user
        """
        username, password, protocol = self._ask_credentials(
            username, ask_password=False)

        # Check if user exists
        if not get_user(username, protocol):
            raise _errors.CredentialError(
                "No user {user}/{protocol}".format(
                    user=username, protocol=protocol))

        password = _get_password('New password:')

        if password:
            try:
                self.persister.begin()
                options = {
                    "raw": False,
                    "fetch": False,
                    "params": (),
                    "columns": True,
                }
                update = ("UPDATE users SET password = %s WHERE username = %s"
                          " AND protocol = %s")
                hashed = _hash_password(username, password, protocol,
                                        self.config)
                options['params'] = (hashed, username, protocol)
                self.persister.exec_stmt(update, options)
            except Exception:
                # We rollback and re-raise
                print("Changing password cancelled.")
                raise
            self.persister.commit()
            print("Password changed.")
        else:
            raise _errors.CredentialError("Can not set empty password")


class UserRoles(UserCommand):

    """Change roles for a Fabric user"""

    command_name = 'roles'
    description = 'Change roles for a Fabric user'

    def execute(self, username, protocol=None):
        """Change roles for a Fabric user
        """
        username, password, protocol = self._ask_credentials(
            username, ask_password=True)

        # Check if user exists
        user = get_user(username, protocol)
        if not user:
            raise _errors.CredentialError(
                "No user {user}/{protocol}".format(user=username,
                                                   protocol=protocol))

        self.persister.begin()
        options = {
            "raw": False,
            "fetch": False,
            "params": (user.user_id,),
            "columns": True,
        }
        cur = self.persister.exec_stmt(
            "SELECT role_id FROM user_roles WHERE user_id = %s",
            options)
        current_roles = [row.role_id for row in cur]

        try:
            self.persister.begin()
            print("\nSelect new role(s) for user, replacing current roles.")
            print("Current roles are marke with an X.")
            _role_listing(selected=current_roles)
            roles = _role_selection(persister=self.persister)

            if not roles:
                confirm_text = "Remove all roles of user {username}?"
                exit_text = "Fabric user roles removed."
                default = 'n'
            else:
                confirm_text = "Replace roles of user {username}?"
                exit_text = "Fabric user roles updated."
                default = 'y'

            if confirm(confirm_text.format(username=user.username),
                       default=default):
                self.persister.exec_stmt(
                    "DELETE FROM user_roles WHERE user_id = %s", options)
                for role_id in roles:
                    FabricCredential.add_user_role(user.user_id, int(role_id))
            else:
                exit_text = "Changing roles for user cancelled."

        except Exception:
            # Whatever happens, we rollback and re-raise
            self.persister.rollback()
            print("Changing roles for user cancelled.")
            raise
        else:
            self.persister.commit()
            print(exit_text)


class RoleList(UserCommand):

    """List roles and their permissions"""

    group_name = 'role'
    command_name = 'list'
    description = 'List roles and their permissions'

    def execute(self):
        _role_listing(persister=self.persister)


def check_credentials(group, command, config=None,
                      username=None, password=None, protocol=None):
    """Check credentials using configuration

    :raises errors.CredentialError: if login failed, or if user has no
                                    permission
    """
    _configure_connections(config)
    _persistence.init_thread()

    if not protocol:
        protocol = FABRIC_DEFAULT_PROTOCOL

    section = 'protocol.' + protocol

    if config:
        username = config.get(section, 'username')
        password = config.get(section, 'password')
        realm = config.get(section, 'realm', vars=FABRIC_PROTOCOL_DEFAULTS)

    user = FabricCredential.fetch_user(username, protocol, config,
                                       password=password, realm=realm)

    if not user:
        import traceback
        traceback.print_exc()
        raise _errors.CredentialError("Login failed")
    if not user.has_permission('core', group, command):
        raise _errors.CredentialError("No permission")


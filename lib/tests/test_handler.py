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
"""Unit tests for testing MySQLHandler.
"""
import unittest
import uuid as _uuid
import tests.utils
import logging

import mysql.fabric.utils as _utils
import mysql.fabric.persistence as _persistence

from mysql.fabric.handler import (
    MySQLHandler,
)

_LOGGER = logging.getLogger(__name__)

class TestMySQLHandler(unittest.TestCase):
    """Unit test for testing MySQLHandler.
    """
    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()

    def test_node_view(self):
        """Test basic properties/methods in the MySQLHandler.
        """
        # Retrieve information on Fabric node. Note though
        # that there is no specific view to retrieve such
        # information.
        node_id_1 = _uuid.uuid4()
        node_startup_1 = _utils.get_time()
        _LOGGER.debug("Fabric Node started.",
            extra={
                'subject' : str(node_id_1),
                'category' : MySQLHandler.NODE,
                'type' : MySQLHandler.START,
                'reported' : node_startup_1,
            }
        )
        node_stop_1 = _utils.get_time()
        _LOGGER.debug("Fabric Node started.",
            extra={
                'subject' : str(node_id_1),
                'category' : MySQLHandler.NODE,
                'type' : MySQLHandler.STOP,
                'reported' : node_stop_1,
            }
        )
        node_id_2 = _uuid.uuid4()
        node_startup_2 = _utils.get_time()
        _LOGGER.debug("Fabric Node started.",
            extra={
                'subject' : str(node_id_2),
                'category' : MySQLHandler.NODE,
                'type' : MySQLHandler.START,
                'reported' : node_startup_2,
            }
        )
        node_view = ("SELECT subject as node_id, "
            "TIMEDIFF(UTC_TIMESTAMP(), reported) as node_uptime, "
            "reported as node_startup FROM log WHERE category = %s "
            "and type = %s ORDER BY node_id, node_startup"
        )
        persister = _persistence.current_persister()
        res = persister.exec_stmt(
            node_view, {
                "params" : (
                    MySQLHandler.idx_category(MySQLHandler.NODE),
                    MySQLHandler.idx_type(MySQLHandler.START)
                )
            }
        )
        self.assertEqual(len(res), 2)

    def test_group_view(self):
        # Try to retrieve non-existent group.
        res = MySQLHandler.group_view("non-existent")
        self.assertEqual(len(res), 0)

        # Retrieve information on a group and check demote and promote.
        _LOGGER.debug("Master is being promoted.",
            extra={
                'subject' : 'group_id_1',
                'category' : MySQLHandler.GROUP,
                'type' : MySQLHandler.PROMOTE,
                'reporter' : 'test_handler'
            }
        )
        _LOGGER.debug("Master is being demoted.",
            extra={
                'subject' : 'group_id_1',
                'category' : MySQLHandler.GROUP,
                'type' : MySQLHandler.DEMOTE,
                'reporter' : 'test_handler'
            }
        )
        _LOGGER.debug("Master is being promoted.",
            extra={
                'subject' : 'group_id_1',
                'category' : MySQLHandler.GROUP,
                'type' : MySQLHandler.PROMOTE,
                'reporter' : 'test_handler'
           }
        )

        res = MySQLHandler.group_view("group_id_1")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][1], 2)
        self.assertEqual(res[0][2], 1)

        # Retrieve information on groups with similar name patterns.
        _LOGGER.debug("Master is being promoted.",
            extra={
                'subject' : 'group_id_2',
                'category' : MySQLHandler.GROUP,
                'type' : MySQLHandler.PROMOTE,
                'reporter' : 'test_handler'
            }
        )

        res = MySQLHandler.group_view("group_id_2")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][1], 1)
        self.assertEqual(res[0][2], 0)

        res = MySQLHandler.group_view("group_id")
        self.assertEqual(len(res), 2)

        # Retrieve information on groups with different name patterns,
        # i.e. all groups.
        _LOGGER.debug("Master is being promoted.",
            extra={
                'subject' : 'abc_group',
                'category' : MySQLHandler.GROUP,
                'type' : MySQLHandler.PROMOTE,
                'reporter' : 'test_handler'
            }
        )

        res = MySQLHandler.group_view()
        self.assertEqual(len(res), 3)

    def test_procedure_view(self):
        # Try to retrieve non-existent procedure.
        res = MySQLHandler.procedure_view("non-existent")
        self.assertEqual(len(res), 0)

        # Retrieve procedures and check stop and abort.
        for i in range(5):
            _LOGGER.debug("Report Message.",
                extra={
                    'subject' : 'proc_id_1',
                    'category' : MySQLHandler.PROCEDURE,
                    'type' : MySQLHandler.START,
                    'reporter' : 'test_handler'
                }
            )
            _LOGGER.debug("Report Message.",
                extra={
                    'subject' : 'proc_id_1',
                    'category' : MySQLHandler.PROCEDURE,
                    'type' : MySQLHandler.STOP,
                    'reporter' : 'test_handler'
                }
            )
        _LOGGER.debug("Report Message.",
            extra={
                'subject' : 'proc_id_1',
                'category' : MySQLHandler.PROCEDURE,
                'type' : MySQLHandler.START,
                'reporter' : 'test_handler'
            }
        )
        _LOGGER.debug("Report Message.",
            extra={
                'subject' : 'proc_id_1',
                'category' : MySQLHandler.PROCEDURE,
                'type' : MySQLHandler.ABORT,
                'reporter' : 'test_handler'
            }
        )
        res = MySQLHandler.procedure_view("proc_id_1")
        self.assertEqual(res[0][0], "proc_id_1")
        self.assertEqual(res[0][1], 6)
        self.assertEqual(res[0][2], 1)

        # Retrieve procedures with similar name patterns.
        _LOGGER.debug("Report Message.",
            extra={
                'subject' : 'proc_id',
                'category' : MySQLHandler.PROCEDURE,
                'type' : MySQLHandler.START,
                'reporter' : 'test_handler'
            }
        )
        _LOGGER.debug("Report Message.",
            extra={
                'subject' : 'proc_id',
                'category' : MySQLHandler.PROCEDURE,
                'type' : MySQLHandler.STOP,
                'reporter' : 'test_handler'
            }
        )
        res = MySQLHandler.procedure_view("proc_id")
        self.assertEqual(len(res), 2)

        # Retrieve procedures with different name patterns, i.e. all procedures.
        _LOGGER.debug("Report Message.",
            extra={
                'subject' : 'other',
                'category' : MySQLHandler.PROCEDURE,
                'type' : MySQLHandler.START,
                'reporter' : 'test_handler'
            }
        )
        _LOGGER.debug("Report Message",
            extra={
                'subject' : 'other',
                'category' : MySQLHandler.PROCEDURE,
                'type' : MySQLHandler.STOP,
                'reporter' : 'test_handler'
            }
        )
        res = MySQLHandler.procedure_view()
        self.assertTrue(len(res) >= 3)

if __name__ == "__main__":
    unittest.main()

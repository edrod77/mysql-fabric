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

"""Unit tests for statistics.
"""
import unittest
import tests.utils
import sys

import mysql.fabric.command as _command
import mysql.fabric.events as _events
import mysql.fabric.protocols.xmlrpc as _xmlrpc

from mysql.fabric.node import (
    FabricNode,
)

NEW_EXECUTION_EVENT_1 = _events.Event()
class ClassCommand_1(_command.ProcedureCommand):
    """Emulates a remote command that triggers a procedure with error.
    """
    group_name = "test"
    command_name = "execution_event"

    def execute(self, synchronous=True):
        """Method that is remotely executed.
        """
        procedures = _events.trigger(
            NEW_EXECUTION_EVENT_1, self.get_lockable_objects()
        )
        return self.wait_for_procedures(procedures, synchronous)

@_events.on_event(NEW_EXECUTION_EVENT_1)
def _new_procedure_group_0():
    """Procedure triggered by NEW_EXECUTION_EVENT_1.
    """
    raise Exception("Error.")

class TestHandlerServices(tests.utils.TestCase):
    """Unit tests for statistics.
    """
    def setUp(self):
        """Configure the existing environment
        """
        self.manager, self.proxy = tests.utils.setup_xmlrpc()

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        tests.utils.teardown_xmlrpc(self.manager, self.proxy)

    def test_statistics_node(self):
        """Test node view.
        """
        # Check statistics on Fabric node.
        res = self.proxy.statistics.node()
        fabric = FabricNode()
        self.check_xmlrpc_simple(res, {
            'node_startup': str(fabric.startup),
        }, rowcount=1)

    def test_statistics_group(self):
        """Test statistics on a group.
        """
        address_1 = tests.utils.MySQLInstances().get_address(0)
        address_2 = tests.utils.MySQLInstances().get_address(1)

        # Check statistics on a non-existent group.
        packet = self.proxy.statistics.group("non-existent")
        result = _xmlrpc._decode(packet)
        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].rowcount, 0)

        # Check statistics on group_id_1 after a promotion.
        self.proxy.group.create("group_id_1")
        self.proxy.group.add("group_id_1", address_1)
        self.proxy.group.promote("group_id_1")
        packet = self.proxy.statistics.group()
        self.check_xmlrpc_simple(packet, {
            'group_id': 'group_id_1',
            'call_count': 1,
            'call_abort': 0,
        }, rowcount=1)

        # Check statistics on group_id_1 after a demotion.
        self.proxy.group.demote("group_id_1")
        res = self.proxy.statistics.group("group_id_1")
        self.check_xmlrpc_simple(res, {
            'group_id': 'group_id_1',
            'call_count': 1,
            'call_abort': 1,
        }, rowcount=1)

        # Check statistics on group_id_2 after a promotion.
        self.proxy.group.create("group_id_2")
        self.proxy.group.add("group_id_2", address_2)
        self.proxy.group.promote("group_id_2")
        res = self.proxy.statistics.group("group_id_2")
        self.check_xmlrpc_simple(res, {
            'group_id': 'group_id_2',
            'call_count': 1,
            'call_abort': 0,
        }, rowcount=1)

        # Check statistics on all groups with a common pattern.
        res = self.proxy.statistics.group("group_id")
        self.check_xmlrpc_simple(res, {}, rowcount=2)

        # Check statistics on all groups.
        res = self.proxy.statistics.group()
        self.check_xmlrpc_simple(res, {}, rowcount=2)

    def test_statistics_procedure(self):
        """Test statistics on procedures.
        """
        # Check statistics on a non-existent procedure.
        res = self.proxy.statistics.procedure("non-existent")
        result = _xmlrpc._decode(res)
        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].rowcount, 0)

        # Check statistics on procedures using the "statistics" pattern.
        self.proxy.statistics.group("group_id")
        res = self.proxy.statistics.procedure("statistics")
        self.check_xmlrpc_simple(res, {
            'proc_name': 'statistics.group',
            'call_count': 1,
            'call_abort': 0,
        }, index=0)
        self.check_xmlrpc_simple(res, {
            'proc_name': 'statistics.procedure',
            'call_count': 2,
            'call_abort': 0,
        }, index=1)
        
        # Check statistics on procedures that fail.
        self.proxy.test.execution_event()
        res = self.proxy.statistics.procedure("test.execution_event")
        self.check_xmlrpc_simple(res, {
            'proc_name': 'test.execution_event',
            'call_count': 1,
            'call_abort': 1,
        }, rowcount=1)

        # Check statistics on procedures that are asynchronously executed and
        # fail. Note that an error is not reported because the procedure is
        # asynchronously executed.
        self.proxy.test.execution_event(False)
        res = self.proxy.statistics.procedure("test.execution_event")
        self.check_xmlrpc_simple(res, {
            'proc_name': 'test.execution_event',
            'call_count': 2,
            'call_abort': 0,
        }, rowcount=1)

        # Check statistics on procecures executed so far, i.e. all procedures.
        res = self.proxy.statistics.procedure()
        self.check_xmlrpc_simple(res, {
            'proc_name': 'statistics.group',
            'call_count': 1,
            'call_abort': 0,
        }, index=0, rowcount=3)
        self.check_xmlrpc_simple(res, {
            'proc_name': 'statistics.procedure',
            'call_count': 5,
            'call_abort': 0,
        }, index=1, rowcount=3)
        self.check_xmlrpc_simple(res, {
            'proc_name': 'test.execution_event',
            'call_count': 2,
            'call_abort': 0,
        }, index=2, rowcount=3)

if __name__ == "__main__":
    unittest.main()

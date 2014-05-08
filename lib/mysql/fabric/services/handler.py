#
# Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
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
"""Retrieve statistic information.
"""
import mysql.fabric.utils as _utils

from mysql.fabric.handler import (
    MySQLHandler,
)

from mysql.fabric.command import (
    Command,
)

from mysql.fabric.node import (
    FabricNode,
)

class Node(Command):
    """Retrieve statistics on the Fabric node.
    """
    group_name = "statistics"
    command_name = "node"

    def execute(self):
        """Statistics on the Fabric node.

        It returns information on the Fabric node, specifically a list with
        the following fileds: node identification, how long it is running,
        when it was started.
        """
        return Command.generate_output_pattern(_node_view)

class Procedure(Command):
    """Retrieve statistics on Procedures.
    """
    group_name = "statistics"
    command_name = "procedure"

    def execute(self, procedure_name=None):
        """Statistics on the Fabric node.

        It returns information on procedures that match the %procedure_name%
        pattern. The information is returned is a list in which each member
        of the list is also a list with the following fields: procedure name,
        number of successful calls, number of unsuccessful calls.

        :param procedure_name: Procedure one wants to retrieve information on.
        """
        return Command.generate_output_pattern(
            MySQLHandler.procedure_view, procedure_name
        )

class Group(Command):
    """Retrieve statistics on Procedures.
    """
    group_name = "statistics"
    command_name = "group"

    def execute(self, group_id=None):
        """Statistics on a Group.

        It returns how many promotions and demotions were executed within a
        group. Specifically, a list with the following fields are returned:
        group_id, number of promotions, number of demotions.

        :param group_id: Group one wants to retrieve information on.
        """
        return Command.generate_output_pattern(
            MySQLHandler.group_view, group_id
        )

def _node_view():
    """Retrieve information on the Fabric node.
    """
    fabric = FabricNode()
    node_id = fabric.uuid
    node_startup = fabric.startup
    node_uptime = _utils.get_time() - node_startup

    return [[str(node_id), str(node_uptime), str(node_startup)]]

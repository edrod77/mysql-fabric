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
"""Unit tests for testing ErrorLog.
"""
import unittest
import uuid as _uuid
import tests.utils

import mysql.fabric.persistence as _persistence

from mysql.fabric.server import (
    MySQLServer,
)
from mysql.fabric.error_log import (
    ErrorLog,
)
from mysql.fabric.utils import (
    get_time,
    get_time_delta,
)

class TestErrorLog(unittest.TestCase):
    """Unit test for testing ErrorLog.
    """
    def setUp(self):
        """Configure the existing environment
        """
        uuid = MySQLServer.discover_uuid(
            tests.utils.MySQLInstances().get_address(0)
        )
        self.server = MySQLServer(_uuid.UUID(uuid),
            tests.utils.MySQLInstances().get_address(0)
        )
        MySQLServer.add(self.server)

    def tearDown(self):
        """Clean up the existing environment
        """
        tests.utils.cleanup_environment()
        self.server.disconnect()
        MySQLServer.remove(self.server)

    def test_init(self):
        """Test basic properties/methods in the ErrorLog.
        """
        # Check that the input parameters are not changed.
        interval = get_time_delta(1)
        now = get_time()
        input_whens = [ 30, 40 ]
        input_reporters = [ "reporter", "reporter" ]
        st = ErrorLog(self.server, interval, now, input_whens,
                               input_reporters)
        self.assertEqual(st.server_uuid, self.server.uuid)
        self.assertEqual(st.interval, interval)
        self.assertEqual(st.now, now)
        self.assertEqual(st.whens, input_whens)
        self.assertEqual(st.reporters, input_reporters)

        # If whens and reporters don't have the same length, an exception is
        # raised
        interval = get_time_delta(1)
        now = get_time()
        input_whens = [ 0, 0, 0, 0 ]
        input_reporters = []
        self.assertRaises(AssertionError, ErrorLog,
                          self.server, interval, now, input_whens,
                          input_reporters)

    def test_persistence(self):
        """Test ErrorLog.
        """
        # Update/Notify and fetch, they should match.
        interval = get_time_delta(1)
        now = get_time()
        input_whens = [ now, now ]
        input_reporters = [ "client:1000", "client:2000" ]
        st = ErrorLog(self.server, interval, now, input_whens,
                               input_reporters)
        ErrorLog.add(self.server, now, "client:1000", "error")
        ErrorLog.add(self.server, now, "client:2000", "error")
        new_st = ErrorLog.fetch(self.server, interval, now)
        self.assertEqual(st.reporters, new_st.reporters)
        self.assertEqual(st.whens, new_st.whens)

        # Call remove, they should be empty and match.
        interval = get_time_delta(1)
        now = get_time()
        input_whens = [ ]
        input_reporters = [ ]
        ErrorLog.remove(self.server)
        st = ErrorLog(self.server, interval, now, input_whens,
                               input_reporters)
        new_st = ErrorLog.fetch(self.server, interval, now)
        self.assertEqual(st.reporters, new_st.reporters)
        self.assertEqual(st.whens, new_st.whens)

        # Update/Notify and refresh, they should match.
        interval = get_time_delta(10)
        now = get_time()
        input_whens = [ now, now - get_time_delta(5) ]
        input_reporters = [ "client:1000", "client:2000" ]
        st = ErrorLog(self.server, interval, now, [], [])
        ErrorLog.add(self.server, now, "client:1000", "error")
        ErrorLog.add(self.server, now - get_time_delta(5),
                            "client:2000", "error")
        ErrorLog.add(self.server, now - get_time_delta(11),
                            "client:3000", "error")
        st.refresh()
        self.assertEqual(set(st.reporters), set(input_reporters))
        self.assertEqual(set(st.whens), set(input_whens))

        # Check whether a statement similar to the one used in the
        # event is fine.
        ErrorLog.remove(self.server)
        ErrorLog.add(self.server, now, "client:1000", "error")
        ErrorLog.add(self.server, now, "client:2000", "error")
        persister = _persistence.current_persister()
        out = persister.exec_stmt(
            "SELECT reported, UTC_TIMESTAMP() as now, "
            "TIMEDIFF(UTC_TIMESTAMP(), reported - MAKETIME(2,0,0)) as diff "
            "FROM error_log"
        )
        self.assertEqual(len(out), 2)
        res = persister.exec_stmt(
            "DELETE FROM error_log WHERE "
            "TIMEDIFF(UTC_TIMESTAMP(), reported - MAKETIME(2,0,0)) > "
            "MAKETIME(1,0,0)"
        )
        out = persister.exec_stmt(
            "SELECT reported, UTC_TIMESTAMP() as now, "
            "TIMEDIFF(UTC_TIMESTAMP(), reported - MAKETIME(2,0,0)) as diff "
            "FROM error_log"
        )
        self.assertEqual(len(out), 0)

    def test_check_instability(self):
        """Test whether a server can be considered unstable or not.
        """
        # Update/Notify and refresh, they should match.
        interval = get_time_delta(10)
        now = get_time()
        input_whens = [ now, now - get_time_delta(5) ]
        input_reporters = [ "client:1000", "client:2000" ]
        st = ErrorLog(self.server, interval, now, [], [])
        ErrorLog.add(self.server, now, "client:1000", "error")
        ErrorLog.add(self.server, now - get_time_delta(5),
                            "client:2000", "error")
        ErrorLog.add(self.server, now - get_time_delta(11),
                            "client:3000", "error")
        st.refresh()
        self.assertEqual(
            st.is_unstable(n_notifications=1, n_reporters=1,
                           filter_reporter=None),
            True
        )
        self.assertEqual(
            st.is_unstable(n_notifications=2, n_reporters=2,
                           filter_reporter=None),
            True
        )
        self.assertEqual(
            st.is_unstable(n_notifications=3, n_reporters=2,
                           filter_reporter=None),
            False
        )
        self.assertEqual(
            st.is_unstable(n_notifications=2, n_reporters=3,
                           filter_reporter=None),
            False
        )
        self.assertEqual(
            st.is_unstable(n_notifications=1, n_reporters=1,
                           filter_reporter=["client:2000"]),
            True
        )

if __name__ == "__main__":
    unittest.main()

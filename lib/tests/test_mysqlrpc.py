#
# Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.
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

import json
import tests.utils

from mysql.connector import errors as cpy_errors, ClientFlag, errorcode

from mysql.fabric.utils import FABRIC_UUID
from mysql.fabric.protocols import mysqlrpc


class TestProtocolMySQLRPCGlobals(tests.utils.TestCase):

    """Test Globals of fabric.protocols.mysqlrpc"""

    def _assertRegexSuccess(self, cases, pattern):
        for case in cases:
            match = pattern.match(case)
            self.assertNotEqual(None, match,
                                "Failed parsing: '{0}'".format(case))

    def _assertRegexFail(self, cases, pattern):
        for case in cases:
            match = pattern.match(case)
            self.assertEqual(
                None, match,
                "Parsed, but should have failed: '{0}'".format(case))

    def test_CHECK_CALL(self):
        cases = [
            "CALL",
            "call",
            "CALL   ",
            "call\n\t"
        ]
        self._assertRegexSuccess(cases, mysqlrpc.CHECK_CALL)

        cases = [
            "  CALL  ",
            " CALL",
            "\n\tCALL",
            " call",
        ]
        self._assertRegexFail(cases, mysqlrpc.CHECK_CALL)

    def test_PARSE_CALL(self):
        cases = [
            "CALL dump.fabric_nodes()",
            "call DuMp.servers(patterns=%s)",
            "CALL dump.servers(NULL, 'group1%')",
            "CALL dump.servers(NULL, group1%)",
        ]
        self._assertRegexSuccess(cases, mysqlrpc.PARSE_CALL)

        cases = [
            "CALL fabric_nodes()",
            "CALL dump.dump.servers(patterns=%s)",
        ]
        self._assertRegexFail(cases, mysqlrpc.PARSE_CALL)

    def test_PARSE_CALL_ARGS(self):
        cases = [
            "ham, spam",
            "'single','quoted'",
            '"double", "quoted"',
        ]
        exp_results = [
            ['ham', ' spam'],
            ["'single'", "'quoted'"],
            ['"double"', ' "quoted"'],
        ]
        for case, exp in zip(cases, exp_results):
            self.assertEqual(exp, mysqlrpc.PARSE_CALL_ARGS.split(case))

    def test_PARSE_CALL_KWARGS(self):
        cases = [
            "ham=spam",
            "single='quoted'",
            'double="quoted"',
            'spaces =  unquoted',
            'spaces =  "double quoted"',
            "spaces   = 'single quoted' ",
            "novalue",
            'wrong-separator',
        ]
        exp_results = [
            ['ham', 'spam'],
            ['single', "'quoted'"],
            ['double', '"quoted"'],
            ['spaces ', '  unquoted'],
            ['spaces ', '  "double quoted"'],
            ['spaces   ', " 'single quoted' "],
            ["novalue"],
            ['wrong-separator'],
        ]
        for case, exp in zip(cases, exp_results):
            self.assertEqual(exp, mysqlrpc.PARSE_CALL_KWARG.split(case))

    def test_CHECK_SHOW_CREATE_PROC(self):
        cases = [
            "SHOW CREATE PROCEDURE",
            "show   create  procedure",
            "show   CReaTe \n procedure\n\t"
        ]
        self._assertRegexSuccess(cases, mysqlrpc.CHECK_SHOW_CREATE_PROC)

        cases = [
            "  SHOW CREATE PROCEDURE",
            "  show   create  procedure",
            "   show   CReaTe \n procedure\n\t"
        ]
        self._assertRegexFail(cases, mysqlrpc.CHECK_SHOW_CREATE_PROC)

    def test_PARSE_SHOW_CREATE_PROC(self):
        cases = [
            "SHOW CREATE PROCEDURE ham.spam",
            "show   CReaTe \n procedure\n\t ham.spam",
        ]
        self._assertRegexSuccess(cases, mysqlrpc.PARSE_SHOW_CREATE_PROC)

        cases = [
            "SHOW CREATE PROCEDURE ham.spam()"
            "SHOW CREATE PROCEDURE NotCorrectSyntax ham.spam()",
            "SHOW CREATE PROCEDURE ham.spam.SPAM()",
            "SHOW CREATE PROCEDURE",
            "SHOW CREATE PROCEDURE  ",
        ]
        self._assertRegexFail(cases, mysqlrpc.PARSE_SHOW_CREATE_PROC)

    def test_PARSE_IS_ROUTINES(self):
        cases = [
            "SELECT * FROM INFORMATION_SCHEMA.ROUTINES",
            "SeLeCT ham, spam FRoM INFoRMaTioN_SCHeMa.RouTINES where spam  ",
            "SELECT * FROM INFORMATION_SCHEMA.ROUTINES   ",
        ]
        self._assertRegexSuccess(cases, mysqlrpc.PARSE_IS_ROUTINES)

        cases = [
            "  SELECT * FROM INFORMATION_SCHEMA.ROUTINES",
            "SELECT FROM INFORMATION_SCHEMA.ROUTINES",
            "SEL ect * FROM INFORMATION_SCHEMA.TABLES",
        ]
        self._assertRegexFail(cases, mysqlrpc.PARSE_IS_ROUTINES)

    def test_PARSE_SET(self):
        cases = [
            "SET format=json",
            "SET ham = spam",
            "SET format = json",
            "set FoRMaT=Json",
            "SET format = json \n\t  ",
        ]
        self._assertRegexSuccess(cases, mysqlrpc.PARSE_SET)

        cases = [
            "SET for mat=json",
            "SETTING for mat=spam",
            "  SETT @format=json",
        ]
        self._assertRegexFail(cases, mysqlrpc.PARSE_SET)

    def test_next_connection_id(self):
        exp = mysqlrpc.NEXT_CNX_ID + 1
        self.assertEqual(exp, mysqlrpc.next_connection_id())


class TestProtocolMySQLRPC(tests.utils.TestCase):

    """Test command interface through MySQL-RPC"""

    def setUp(self):
        self.cnx = tests.utils.setup_mysqlrpc()

    def tearDown(self):
        self.cnx.close()
        tests.utils.cleanup_environment()

    def test_wrong_command_parameter(self):
        cur = self.cnx.cursor()
        try:
            cur.execute("CALL dump.servers(patternsss=%)")
        except cpy_errors.DataError as exc:
            exp_errno = errorcode.ER_WRONG_PARAMETERS_TO_PROCEDURE
            self.assertTrue(isinstance(exc, cpy_errors.DataError))
            self.assertEqual(exp_errno, exc.errno)
            self.assertEqual('22000', exc.sqlstate,
                             "Wrong SQLState, was {0}".format(exc.sqlstate))

    def test_dump_servers(self):
        cur = self.cnx.cursor()
        exp_results = [
            [(unicode(FABRIC_UUID), 1, None)],
            [],
        ]
        results = []
        for result in cur.execute("CALL dump.servers(patterns=%)", multi=True):
            results.append(result.fetchall())

        self.assertEqual(exp_results, results)

    def test_ping(self):
        self.assertEqual(None, self.cnx.ping())
        self.cnx.close()
        self.assertRaises(cpy_errors.InterfaceError, self.cnx.ping)

    def test_check_protocol_41(self):
        self.cnx.set_client_flags([-ClientFlag.PROTOCOL_41])
        self.assertRaises(cpy_errors.InterfaceError,
                          self.cnx.reconnect)

    def test_check_multiresult(self):
        self.cnx.set_client_flags([-ClientFlag.MULTI_RESULTS])
        self.assertRaises(cpy_errors.InterfaceError,
                          self.cnx.reconnect)

    def test_format_json(self):
        cur = self.cnx.cursor()
        cur.execute("SET format=json")

        cur.execute("CALL dump.servers(patterns=%)")
        exp = [[{u'message': None,
                 u'fabric_uuid': unicode(FABRIC_UUID), u'ttl': 1}], []]
        self.assertEqual(exp, json.loads(cur.fetchone()[0]))

    def test_format_unknown(self):
        cur = self.cnx.cursor()
        self.assertRaises(cpy_errors.ProgrammingError,
                          cur.execute, "SET format=spam")

    def test_show_create_procedure(self):
        cur = self.cnx.cursor()
        cur.execute("SHOW CREATE PROCEDURE dump.servers")
        self.assertTrue(cur.fetchone()[0].startswith('dump servers'))

    def test_show_create_procedure_unknown(self):
        cur = self.cnx.cursor()
        self.assertRaises(cpy_errors.DataError, cur.execute,
                          "SHOW CREATE PROCEDURE ham.spam")

    def test_is_routines(self):
        cur = self.cnx.cursor()
        cur.execute("SELECT * FROM INFORMATION_SCHEMA.ROUTINES")

        exp = [u'SPECIFIC_NAME', u'ROUTINE_CATALOG', u'ROUTINE_SCHEMA',
               u'ROUTINE_NAME', u'ROUTINE_TYPE']
        self.assertEqual(exp, [ d[0] for d in cur.description])

        found = False
        needle = u'dump.servers'
        for row in cur:
            if row[0] == needle:
                found = True
                break

        self.assertTrue(found, "Command '{0}' not in result".format(needle))

    def test_call_syntax_error(self):
        cur = self.cnx.cursor()
        cases = [
            "CALL NotCorrectSyntax ham.spam()",
            "CALL"
        ]
        for case in cases:
            self.assertRaises(
                cpy_errors.ProgrammingError,
                cur.execute, case,
                "ProgrammingError not raised with '{0}'".format(case)
            )

    def test_show_create_proc_syntax_error(self):
        cur = self.cnx.cursor()
        cases = [
            "SHOW CREATE PROCEDURE NotCorrectSyntax ham.spam()",
            "SHOW CREATE PROCEDURE ham.spam.SPAM()",
            "SHOW CREATE PROCEDURE",
        ]
        for case in cases:
            self.assertRaises(
                cpy_errors.ProgrammingError,
                cur.execute, case,
                "ProgrammingError not raised with '{0}'".format(case)
            )

    def _execute_cmd(self, exp_nr_results, stmt, params=()):
        cur = self.cnx.cursor()
        results = []

        for res in cur.execute(stmt, params, multi=True):
            results.append(cur.fetchall())

        self.assertEqual(exp_nr_results, len(results),
                         "Got {0} result sets but expected {1}".format(
                             len(results), exp_nr_results))

        return results

    def _execute_cmd_check_error(self, needle, stmt, params=()):
        results = self._execute_cmd(1, stmt, params)
        self.assertTrue(needle in results[0][0][-1])

    def _test_cmd_results(self, cmd_name, cases, exp):

        # The first result is the info, should always be there
        try:
            if not exp[0][0] == unicode(FABRIC_UUID):
                raise IndexError
        except IndexError:
            exp.insert(0, (unicode(FABRIC_UUID), 1, None))

        for i, case in enumerate(cases):
            self.assertEqual(exp[i], case,
                             "Result {0} not correct {1}".format(i, cmd_name))

    def test_cmd_group_create_destroy(self):
        new_group = 'testgroup'
        stmt = "CALL group.create(%s, description='A Test Group')"
        results = self._execute_cmd(3, stmt, (new_group,))

        cases = [
            results[0][0],
            results[1][-1][-3:],
            results[2][-1][-1]
        ]
        exp = [
            (1, 1, 1),
            u'Executed action (_create_group).'
        ]
        self._test_cmd_results('group.create', cases, exp)

        # Create same group, should give error
        stmt = "CALL group.create(%s, description='A Test Group')"
        self._execute_cmd_check_error('already exists', stmt, (new_group,))

        # Destroy group
        stmt = "CALL group.destroy(%s)"
        results = self._execute_cmd(3, stmt, (new_group,))

        cases = [
            results[0][0],
            results[1][-1][-3:],
            results[2][-1][-1]
        ]
        exp = [
            (1, 1, 1),
            u'Executed action (_destroy_group).',
        ]
        self._test_cmd_results('group.destroy', cases, exp)

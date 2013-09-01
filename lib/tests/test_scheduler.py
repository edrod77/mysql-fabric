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

"""Unit tests for the scheduler module.
"""
import unittest
import logging
import threading
import time

from mysql.fabric import (
    executor as _executor,
    scheduler as _scheduler,
    errors as _errors,
)

_LOGGER = logging.getLogger(__name__)

class Run(threading.Thread):
    def __init__(self, scheduler, procedure):
        threading.Thread.__init__(self)
        self.lock = threading.Condition()
        self.scheduler = scheduler
        self.procedure = procedure

    def run(self):
        try:
            procedure = self.scheduler.get(self.procedure)
            assert(procedure is not None)
            while True:
                time.sleep(1)
        except _errors.LockManagerError:
            pass

class TestScheduler(unittest.TestCase):
    """Test scheduler.
    """
    def test_scheduler(self):
        scheduler = _scheduler.Scheduler()
        procedure_1 = _executor.Procedure()
        objects_1 = set(["lock"])
        objs = { "lock" : [procedure_1] }
        procs = { procedure_1 : (objects_1, threading.current_thread(), None) }
        free = [ procedure_1 ]

        # Enqueue a procedure.
        scheduler.enqueue_procedure(procedure_1)

        # Get next procedure.
        locked_procedure = scheduler.next_procedure()
        self.assertEqual(locked_procedure, procedure_1)
        self.assertEqual(scheduler.lock_manager.objects, objs)
        self.assertEqual(scheduler.lock_manager.procedures, procs)
        self.assertEqual(scheduler.lock_manager.free, free)

        # Release a procedure.
        scheduler.done(procedure_1)
        self.assertEqual(scheduler.lock_manager.objects, {})
        self.assertEqual(scheduler.lock_manager.procedures, {})
        self.assertEqual(scheduler.lock_manager.free, [])


class TestLockManager(unittest.TestCase):
    """Test LockManager.
    """
    def test_lock(self):
        scheduler = _scheduler.LockManager()
        procedure_1 = _executor.Procedure()
        objects_1 = set(["a", "b", "c"])
        objs = { "a" : [procedure_1], "b" : [procedure_1],
                "c" : [procedure_1]
        }
        procs = { procedure_1 : (objects_1, threading.current_thread(), None) }
        free = [ procedure_1 ]

        # Lock a procedure.
        scheduler.lock(procedure_1, objects_1)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Try to lock a procedure twice.
        self.assertRaises(
            _errors.LockManagerError, scheduler.lock, procedure_1, objects_1
        )
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Release a procedure.
        scheduler.release(procedure_1)
        self.assertEqual(scheduler.objects, {})
        self.assertEqual(scheduler.procedures, {})
        self.assertEqual(scheduler.free, [])

        # Release a procedure that does not exist.
        self.assertRaises(
            _errors.LockManagerError, scheduler.release, procedure_1
        )

    def test_lock_priority(self):
        scheduler = _scheduler.LockManager()

        self.assertEqual(scheduler.objects, {})
        self.assertEqual(scheduler.procedures, {})
        self.assertEqual(scheduler.free, [])

        # Lock procedure 1.
        procedure_1 = _executor.Procedure()
        objects_1 = set(["a", "b", "c"])
        objs = { "a" : [procedure_1], "b" : [procedure_1],
                "c" : [procedure_1]
        }
        procs = { procedure_1 : (objects_1, threading.current_thread(), None) }
        free = [ procedure_1 ]
        scheduler.lock(procedure_1, objects_1)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Lock procedure 2 with high priority.
        procedure_2 = _executor.Procedure()
        objects_2 = set(["a", "c"])
        objs = { "a" : [procedure_2],  "c" : [procedure_2] }
        procs = { procedure_2 : (objects_2, threading.current_thread(), None) }
        free = [ procedure_2 ]
        scheduler.lock(procedure_2, objects_2, True)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

    def test_enqueue(self):
        scheduler = _scheduler.LockManager()

        self.assertEqual(scheduler.objects, {})
        self.assertEqual(scheduler.procedures, {})
        self.assertEqual(scheduler.free, [])

        # Enqueue procedure 1.
        procedure_1 = _executor.Procedure()
        objects_1 = set(["a", "b", "c"])
        objs = { "a" : [procedure_1], "b" : [procedure_1],
                "c" : [procedure_1]
        }
        procs = { procedure_1 : (objects_1, None, None) }
        free = [ procedure_1 ]
        scheduler.enqueue(procedure_1, objects_1)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Enqueue procedure 2.
        procedure_2 = _executor.Procedure()
        objects_2 = set(["a", "c"])
        scheduler.enqueue(procedure_2, objects_2)
        objs["a"].append(procedure_2)
        objs["c"].append(procedure_2)
        procs[procedure_2] = (objects_2, None, None)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Enqueue procedure 3.
        procedure_3 = _executor.Procedure()
        objects_3 = set(["b"])
        scheduler.enqueue(procedure_3, objects_3)
        objs["b"].append(procedure_3)
        procs[procedure_3] = (objects_3, None, None)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Enqueue procedure 4.
        procedure_4 = _executor.Procedure()
        objects_4 = set(["f"])
        scheduler.enqueue(procedure_4, objects_4)
        objs["f"] = [procedure_4]
        procs[procedure_4] = (objects_4, None, None)
        free.append(procedure_4)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Enqueue procedure 5.
        procedure_5 = _executor.Procedure()
        objects_5 = set(["d", "f"])
        scheduler.enqueue(procedure_5, objects_5)
        objs["d"] = [procedure_5]
        objs["f"].append(procedure_5)
        procs[procedure_5] = (objects_5, None, None)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Release procedure 5.
        del objs["d"]
        objs["f"].remove(procedure_5)
        del procs[procedure_5]
        scheduler.release(procedure_5)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(scheduler.free, free)

        # Release procedure 1.
        objs["a"].remove(procedure_1)
        objs["b"].remove(procedure_1)
        objs["c"].remove(procedure_1)
        del procs[procedure_1]
        free.remove(procedure_1)
        free.append(procedure_2)
        free.append(procedure_3)
        scheduler.release(procedure_1)
        self.assertEqual(scheduler.objects, objs)
        self.assertEqual(scheduler.procedures, procs)
        self.assertEqual(set(scheduler.free), set(free))

    def test_check_conflicts(self):
        scheduler = _scheduler.LockManager()

        # Enqueue procedures.
        procedure_1 = _executor.Procedure()
        objects_1 = set(["a", "b", "c"])

        procedure_2 = _executor.Procedure()
        objects_2 = set(["a", "c"])

        procedure_3 = _executor.Procedure()
        objects_3 = set(["b"])

        procedure_4 = _executor.Procedure()
        objects_4 = set(["f"])

        scheduler.enqueue(procedure_1, objects_1)
        scheduler.enqueue(procedure_2, objects_2)
        scheduler.enqueue(procedure_3, objects_3)
        scheduler.enqueue(procedure_4, objects_4)

        # Check which procedures have acquire/request locks on "a".
        procedures = scheduler.check_conflicts(set(["a"]))
        self.assertEqual(set([procedure_1, procedure_2]), set(procedures))

        # Check which procedures have acquire/request locks on "c".
        procedures = scheduler.check_conflicts(set(["c"]))
        self.assertEqual(set([procedure_1, procedure_2]), set(procedures))

        # Check which procedures have acquire/request locks on "b".
        procedures = scheduler.check_conflicts(set(["b"]))
        self.assertEqual(set([procedure_1, procedure_3]), set(procedures))

        # Check which procedures have acquire/request locks on "f".
        procedures = scheduler.check_conflicts(set(["f"]))
        self.assertEqual(set([procedure_4]), set(procedures))

    def test_break_conflicts_nothread(self):
        scheduler = _scheduler.LockManager()

        # Enqueue procedures.
        procedure_1 = _executor.Procedure()
        objects_1 = set(["a", "b", "c"])

        procedure_2 = _executor.Procedure()
        objects_2 = set(["a", "c"])

        procedure_3 = _executor.Procedure()
        objects_3 = set(["b"])

        procedure_4 = _executor.Procedure()
        objects_4 = set(["f"])

        scheduler.enqueue(procedure_1, objects_1)
        scheduler.enqueue(procedure_2, objects_2)
        scheduler.enqueue(procedure_3, objects_3)
        scheduler.enqueue(procedure_4, objects_4)

        # Abort procedures which have acquire/request locks on "f".
        self.assertEqual(scheduler.free, [procedure_1, procedure_4])
        self.assertTrue(procedure_4 in scheduler.procedures)
        procedures = scheduler.break_conflicts(set(["f"]))
        self.assertEqual(set([procedure_4]), set(procedures))
        self.assertEqual(scheduler.free, [procedure_1])
        self.assertTrue(procedure_4 not in scheduler.procedures)

        # Abort procedures which have acquire/request locks on "b".
        self.assertTrue(procedure_1 in scheduler.procedures)
        self.assertTrue(procedure_3 in scheduler.procedures)
        procedures = scheduler.break_conflicts(set(["b"]))
        self.assertEqual(set([procedure_1, procedure_3]), set(procedures))
        self.assertEqual(scheduler.free, [procedure_2])
        self.assertTrue(procedure_1 not in scheduler.procedures)
        self.assertTrue(procedure_3 not in scheduler.procedures)

    def test_break_conflicts_thread(self):
        scheduler = _scheduler.LockManager()

        # Enqueue procedures.
        procedure_1 = _executor.Procedure()
        objects_1 = set(["a"])

        procedure_2 = _executor.Procedure()
        objects_2 = set(["b"])

        scheduler.enqueue(procedure_1, objects_1)
        scheduler.enqueue(procedure_2, objects_2)

        # Start threads.
        thread_1 = Run(scheduler, procedure_1)
        thread_1.start()
        thread_2 = Run(scheduler, procedure_2)
        thread_2.start()

        # Abort threads which have acquire/request locks on "a"
        # and "b".
        procedures = scheduler.break_conflicts(set(["a"]))
        self.assertEqual(set([procedure_1]), set(procedures))
        procedures = scheduler.break_conflicts(set(["b"]))
        self.assertEqual(set([procedure_2]), set(procedures))

        # Wait for threads.
        thread_1.join()
        thread_2.join()


if __name__ == "__main__":
    unittest.main()

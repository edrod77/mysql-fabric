"""Tests the functionality that distributes the MySQL system directory to a
given set of servers. Since it is difficult to test scp, we currently test the
functionality using the cp features. Basically by creating local directories
and copying between them.
"""

import os.path
import unittest
import shutil
import mysql.hub.commands.distribute_datadir as _dd

class TestDistributeDatadir(unittest.TestCase):
    """Create a set of directories in the local system that can be used to
        test the basic parallel copying functionality. Invoke the datadir
        copying functionality on these set of local directories.s
    """
    def setUp(self):
        """Creates the following a "source" and four destination directories,
            on which the parallel copying functionality is invoked.sss
        """
        self.base_dir = os.path.join(os.getcwd(),  "test_distribute_dirs")

        shutil.rmtree(self.base_dir, ignore_errors=True)

        os.mkdir(self.base_dir)

        self.source = os.path.join(self.base_dir,  "source")
        self.source_dir1 = os.path.join(self.source,  "dir1")
        self.source_dir1_file1 = os.path.join(self.source_dir1,  \
                                                                "dir1_file1.txt")
        self.source_dir1_file2 = os.path.join(self.source_dir1,  \
                                                                "dir1_file2.txt")
        self.source_dir2 = os.path.join(self.source,  "dir2")
        self.source_dir3 = os.path.join(self.source,  "dir3")

        self.destn1 = os.path.join(self.base_dir,  "destn1")
        self.destn2 = os.path.join(self.base_dir,  "destn2")
        self.destn3 = os.path.join(self.base_dir,  "destn3")
        self.destn4 = os.path.join(self.base_dir,  "destn4")

        os.mkdir(self.source)
        os.mkdir(self.source_dir1)
        os.mkdir(self.source_dir2)
        os.mkdir(self.source_dir3)
        open(self.source_dir1_file1,  "w").close()
        open(self.source_dir1_file2,  "w").close()

        os.mkdir(self.destn1)
        os.mkdir(self.destn2)
        os.mkdir(self.destn3)
        os.mkdir(self.destn4)

    def tearDown(self):
        """Clean up the created directories structure.
        """
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def test_distribute_datadir(self):
        """Call the parallel copying functionality on the source dir.
        """
        destinations = [self.destn1,  self.destn2,  self.destn3,  self.destn4]
        _dd.data_dir_copy_initiate(self.source,  destinations,  5, 3, True)
        list1 = os.listdir(self.source)
        list2 = os.listdir(self.destn1)
        list3 = os.listdir(self.destn2)
        list4 = os.listdir(self.destn3)
        list5 = os.listdir(self.destn4)
        self.assertEqual(set(list1),  set(list2))
        self.assertEqual(set(list1),  set(list3))
        self.assertEqual(set(list1),  set(list4))
        self.assertEqual(set(list1),  set(list5))

if __name__ == "__main__":
    unittest.main()

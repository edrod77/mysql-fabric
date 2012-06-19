"""Unit tests for resource module.
"""

import unittest
import mysql.hub.resource as resource
import mysql.hub.errors as errors

from tests.utils import DummyManager

class TestResourceManager(unittest.TestCase):
    """Test resource manager.
    """

    def setUp(self):
        manager = DummyManager()
        self.resources = resource.ResourceManager(manager)

    def test_basic(self):
        resources = self.resources

        # Test that manipulations of non-existant paths trigger an
        # error
        self.assertRaises(errors.PathError, resources.get, 'mysql.nonexistant')
        self.assertRaises(errors.PathError, resources.set, 'mysql.nonexistant', 1919)

        # Check creation of paths
        resources.create('mysql.dummy', 4711)
        val1, _ = resources.get('mysql.dummy')
        self.assertEqual(val1, 4711)
        self.assertRaises(errors.PathError, resources.create, 'mysql.dummy', 4711)
        self.assertRaises(errors.PathError, resources.create, 'mysql', 4711)

        #
        # Test changing values of paths
        #
        success, val2, _ = resources.set('mysql.dummy', 1919)
        self.assertTrue(success)
        self.assertEqual(val2, 1919)
        self.assertRaises(errors.PathError, resources.set, 'mysql.nonexistant', 1919)
        # Test that you cannot set the value of a subtree
        self.assertRaises(errors.PathError, resources.set, 'mysql', 1919)

        # Test iterating over nodes in a subtree
        items = [ tree.name for tree in resources.get('mysql') ]
        self.assertTrue('dummy' in items and len(items) == 1)

        #
        # Testing delete
        #

        # Check that we can't delete a subtree
        self.assertRaises(errors.PathError, resources.delete, 'mysql')

        # Insert another subnode and check that we can delete the
        # other node and that it is really deleted.
        resources.create('mysql.another', 19)
        success, _version = resources.delete('mysql.dummy')
        self.assertTrue(success, "Delete of resource should return True")
        self.assertRaises(errors.PathError, resources.delete, 'mysql.dummy')
        self.assertRaises(errors.PathError, resources.get, 'mysql.dummy')
        self.assertRaises(errors.PathError, resources.set, 'mysql.dummy', 1212)
        
        # Check that the inserted node is still there
        _val, _ver = resources.get('mysql.another')
        self.assertEqual(_val, 19)

        # Check that the subtree still exists
        self.assertTrue(len(resources.get('mysql')) > 0)

        # Check that deletion of the last resource in the subtree
        # removes the entire subtree.
        success, _version = resources.delete('mysql.another')
        self.assertRaises(errors.PathError, resources.get, 'mysql') 

    def test_versions(self):
        resources = self.resources

        # First create a path and check that we get the same version
        # from it for subsequent retrievals.
        resources.create('mysql.dummy', 100)
        value, ver1 = resources.get('mysql.dummy')
        value, ver1a = resources.get('mysql.dummy')
        self.assertEqual(value, 100)
        self.assertEqual(ver1, ver1a, "Version should not be increased")

        # Set the value to something new and check that the value is
        # stored and the version is increased.
        success, _, _ = resources.set('mysql.dummy', 200)
        self.assertTrue(success)
        val2, ver2 = resources.get('mysql.dummy')
        self.assertEqual(val2, 200)
        self.assertGreater(ver2, ver1, "Version should be increased")

        # Try to get a value with the wrong version
        value, ver2b = resources.get('mysql.dummy', ver1)
        self.assertEqual(value, None)
        self.assertEqual(ver2, ver2b)

        # Try to set the value to something but provide the wrong
        # version. Ensure that the value and the version is not
        # changed.
        success, val2a, ver2a = resources.set('mysql.dummy', 300, ver1)
        self.assertFalse(success)
        self.assertGreater(ver2a, ver1)
        self.assertEqual(ver2a, ver2)
        val2b, ver2b = resources.get('mysql.dummy')
        self.assertEqual(val2b, 200)
        self.assertEqual(ver2b, ver2)

        # Check that we can set the value if we give the correct
        # version
        success, val3, ver3 = resources.set('mysql.dummy', 300, ver2b)
        self.assertTrue(success)
        self.assertEqual(val3, 300)
        self.assertGreater(ver3, ver2b)

        # Check that deleting with wrong version does not delete the
        # entry...
        success, ver3a = resources.delete('mysql.dummy', ver2b)
        self.assertFalse(success)
        self.assertEqual(ver3a, ver3)
        val3a, _ = resources.get('mysql.dummy')
        self.assertEqual(val3, val3a)

        # ...but using the right version does.
        success, _ = resources.delete('mysql.dummy', ver3a)
        self.assertTrue(success)
        self.assertRaises(errors.PathError, resources.get, 'mysql.dummy')

    def test_triggers(self):
        resources = self.resources
        check = []
        def make_trigger(x):
            def inner(value, version):
                check.append((x, value, version))
            return inner

        resources.create('mysql.dummy', 100)
        resources.add_trigger('mysql.dummy', make_trigger(3))
        resources.set('mysql.dummy', 200)
        self.assertTrue((3, 200, 2) in check and len(check) == 1)

if __name__ == '__main__':
    unittest.main()

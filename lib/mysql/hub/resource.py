"""Resource management module.
"""

import mysql.hub.errors as errors

SEP = '.'

# Error messages generated inside the module
_RES_UNDEF = "Resource '{0}' not defined ('{1}' missing)"
_RES_NOTRES = "Resource '{0}' not defined ('{0}' is a subtree)"
_RES_DEF = "Resource '{0}' already defined"

class Resource(object):
    """A resource node in the resource tree.
    All leaves in the resource tree are instances of the resource class.
    """

    __slots__ = ('__value', '__version', '__name', '__triggers')

    def __init__(self, value, name):
        self.__value = value
        self.__name = name
        self.__version = 1
        self.__triggers = []

    def set_value(self, value):
        self.__value = value
        self.__version += 1

        # Execute all the triggers associated with the resource.
        # TODO: Queue the function to the Execute rather than execute them here.
        for trg in self.__triggers:
            trg(self.__value, self.__version)
        self.__triggers = []

    @property
    def value(self):
        return self.__value

    @property
    def version(self):
        return self.__version

    @property
    def name(self):
        return self.__name

    def add_trigger(self, func):
        """Add a function as a trigger to the resource.

        The function will be called with the arguments (value,
        version) when the value in the resource is changed.
        """
        self.__triggers.append(func)

class ResourceManager(object):
    """Class that manages the resource tree.

    The resource tree is used to store information about the farm
    being managed. The data is organized in a tree form, similar to a
    directory structure, with the leaves ("files") being data
    structures.

    The resource tree is implemented to support wait-free operations,
    meaning that there is a version assigned to each value as well.
    """

    def __init__(self, manager):
        self.__root = {}
        self.__manager = manager

    def _trail_for(self, path):
        """Compute a "trail" to the leaf node.

        The trail consists of pairs (key, node) where node[key] leads
        to the next node on the trail.
        """
        trail = []
        tree = self.__root
        for part in path:
            trail.append((part, tree))
            try:
                tree = tree[part]
            except KeyError:
                message = _RES_UNDEF.format(SEP.join(path), part)
                raise errors.PathError(message)
        return trail

    def _find_subtree(self, path):
        """Find the subtree where the resource is located.
        """
        trail = self._trail_for(path)
        leaf, tree = trail.pop()
        return tree, leaf

    def _follow(self, path):
        """Follow a path to find a subtree or resource.
        """
        if isinstance(path, basestring):
            path = path.split(SEP)
        tree, leaf = self._find_subtree(path)
        if leaf in tree:
            return tree[leaf]
        else:
            message = _RES_UNDEF.format(SEP.join(path), leaf)
            raise errors.PathError(message)

    def create(self, path, initial):
        if isinstance(path, basestring):
            path = path.split(SEP)
        leaf = path[-1]
        tree = self.__root
        for part in path[:-1]:
            tree = tree.setdefault(part, {})
        if leaf in tree:
            message = _RES_DEF.format(SEP.join(path))
            raise errors.PathError(message)
        tree[leaf] = Resource(initial, leaf)
        return None

    def delete(self, path, version=None):
        if isinstance(path, basestring):
            path = path.split(SEP)
        trail = self._trail_for(path)
        leaf, tree = trail.pop()
        if leaf not in tree:
            message = _RES_UNDEF.format(SEP.join(path), leaf)
            raise errors.PathError(message)
        if not isinstance(tree[leaf], Resource):
            message = _RES_NOTRES.format(SEP.join(path))
            raise errors.PathError(message)
        if version is None or tree[leaf].version == version:
            del tree[leaf]
            while len(tree) == 0 and len(trail) > 0:
                leaf, tree = trail.pop()
                del tree[leaf]
            return True, version
        return False, tree[leaf].version

    def get(self, path, version=None):
        tree = self._follow(path)
        if isinstance(tree, Resource):
            if version is None or version == tree.version:
                return tree.value, tree.version
            return None, tree.version
        return tree.values()

    def set(self, path, value, version=None):
        tree = self._follow(path)
        if isinstance(tree, Resource):
            if version is None or version == tree.version:
                tree.set_value(value)
                return (True, tree.value, tree.version)
            else:
                return (False, tree.value, tree.version)
        message = _RES_NOTRES.format(SEP.join(path))
        raise errors.PathError(message)

    def add_trigger(self, path, func, version=None):
        tree = self._follow(path)
        if isinstance(tree, Resource):
            if version is None or version == tree.version:
                tree.add_trigger(func)
                return (True, tree.version)
            else:
                return (False, tree.version)

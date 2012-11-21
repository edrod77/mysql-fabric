"""Define features that can be used throughout the code.
"""
class SingletonMeta(type):
    """Define a Singleton.
    This Singleton class can be used as follows::

      class MyClass(object):
        __metaclass__ = SingletonMeta
      ...
    """
    _instances = {}
    def __call__(mcs, *args, **kwargs):
        if mcs not in mcs._instances:
            mcs._instances[mcs] = super(SingletonMeta, mcs).__call__(*args,
                                                                     **kwargs)
        return mcs._instances[mcs]


class Singleton(object):
    """Define a Singleton.
    This Singleton class can be used as follows::

      class MyClass(Singleton):
      ...
    """
    __metaclass__ = SingletonMeta

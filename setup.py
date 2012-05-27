from distutils.core import setup

import os

def find_packages(*args, **kwrds):
    """Find all packages and sub-packages and return a list of them.

    The function accept any number of directory names that will be
    searched to find packages. Packages are identified as
    sub-directories containing an __init__.py file.  All duplicates
    will be removed from the list and it will be sorted
    alphabetically.

    Packages can be excluded by pattern using the 'exclude' keyword,
    which accepts a list of patterns.  All packages with names that
    match the beginning of an exclude pattern will be excluded.
    """

    from fnmatch import fnmatch
    excludes = kwrds.get('exclude', [])
    pkgs = {}
    for base_path in args:
        for root, _dirs, files in os.walk(base_path):
            if '__init__.py' in files:
                assert root.startswith(base_path)
                pkg = root[len(base_path)+1:].replace(os.sep, '.')
                pkgs[pkg] = root

    result = pkgs.keys()
    for excl in excludes:
        # We exclude packages that *begin* with an exclude pattern.
        result = [pkg for pkg in result if not fnmatch(pkg, excl + "*")]
    result.sort()
    return result

ARGS = {}

META_INFO = {
    'name': "mysql-hub",
    'version': "0.1",
    'license': "GPLv2",
    'description': "Management system for MySQL deployments",
    'packages': find_packages("lib", exclude=["mysql.hub.tests"]),
    'package_dir': { '': 'lib' },
    'requires': [
        'mysql.connector (>=1.0)',
        ],
    'classifiers': [
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        ],
}

ARGS.update(META_INFO)

setup(**ARGS)

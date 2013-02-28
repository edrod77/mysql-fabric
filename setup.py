import distutils
import os
import sys
import glob

from distutils.core import setup, Command
from distutils.sysconfig import get_python_lib

def fetch_version(module_name):
    """Retrieve information on Fabric version within *module_name*.
    """
    mod = __import__(module_name, globals(), locals(), ['__version__'])
    return mod.__version__

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

def check_connector():
    """Check if connector is properly installed.

    It returns a tuple with the following information:
    * Installed - Indicates whether the connector is properly installed
                  or not.
    * Path MySQL - Path to the package *mysql*.
    * Connector Path - Path to the package *mysql.connector*.
    """
    path_connector = None
    path_mysql = None
    try:
        import mysql
        path_mysql = os.path.dirname(mysql.__file__)

        import mysql.connector
        path_connector = os.path.dirname(mysql.connector.__file__)

        return True, path_mysql, path_connector
    except ImportError as error:
        return False, path_mysql, path_connector

def check_sphinx():
    """Check if sphinx is properly installed.

    It returns a tuple with the following information:
    * Installed - Indicates whether sphinx is properly installed
                  or not.
    * Sphinx Path - Path to the package *sphinx*.
    """
    path_sphinx = None
    try:
        import sphinx
        path_sphinx = os.path.dirname(sphinx.__file__)
        return True, path_sphinx
    except ImportError as error:
        return False, path_sphinx

def check_fabric():
    """Check if Fabric is properly built/installed.

    It returns a tuple with the following information:
    * Installed - Indicates whether Fabric is properly installed
                  or not.
    * Path MySQL - Path to the package *mysql*.
    * Fabric Path - Path to the package *mysql.hub*.
    """
    path_fabric = None
    path_mysql = None
    try:
        import mysql
        path_mysql = os.path.dirname(mysql.__file__)

        import mysql.hub
        path_fabric = os.path.dirname(mysql.hub.__file__)
        return True, path_mysql, path_fabric
    except ImportError as error:
        return False, path_mysql, path_fabric

def check_path_for_docs(directory):
    """Set the path to ** when documentation is being generated and
    check whether fabric and connector python are properly installed
    or not.
    """
    fix_path(directory)

    result, path_mysql, path_connector = check_connector()
    if not result:
        sys.stderr.write(
            "Tried to look for mysql.connector at (%s).\n" % \
            (path_mysql, )
            )
        sys.stderr.write(
            "Sphinx was supposed to use (%s).\n" % \
            (sys.path[0])
            )
        exit(1)

    result, path_myql, path_fabric = check_fabric()
    if not result:
        sys.stderr.write(
            "Tried to look for mysql.hub at (%s).\n" % \
            (path_mysql, )
            )
        sys.stderr.write(
            "Sphinx was supposed to use (%s).\n" % \
            (sys.path[0])
            )
        exit(1)

def fix_path(directory):
    """Fix path by pointing to the appropriate directory.
    """
    sys.path.insert(0, os.path.abspath(directory))

#
# If Sphinx is installed, create a documentation builder based
# on it. Otherwise, create a fake builder just to report something
# when --help-commands are executed so that users may find out
# that it is possible to build the documents provided Sphinx is
# properly installed.
#
result, path_sphinx = check_sphinx()
if result:
    import sphinx.setup_command as _sphinx
    class build_docs(_sphinx.BuildDoc):
        """Create documentation using Sphinx.
        """
        user_options = _sphinx.BuildDoc.user_options + [
            ("code-dir=", None, "Look for code in the directory."),
            ]

        description = "Create documentation using sphinx"

        def initialize_options(self):
            _sphinx.BuildDoc.initialize_options(self)
            self.code_dir = None

        def finalize_options(self):
            _sphinx.BuildDoc.finalize_options(self)
            if not self.code_dir:
                self.use_directory = get_python_lib()
            else:
                self.use_directory = self.code_dir

        def run(self):
            check_path_for_docs(self.use_directory)
            _sphinx.BuildDoc.run(self)
else:
    class build_docs(Command):
        """Create documentation. Please, install sphinx.
        """
        user_options = [
            ('unknown', None, "Sphinx is not installed. Please, install it."),
            ]
        description = "create documentation. Please, install sphinx"

        def initialize_options (self):
            pass

        def finalize_options (self):
            pass

        def run(self):
            sys.stderr.write(
                "Sphinx is not installed. Please, install it.\n"
                )
            exit(1)

META_INFO = {
    'name': "mysql-hub",
    'license': "GPLv2",
    'description': "Management system for MySQL deployments",
    'packages': find_packages("lib", exclude=["tests"]),
    'package_dir': {
        '': 'lib',
        },
    'requires': [
        'mysql.connector (>=1.0)',
        ],
    'scripts': [
        'scripts/fabric',
        ],
    'data_files' : [
        ('/etc/fabric/', ['data/main.cfg']),
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
    'cmdclass': {
        'build_docs' : build_docs,
        },
}

#
# When building the documentation that path is fixed based on
# the --code-dir option within the build_docs class.
#
if "build_docs" not in sys.argv:
    fix_path("lib")
    META_INFO ['version'] = fetch_version('mysql.hub')
setup(**META_INFO)

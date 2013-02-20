import distutils
import os
import sys
import glob

from distutils.core import setup, Command

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

def fix_path_for_docs():
    """Set the path to *build/lib* when a documentation is being
    generated and check whether the connector is properly installed
    or not.
    """
    built = glob.glob("./build/lib.*")
    if built:
        for path in built:
            version =  \
                ".".join([str(version) for version in sys.version_info[0:2]])
            if path.find(version) != -1:
                sys.path.insert(0, os.path.abspath(built[0]))

    result, path_mysql, path_connector = check_connector()
    if not result:
        sys.stderr.write(
            "Tried to look for mysql.connector at (%s).\n" % (path_mysql, )
            )
        exit(1)

    result, path_myql, path_fabric = check_fabric()
    if not result:
        sys.stderr.write(
            "Tried to look for mysql.hub at (%s).\n" % (path_mysql, )
            )
        exit(1)
 

def fix_path_for_code():
    """Set the path to *lib* when a documentation is being
    generated.
    """
    sys.path.insert(0, os.path.abspath("lib"))


# If Sphinx is installed, create a documentation builder based
# on it. Otherwise, create a fake builder just to report something
# when --help-commands are executed so that users may find out
# that it is possible to build the documents provided Sphinx is
# properly installed.
if check_sphinx():
    import sphinx.setup_command
    class build_docs(sphinx.setup_command.BuildDoc):
        """
        Create documentation using Sphinx.
        """
        description = "Create documentation using sphinx"
else:
    class build_docs(Command):
        """
        Create documentation. Please, install sphinx.
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
            
DOC_INFO = {
    'cmdclass': { 'build_docs' : build_docs
        },
    }

# It is necessary to check the command-line parameters at this point and
# fix the path because the fetch version function imports "mysql.hub"
# thus setting the path to a mysql package. While generating documents,
# we want this path to be build/lib.
#
# TODO: REMOVE fetch_version so we can avoid fixing the path here and
# do it within a command.
#
if "build_docs" in sys.argv:
    fix_path_for_docs()
else:
    fix_path_for_code()


META_INFO = {
    'name': "mysql-hub",
    'version': fetch_version('mysql.hub'),
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

META_INFO.update(DOC_INFO)
setup(**META_INFO)

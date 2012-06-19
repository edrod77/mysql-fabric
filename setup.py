import distutils
import glob
import os
import stat

from distutils.command.build_scripts import build_scripts as _build_scripts
from distutils.command.build import build as _build
from distutils.core import setup
from distutils.util import convert_path


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

META_INFO = {
    'name': "mysql-hub",
    'version': "0.1",
    'license': "GPLv2",
    'description': "Management system for MySQL deployments",
    'packages': find_packages("lib", exclude=["tests"]),
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

_COMMANDS = []

for fname in glob.glob('lib/mysql/hub/commands/*.py'):
    head, tail = os.path.split(fname)
    if tail == '__init__.py':
        continue
    mod, _ = os.path.splitext(tail)
    pkg = '.'.join(head.split(os.sep)[1:])
    _COMMANDS.append((pkg, mod))

# Build scripts to install
class my_build_scripts(_build_scripts):
    def run(self):
        # Copy existing scripts first
        _build_scripts.run(self)
        # Create scripts for all modules found in mysql.hub.commands
        self.mkpath(self.build_dir)

        for pkg, mod in _COMMANDS:
            # Lines without terminating newline
            lines = [
                "#!" + self.executable,
                "",
                "import sys",
                "",
                "from {0}.{1} import main".format(pkg, mod),
                "",
                "main(sys.argv[1:])",
                ]

            # Write the file contents
            cmdname = "hub-" + mod
            outfile = os.path.join(self.build_dir, cmdname)
            distutils.log.info("creating script %s" % (cmdname,))
            with open(outfile, "w") as out:
                out.writelines(line + "\n" for line in lines)
            os.chmod(cmdname, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

class my_build(_build):
    def has_scripts(self):
        return _build.has_scripts(self) or len(_COMMANDS) > 0

    sub_commands = []
    for cmd in _build.sub_commands:
        if cmd[0] == 'build_scripts':
            cmd = (cmd[0], has_scripts)
        sub_commands.append(cmd)

ARGS = {
    'cmdclass': {
        'build': my_build,
        'build_scripts': my_build_scripts,
        },
    }

ARGS.update(META_INFO)

setup(**ARGS)

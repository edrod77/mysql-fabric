"""Package that contain all the commands of the system as separate
modules.

All command-line scripts are placed in this package and when the
system is installed, a small stub is generated that call the main
function of the module.

For example, consider the mysql.hub.command.start module in this
package. When installing, a script ``hub-start`` will be created
contaning the code::

   from mysql.hub.command.start import main
   main()

In this package, there also exist some helper utilities that are used
to implement commands, such as option parsing utilities.
"""


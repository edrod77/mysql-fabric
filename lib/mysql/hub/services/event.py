"""Service interface for working with events.
"""

import mysql.hub.events as _events

def trigger(event, *args):
    try:
        _events.trigger(event, *args)
        return True
    except Exception as err:
        return err

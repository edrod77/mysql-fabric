import os.path
import glob

def fetch_modules():
    result = []
    here = os.path.dirname(os.path.realpath(__file__))
    for path in glob.glob(os.path.join(here, '*.py')):
        _dir, fname = os.path.split(path)
        base, _ext = os.path.splitext(fname)
        if not base.endswith('__init__'):
            result.append(base)
    return result

__all__ = fetch_modules()

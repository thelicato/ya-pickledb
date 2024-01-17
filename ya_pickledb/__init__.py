__title__ = "ya-pickledb"
__version__ = "0.1.0"
__description__ = "Yet another PickleDB (thread-safe!)"

from .pickledb import YAPickleDB


def load(location, auto_dump):
    '''Return a pickledb object. location is the path to the json file.'''
    return YAPickleDB(location, auto_dump)

import os
import json
import msgpack
import atexit
import safer
from zlib import crc32
from threading import Lock, Thread
from typing import Dict
from .cache import Cache
from .errors import KeyStringError, FileAccessError, LoadChecksumError


class YAPickleDB(object):
    def __init__(self, location, auto_dump):
        '''Creates a database object and loads the data from the location path.
        If the file does not exist it will be created on the first update.
        '''
        self.load(location, auto_dump)
        self.dthread = None
        self.db_lock = Lock()
        self.cache = Cache()

        # Write data into the database on exit
        atexit.register(self._autodumpdb)

    def __getitem__(self, item):
        '''Syntax sugar for get()'''
        return self.get(item)

    def __setitem__(self, key, value):
        '''Syntax sugar for set()'''
        return self.set(key, value)

    def __delitem__(self, key):
        '''Syntax sugar for rem()'''
        return self.rem(key)

    def load(self, location, auto_dump):
        '''Loads, reloads or changes the path to the db file'''
        location = os.path.expanduser(location)
        self.path = location
        self.auto_dump = auto_dump
        if os.path.exists(location):
            self._loaddb()
        else:
            self.db = {}
        return True

    def _dump(self):
        '''Dump memory db to file'''
        if self.dthread is not None and self.dthread.is_alive():
            self.dthread.join()

        self.dthread = Thread(
            target=Util.dump_db, args=(self, self.db_lock)
        )
        self.dthread.start()
        self.dthread.join()
        return True

    def commit(self):
        self._dump()

    def _loaddb(self):
        '''Load or reload the json info from the file'''
        try:
            self.db = Util.read_plain_db(self)
        except ValueError:
            if os.stat(self.path).st_size == 0:  # Error raised because file is empty
                self.db = {}
            else:
                raise  # File is not empty, avoid overwriting it

    def _autodumpdb(self):
        '''Write/save the json dump into the file if auto_dump is enabled'''
        if self.auto_dump:
            self._dump()

    def _check_key_cache(self, key):
        if self.cache.is_key_expired(key):
            self.cache.delete_key(key)
            self.rem(key)

    def _check_all_keys(self):
        for key in self.db.keys():
            self._check_key_cache(key)

    def set(self, key, value, max_age=None):
        '''Set the str or int value of a key'''
        if isinstance(key, str) or isinstance(key, int):
            self.db[key] = value
            if max_age:
                self.cache.add_key_to_cache(key, max_age)
            else:
                self._autodumpdb()
            return True
        else:
            raise KeyStringError("Key/name must be a string!")

    def get(self, key):
        '''Get the value of a key'''
        try:
            self._check_key_cache(key)
            return self.db[key]
        except KeyError:
            return None

    def getall(self):
        '''Return a list of all keys in db'''
        self._check_all_keys()
        return self.db.keys()

    def exists(self, key):
        '''Return True if key exists in db, return False if not'''
        self._check_all_keys()
        return key in self.db

    def rem(self, key):
        '''Delete a key'''
        if not key in self.db:  # return False instead of an exception
            return False
        del self.db[key]
        self._autodumpdb()
        return True

    def totalkeys(self, name=None):
        '''Get a total number of keys, lists, and dicts inside the db'''
        self._check_all_keys()
        total = len(self.db)
        return total

    def lcreate(self, name):
        '''Create a list, name must be str'''
        if isinstance(name, str):
            self.db[name] = []
            self._autodumpdb()
            return True
        else:
            raise KeyStringError("Key/name must be a string!")

    def lpush(self, name, value):
        '''Add a value to a list'''
        self.db[name].append(value)
        self._autodumpdb()
        return True

    def lextend(self, name, seq):
        '''Extend a list with a sequence'''
        self.db[name].extend(seq)
        self._autodumpdb()
        return True

    def lgetall(self, name):
        '''Return all values in a list'''
        return self.db[name]

    def lget(self, name, pos):
        '''Return one value in a list'''
        return self.db[name][pos]

    def lrange(self, name, start=None, end=None):
        '''Return range of values in a list '''
        return self.db[name][start:end]

    def lremlist(self, name):
        '''Remove a list and all of its values'''
        number = len(self.db[name])
        del self.db[name]
        self._autodumpdb()
        return number

    def lremvalue(self, name, value):
        '''Remove a value from a certain list'''
        self.db[name].remove(value)
        self._autodumpdb()
        return True

    def lpop(self, name, pos):
        '''Remove one value in a list'''
        value = self.db[name][pos]
        del self.db[name][pos]
        self._autodumpdb()
        return value

    def llen(self, name):
        '''Returns the length of the list'''
        return len(self.db[name])

    def lexists(self, name, value):
        '''Determine if a value  exists in a list'''
        return value in self.db[name]

    def hcreate(self, name):
        '''Create a dict, name must be str'''
        if isinstance(name, str):
            self.db[name] = {}
            self._autodumpdb()
            return True
        else:
            raise KeyStringError("Key/name must be a string!")

    def hset(self, name, key, value):
        '''Add a key-value pair to a dict, "pair" is a tuple'''
        self.db[name][key] = value
        self._autodumpdb()
        return True

    def hget(self, name, key):
        '''Return the value for a key in a dict'''
        return self.db[name][key]

    def hgetall(self, name):
        '''Return all key-value pairs from a dict'''
        return self.db[name]

    def hrem(self, name):
        '''Remove a dict and all of its pairs'''
        del self.db[name]
        self._autodumpdb()
        return True

    def hpop(self, name, key):
        '''Remove one key-value pair in a dict'''
        value = self.db[name][key]
        del self.db[name][key]
        self._autodumpdb()
        return value

    def hkeys(self, name):
        '''Return all the keys for a dict'''
        return self.db[name].keys()

    def hvals(self, name):
        '''Return all the values for a dict'''
        return self.db[name].values()

    def hexists(self, name, key):
        '''Determine if a key exists or not in a dict'''
        return key in self.db[name]

    def deldb(self):
        '''Delete everything from the database'''
        self.db = {}
        self._autodumpdb()
        return True


class Util:
    @staticmethod
    def check_mag(mag):
        return mag == b"YAPDB"

    @staticmethod
    def read_plain_db(obj: YAPickleDB) -> Dict:
        with safer.open(obj.path, "rb") as fctx:
            if not Util.check_mag(fctx.read(5)):
                raise FileAccessError("File magic number not known")
            checksum = int.from_bytes(fctx.read(4), "little", signed=False)
            data = fctx.read()
            calculated_checksum = crc32(data)
            if calculated_checksum != checksum:
                raise LoadChecksumError(
                    f"calculated checksum: {calculated_checksum} is different from stored checksum {checksum}"
                )
            try:
                db_dump = msgpack.unpackb(data)
                curr_db = {key: json.loads(
                    db_dump[key]) for key in db_dump}
            except FileNotFoundError:
                raise FileAccessError("File not found")
            return curr_db

    @staticmethod
    def dump_db(obj: YAPickleDB, lock: Lock):
        cached_keys = obj.cache.get_cached_keys()
        db_data = {key: json.dumps(obj.db[key])
                   for key in obj.db if key not in cached_keys}

        data = msgpack.packb(db_data, use_bin_type=True)
        buffer = b"YAPDB"
        buffer += (crc32(data)).to_bytes(4, "little")
        buffer += data

        try:
            lock.acquire()
            with safer.open(obj.path, "wb") as file:
                file.write(buffer)
                lock.release()
                return True
        except:
            raise FileAccessError("File already exists")

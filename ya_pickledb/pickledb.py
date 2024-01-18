import os
import json
import msgpack
import atexit
import safer
from zlib import crc32
from threading import Lock, Thread
from typing import Dict, List, Any
from .cache import Cache
from .errors import WrongTypeError, FileAccessError, LoadChecksumError


types_mapping = {
    "location": {
        "error_str": "The location must be a string",
        "valid_types": [str]
    },
    "key": {
        "error_str": "The key must be a string",
        "valid_types": [str]
    },
    "value": {
        "error_str": "The value must be either a string, a number or a bool",
        "valid_types": [str, int, float, bool]
    },
    "position": {
        "error_str": "The value must be a int",
        "valid_types": [int]
    },
    "list": {
        "error_str": "This is not a list!",
        "valid_types": [list]
    },
    "dict": {
        "error_str": "This is not a dict!",
        "valid_types": [dict]
    }
}


class YAPickleDB(object):
    def __init__(self, location: str, auto_dump):
        '''Creates a database object and loads the data from the location path.
        If the file does not exist it will be created on the first update.
        '''
        self._ensure_type('location', location)
        self._load(location, auto_dump)
        self.dthread = None
        self.db_lock = Lock()
        self.cache = Cache()

        # Write data into the database on exit
        atexit.register(self._autodumpdb)

    def __getitem__(self, item: str) -> Any:
        '''Syntax sugar for get()'''
        return self.get(item)

    def __setitem__(self, key: str, value: str | int | float | bool) -> None:
        '''Syntax sugar for set()'''
        return self.set(key, value)

    def __delitem__(self, key: str) -> None:
        '''Syntax sugar for rem()'''
        return self.rem(key)

    def _ensure_type(self, type_key: str, value: Any) -> None:
        valid_type_keys = types_mapping.keys()
        if not type_key in valid_type_keys:
            raise Exception("Invalid type key")
        valid_types = types_mapping[type_key]["valid_types"]
        error_str = types_mapping[type_key]["error_str"]
        if not any([isinstance(value, t) for t in valid_types]):
            raise WrongTypeError(error_str)

    def _load(self, location: str, auto_dump: bool) -> bool:
        '''Loads, reloads or changes the path to the db file'''
        location = os.path.expanduser(location)
        self.path = location
        self.auto_dump = auto_dump
        if os.path.exists(location):
            self._loaddb()
        else:
            self.db = {}
        return True

    def _dump(self) -> bool:
        '''Dump memory db to file'''
        if self.dthread is not None and self.dthread.is_alive():
            self.dthread.join()

        self.dthread = Thread(
            target=Util.dump_db, args=(self, self.db_lock)
        )
        self.dthread.start()
        self.dthread.join()
        return True

    def commit(self) -> None:
        self._dump()

    def _loaddb(self) -> None:
        '''Load or reload the json info from the file'''
        try:
            self.db = Util.read_plain_db(self)
        except ValueError:
            if os.stat(self.path).st_size == 0:  # Error raised because file is empty
                self.db = {}
            else:
                raise  # File is not empty, avoid overwriting it

    def _autodumpdb(self) -> None:
        '''Write/save the json dump into the file if auto_dump is enabled'''
        if self.auto_dump:
            self._dump()

    def _check_key_cache(self, key: str) -> None:
        self._ensure_type("key", key)
        if self.cache.is_key_expired(key):
            self.cache.delete_key(key)
            self.rem(key)

    def _check_all_keys(self) -> None:
        for key in self.db.keys():
            self._check_key_cache(key)

    def set(self, key: str, value: str | int | float | bool, max_age: int = None):
        '''Set the str or int value of a key'''
        self._ensure_type('value', value)
        self.db[key] = value
        if max_age:
            self.cache.add_key_to_cache(key, max_age)
        else:
            # If max_age is set then it should not be dumped
            self._autodumpdb()
        return True

    def get(self, key: str) -> str | int | float | bool | None:
        '''Get the value of a key'''
        self._ensure_type('key', key)
        try:
            self._check_key_cache(key)
            return self.db[key]
        except KeyError:
            return None

    def getall(self) -> List[str]:
        '''Return a list of all keys in db'''
        self._check_all_keys()
        return self.db.keys()

    def exists(self, key: str) -> bool:
        '''Return True if key exists in db, return False if not'''
        self._ensure_type('key', key)
        self._check_all_keys()
        return key in self.db

    def rem(self, key: str) -> bool:
        '''Delete a key'''
        self._ensure_type('key', key)
        if not self.exists(key):
            return False
        del self.db[key]
        self._autodumpdb()
        return True

    def totalkeys(self) -> int:
        '''Get a total number of keys, lists, and dicts inside the db'''
        self._check_all_keys()
        total = len(self.db)
        return total

    def lcreate(self, name: str) -> bool:
        '''Create a list, name must be str'''
        self._ensure_type('key', name)
        self.db[name] = []
        self._autodumpdb()
        return True

    def lpush(self, name: str, value: str | int | float | bool) -> bool:
        '''Add a value to a list'''
        self._ensure_type('key', name)
        self._ensure_type('value', value)
        if not self.exists(name):
            self.lcreate(name)
        self._ensure_type('list', self.db[name])
        self.db[name].append(value)
        self._autodumpdb()
        return True

    def lgetall(self, name: str) -> List[str | int | float | bool] | None:
        '''Return all values in a list'''
        self._ensure_type('key', name)
        if not self.exists(name):
            return None
        self._ensure_type('list', self.db[name])
        return self.db[name]

    def lget(self, name: str, pos: int) -> str | int | float | bool | None:
        '''Return one value in a list'''
        self._ensure_type('key', name)
        self._ensure_type('position', pos)
        if not self.exists(name):
            return None
        self._ensure_type('list', self.db[name])
        if self.llen(name) < pos + 1:
            return None
        return self.db[name][pos]

    def lremlist(self, name: str) -> None:
        '''Remove a list and all of its values'''
        if not self.exists(name):
            return
        self._ensure_type('list', self.db[name])
        del self.db[name]
        self._autodumpdb()

    def lremvalue(self, name: str, value: str | int | float | bool) -> None:
        '''Remove a value from a certain list'''
        if not self.exists(name):
            return
        self._ensure_type('list', self.db[name])
        try:
            self.db[name].remove(value)
            self._autodumpdb()
        except ValueError:
            return

    def lpop(self, name: str, pos: int) -> str | int | float | bool | None:
        '''Remove one value in a list'''
        self._ensure_type('key', name)
        self._ensure_type('position', pos)
        if not self.exists(name):
            return None
        self._ensure_type('list', self.db[name])
        if self.llen(name) < pos + 1:
            return None
        value = self.db[name][pos]
        del self.db[name][pos]
        self._autodumpdb()
        return value

    def llen(self, name: str) -> int | None:
        '''Returns the length of the list'''
        self._ensure_type('key', name)
        if not self.exists(name):
            return None
        self._ensure_type('list', self.db[name])
        return len(self.db[name])

    def lexists(self, name: str, value: str | int | float | bool) -> bool:
        '''Determine if a value  exists in a list'''
        self._ensure_type('key', name)
        self._ensure_type('value', value)
        if not self.exists(name):
            return False
        self._ensure_type('list', self.db[name])
        return value in self.db[name]

    def hcreate(self, name: str) -> bool:
        '''Create a dict, name must be str'''
        self._ensure_type('key', name)
        self.db[name] = {}
        self._autodumpdb()
        return True

    def hset(self, name: str, key: str, value: Any) -> bool:
        '''Add a key-value pair to a dict'''
        self._ensure_type('key', name)
        self._ensure_type('key', key)
        if not self.exists(name):
            self.hcreate(name)
        self.db[name][key] = value
        self._autodumpdb()
        return True

    def hget(self, name: str, key: str) -> Any:
        '''Return the value for a key in a dict'''
        self._ensure_type('key', name)
        self._ensure_type('key', key)
        if not self.exists(name):
            return None
        self._ensure_type('dict', self.db[name])
        if not self.hexists(name, key):
            return None
        return self.db[name][key]

    def hgetall(self, name: str) -> Dict[str, Any] | None:
        '''Return all key-value pairs from a dict'''
        self._ensure_type('key', name)
        if not self.exists(name):
            return None
        self._ensure_type('dict', self.db[name])
        return self.db[name]

    def hrem(self, name: str) -> bool:
        '''Remove a dict and all of its pairs'''
        if not self.exists(name):
            return False
        self._ensure_type('dict', self.db[name])
        del self.db[name]
        self._autodumpdb()
        return True

    def hpop(self, name: str, key: str) -> Any:
        '''Remove one key-value pair in a dict'''
        self._ensure_type('key', name)
        self._ensure_type('key', key)
        if not self.exists(name):
            return None
        self._ensure_type('dict', self.db[name])
        if not self.hexists(name, key):
            return None
        value = self.db[name][key]
        del self.db[name][key]
        self._autodumpdb()
        return value

    def hkeys(self, name: str) -> List[str] | None:
        '''Return all the keys for a dict'''
        self._ensure_type('key', name)
        if not self.exists(name):
            return None
        self._ensure_type('dict', self.db[name])
        return self.db[name].keys()

    def hvals(self, name: str) -> List[Any] | None:
        '''Return all the values for a dict'''
        self._ensure_type('key', name)
        if not self.exists(name):
            return None
        self._ensure_type('dict', self.db[name])
        return self.db[name].values()

    def hexists(self, name: str, key: str) -> bool:
        '''Determine if a key exists or not in a dict'''
        self._ensure_type('key', name)
        self._ensure_type('key', key)
        if not self.exists(name):
            return False
        self._ensure_type('dict', self.db[name])
        return key in self.db[name]

    def deldb(self) -> bool:
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

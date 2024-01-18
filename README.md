<h1 align="center">
	<img src="https://github.com/thelicato/ya-pickledb/blob/main/logo.png?raw=true" width="400">
</h1>

<h4 align="center">Yet another PickleDB (thread-safe!)</h4>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-usage">Usage</a> •
  <a href="#-installation">Installation</a> •
  <a href="#-license">License</a> •
</p>

---

``ya-pickledb`` (Yet Another PickleDB) is a an open-source key-value store for *Python* using the ``msgpack`` module. It is heavily inspired by [pickledb](https://github.com/patx/pickledb), but also provides new features (like thread-safety using ``safer``). 

The new feature additions are inspired by [elara](https://github.com/saurabh0719/elara); but since it cannot serialize complex data structure to filesystem I decided to take the best of both worlds and create a new package.

## ⚡ Features

- Manipulate different data structures (strings, lists, dictionaries and so on)
- Fast and flexible
- Thread-safe!
- Choose between manual and auto commits
- Cache simple data structures

## 📚 Usage

```python
import ya_pickledb

db = ya_pickledb.load('kvstore.db', True)

db.set('foo', 'bar')
value = db.get('foo')

print(value)
```

This is the full list of all functions available to the user:
- ``commit()``: manually save the data to file storage.
- ``set(key, value, max_age=None)``: set the value of a key.
- ``get(key)``: get the value of a key.
- ``getall()``: get a list of all keys.
- ``exists(key)``: get wheter a key exists or not.
- ``rem(key)``: remove a key.
- ``totalkeys()``: get a total number of keys inside the db.
- ``lcreate(key)``: create a list.
- ``lpush(key, value)``: add a value to a list.
- ``lgetall(key)``: return all values in a list.
- ``lget(key, value, pos)``: return the value in a specific position of a list.
- ``lremlist(key)``: remove a list.
- ``lremvalue(key, value)``: remove a value from a list.
- ``lpop(key, pos)``: remove ne value in a list.
- ``llen(key)``: return the length of a list.
- ``lexists(key, value)``: determine if a value exists in a list.
- ``hcreate(key)``: create a dict.
- ``hset(key, dict_key, value)``: add a key-value pair to a dict.
- ``hget(key, dict_key)``: get the value for a key in a dict.
- ``hgetall(key)``: get all the key-value pairs from a dict.
- ``hrem(key)``: remove a dict.
- ``hpop(key, dict_key)``: remove one key-value pair fro a dict.
- ``hkeys(key)``: get all the keys for a dict.
- ``hvals(key)``: get all the values for a dict.
- ``hexists(key, dict_key)``: determine if a key exists in a dict.

All the functions should return ``None`` if something goes wrong (e.g. the ``key`` accessed is missing). Errors are raised when the input type is wrong (e.g. setting a ``bool`` as key).

## 🚀 Installation

Run the following command to install the latest version:

```
pip install ya_pickledb
```


## 🪪 License

*ya_pickledb* is made with 🖤 by the [thelicato](https://thelicato.io) and released under the [MIT LICENSE](https://github.com/thelicato/ya-pickledb/blob/main/LICENSE).
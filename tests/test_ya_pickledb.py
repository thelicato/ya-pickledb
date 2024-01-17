import ya_pickledb


def test_load():
    x = ya_pickledb.load('tests.db', auto_dump=False)
    assert x is not None


def test_set():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.set('key', 'value')
    x = db.get('key')
    assert x == 'value'


def test_getall():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.deldb()
    db.set('key1', 'value1')
    db.set('key2', 'value2')
    db.dcreate('dict1')
    db.lcreate('list1')
    x = db.getall()
    y = dict.fromkeys(['key2', 'key1', 'dict1', 'list1']).keys()
    assert x == y


def test_get():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.set('key', 'value')
    x = db.get('key')
    assert x == 'value'


def test_rem():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.set('key', 'value')
    db.rem('key')
    x = db.get('key')
    assert x is False


def test_append():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.set('key', 'value')
    db.append('key', 'value')
    x = db.get('key')
    assert x == 'valuevalue'


def test_exists():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.set('key', 'value')
    x = db.exists('key')
    assert x is True
    db.rem('key')


def test_not_exists():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.set('key', 'value')
    x = db.exists('not_key')
    assert x is False
    db.rem('key')


def test_lexists():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.lcreate('list')
    db.ladd('list', 'value')
    x = db.lexists('list', 'value')
    assert x is True
    db.lremlist('list')


def test_not_lexists():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.lcreate('list')
    db.ladd('list', 'value')
    x = db.lexists('list', 'not_value')
    assert x is False
    db.lremlist('list')


def test_lrange():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.lcreate('list')
    db.ladd('list', 'one')
    db.ladd('list', 'two')
    db.ladd('list', 'three')
    db.ladd('list', 'four')
    x = db.lrange('list', 1, 3)
    assert x == ['two', 'three']
    db.lremlist('list')


def test_dexists():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.dcreate('dict')
    db.dadd('dict', ('key', 'value'))
    x = db.dexists('dict', 'key')
    assert x is True
    db.drem('dict')


def test_not_dexists():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.dcreate('dict')
    db.dadd('dict', ('key', 'value'))
    x = db.dexists('dict', 'not_key')
    assert x is False
    db.drem('dict')

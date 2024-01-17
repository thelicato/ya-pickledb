import time
import ya_pickledb


def test_cache_get():
    db = ya_pickledb.load('tests.db', auto_dump=False)
    db.set('foo', 'bar', 1)
    x = db.get('foo')
    assert x == 'bar'
    time.sleep(2)
    x = db.get('foo')
    assert x == None


def test_cache_dump():
    db = ya_pickledb.load('cache.db', auto_dump=True)
    db.set('foo', 'bar', 1)
    db.set('lorem', 'ipsum',)
    time.sleep(1)
    db = ya_pickledb.load('cache.db', auto_dump=True)
    assert 'foo' not in db.getall()

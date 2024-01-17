import os
import time
import threading
import ya_pickledb


def set_a(db: ya_pickledb.YAPickleDB):
    for i in range(100, 110):
        db.set(str(i), i)
        time.sleep(0.2)


def set_b(db: ya_pickledb.YAPickleDB):
    for i in range(200, 210):
        db.set(str(i), i)
        time.sleep(0.2)


def test_threads():
    db = ya_pickledb.load('./threads.db', auto_dump=True)
    db.deldb()

    set_a_thread = threading.Thread(target=set_a, args=(db,))
    set_b_thread = threading.Thread(target=set_b, args=(db,))
    set_a_thread.start()
    set_b_thread.start()

    # Join threads to main thread
    set_a_thread.join()
    set_b_thread.join()

    # Read db
    db = ya_pickledb.load('./threads.db', auto_dump=True)
    ranges = list(range(100, 110)) + list(range(200, 210))
    all_keys = [str(r) for r in ranges]
    x = [k for k in db.getall()]
    assert all([k in x for k in all_keys]) and len(x) == len(all_keys)

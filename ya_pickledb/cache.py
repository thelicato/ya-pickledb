import datetime


class Temp_obj:
    def __init__(self, key, max_age):
        self.key = key
        self.expiry = datetime.datetime.now() + datetime.timedelta(seconds=max_age)

    def is_expired(self):
        return datetime.datetime.now() > self.expiry


class Cache:
    cache: list[Temp_obj]

    def __init__(self) -> None:
        self.cache = []

    def _get_by_key(self, key):
        el = None
        for obj in self.cache:
            if obj.key == key:
                el = obj
                break
        return el

    def add_key_to_cache(self, key, max_age):
        temp_obj = Temp_obj(key, max_age)
        self.cache.append(temp_obj)

    def delete_key(self, key):
        for i, obj in enumerate(self.cache):
            if obj.key == key:
                self.cache.pop(i)
                break

    def is_key_expired(self, key):
        el = self._get_by_key(key)
        return el.is_expired() if el else False

    def get_cached_keys(self):
        return [obj.key for obj in self.cache]

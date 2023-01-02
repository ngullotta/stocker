import hashlib
import pickle
from pathlib import Path
from typing import Hashable, Union


class CacheController:
    def __init__(
        self, cache_path: Path, object_type_file_map: dict = {}
    ) -> None:
        self.cache_path = Path(cache_path)
        self.object_type_file_map = object_type_file_map

    def hash(self, key: Union[str, Path], hashfunc: Hashable = hashlib.sha256):
        _hash = hashfunc()
        if isinstance(key, str):
            _hash.update(key.encode())
            return _hash.hexdigest()

        if not key.exists():
            return ""

        with open(key, "rb") as fp:
            _hash.update(fp.read(0x2000))

        return _hash.hexdigest()

    def fetch(
        self,
        key: Union[str, Path],
        hashfunc: Hashable = hashlib.sha256,
        on_miss={"func": pickle.dump, "args": [], "kwargs": {}},
        on_hit={"func": pickle.load, "args": [], "kwargs": {}},
    ):
        _hash = self.hash(key, hashfunc=hashfunc)
        return self.get(_hash, on_miss=on_miss, on_hit=on_hit)

    def get(self, key: str, on_miss: dict = {}, on_hit: dict = {}) -> object:
        if not self.cache_path.exists():
            return Path()

        path = self.cache_path / key

        if path.exists():
            func = on_hit.get("func")
            args = on_hit.get("args", [])
            args.insert(0, path)
            kwargs = on_hit.get("kwargs", {})
            return func(*args, **kwargs)

        func = on_miss.get("func")
        args = on_miss.get("args", [])
        kwargs = on_miss.get("kwargs", {})
        obj = func(*args, **kwargs)
        return self.cache(key, obj)

    def cache(self, key: str, obj: object):
        if not self.cache_path.exists():
            return obj
        path = self.cache_path / key
        if path.exists():
            return obj

        _map = self.object_type_file_map.get(type(obj), None)
        if _map is None:
            return obj

        func = _map.get("name")
        args = _map.get("args")
        kwargs = _map.get("kwargs")

        if func is not pickle.dump:
            sub = kwargs.pop("sub", None)
            if sub is not None:
                obj = obj[sub]
            func = getattr(obj, func)
            args.insert(0, path)
            func(*args, **kwargs)
            return obj

        with open(path, "wb") as fp:
            pickle.dump(obj, fp, protocol=pickle.HIGHEST_PROTOCOL)

        return obj

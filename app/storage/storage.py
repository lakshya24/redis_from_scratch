from typing import Optional
import time


class Entry:
    def __init__(
        self,
        value,
        len: int,
        ttl: Optional[int] = None,
        type: str = "string",
        stream_id:Optional[str]=None,
    ):
        self.value = value
        self.len: int = len
        self.ttl_ms: float = (time.time() * 1000) + ttl if ttl else 0.0
        self.infinite_alive: bool = not ttl
        self.type: str = type
        self.stream_id: Optional[str] = stream_id

    def print(self) -> None:
        print(f"value is {self.value}, len is: {self.len}")


class Storage:
    def __init__(self) -> None:
        self._storage: dict[str, Entry] = {}

    def add(self, key: str, entry_dict: Entry) -> None:
        self._storage[key] = entry_dict

    def get(
        self,
        key: str,
    ) -> Optional[Entry]:
        if self.has(key):
            val = self._storage[key]
            return val
        return None

    def has(self, key: str) -> bool:
        return key in self._storage

    def remove(self, key: str):
        del self._storage[key]


kvPair: Storage = Storage()

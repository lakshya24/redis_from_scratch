from _collections_abc import dict_keys
import asyncio
from dataclasses import dataclass
import enum
from typing import Dict, List, Optional
import time


@dataclass
class StreamEntry:
    def __init__(self, t_ms, seq, entry_key, entry_value) -> None:
        self.t_ms = t_ms
        self.seq = seq
        self.entry_key = entry_key
        self.entry_value = entry_value
        self.stream_id: str = f"{self.t_ms}-{self.seq}"
        self.flattenned_entry: List = [
            f"{self.stream_id}",
            [self.entry_key, self.entry_value],
        ]


class RespDatatypes(enum.Enum):
    STRING = "string"
    STREAM = "stream"


class Entry:
    def __init__(
        self,
        value,
        len: int,
        ttl: Optional[int] = None,
        type: str = RespDatatypes.STRING.value,
        stream_id: Optional[str] = None,
    ) -> None:
        self.value = value
        self.len: int = len
        self.ttl_ms: float = (time.time() * 1000) + ttl if ttl else 0.0
        self.infinite_alive: bool = not ttl
        self.type: str = type
        # self.stream_id: Optional[str] = stream_id

    def print(self) -> None:
        print(f"value is {self.value}, len is: {self.len}")


class Storage:
    def __init__(self, rdb_file: Optional[str] = None) -> None:
        self._storage: Dict[str, Entry] = {}
        self.rdb_file = rdb_file

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
STREAM_LOCK = asyncio.Lock()
STREAM_CONDITIONALS = {}

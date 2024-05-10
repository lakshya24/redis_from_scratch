from abc import ABC, abstractmethod
from typing import Literal
from typing import List
from app.storage.storage import Entry, StreamEntry, kvPair
from typing import Optional
import time


class CommandProcessor(ABC):
    TERMINATOR: str = "\r\n"

    @abstractmethod
    def response(self) -> bytes:
        pass

    @classmethod
    def parse(cls, input: bytes):
        command = input.decode()
        commands = command.split("\r\n")
        # skip bulk strings
        commands = [c for c in commands if c and not c.startswith(("$", "*"))]
        command_to_exec: str = commands[0].strip().upper()
        args = commands[1:]
        if command_to_exec == "ECHO":
            return Echo(args)
        elif command_to_exec == "PING":
            return Ping()
        elif command_to_exec == "GET":
            return Get(args)
        elif command_to_exec == "SET":
            return Set(args)
        elif command_to_exec == "TYPE":
            return Type(args)
        elif command_to_exec == "XADD":
            return Xadd(args)


class Ping(CommandProcessor):
    def response(self) -> bytes:
        return b"+PONG\r\n"


class Echo(CommandProcessor):
    def response(self) -> bytes:
        if isinstance(self.message, list):
            self.message = "".join(self.message)
        return f"+{self.message}\r\n".encode()

    def __init__(self, message) -> None:
        self.message = message


class Set(CommandProcessor):
    def response(self) -> bytes:

        val_len = len(self.val)
        entry: Entry = Entry(self.val, val_len, self.px)
        kvPair.add(self.key, entry)
        return b"+OK\r\n"

    def __init__(self, message) -> None:
        self.message = message
        if not isinstance(self.message, list):
            raise Exception(f"cannot process {self.message}")
        if len(self.message) < 2:
            raise Exception(f"malfprmed key vals {self.message}")
        self.key: str = self.message[0]
        self.val: str = self.message[1]
        if len(self.message) > 3:
            self.px = int(self.message[3])
        else:
            self.px = None


class Get(CommandProcessor):
    def response(self) -> bytes:
        key: str = "".join(self.message)
        val: Optional[Entry] = kvPair.get(key)
        if val:
            found_ttl_ms: float = val.ttl_ms
            curr_ttl_ms: float = time.time() * 1000
            delta = curr_ttl_ms - found_ttl_ms
            if delta <= 0 or val.infinite_alive:
                return f"${val.len}\r\n{val.value}\r\n".encode()
            else:
                kvPair.remove(key)
        return b"$-1\r\n"

    def __init__(self, message) -> None:
        self.message = message


class Type(CommandProcessor):
    def __init__(self, message) -> None:
        self.message = message

    def response(self) -> bytes:
        lookup_key: str = self.message[0]
        val: Optional[Entry] = kvPair.get(lookup_key)
        if val:
            return f"+{val.type}\r\n".encode()
        return b"+none\r\n"


class Xadd(CommandProcessor):
    def __init__(self, message) -> None:
        self.message = message
        print(f"list is: {self.message}")
        if not isinstance(self.message, list):
            raise Exception(f"cannot process {self.message}")
        if len(self.message) < 4:
            raise Exception(f"malformed key vals {self.message}")
        self.stream_key: str = self.message[0]
        self.stream_params: List[str] = self.message[1:]

    def response(self) -> bytes:
        val_len = 2
        stream_entry: StreamEntry = StreamEntry(
            self.stream_params[0], self.stream_params[1], self.stream_params[2]
        )
        stream_values: List[StreamEntry] = [stream_entry]
        entry: Entry = Entry(stream_values, val_len, ttl=None, type="stream")
        if data := kvPair.get(self.stream_key):
            if isinstance(data.value, list):
                last_id: str = data.value[-1].entry_id
                entry_id: str = stream_entry.entry_id
                if entry_id == "0-0":
                    return "-ERR The ID specified in XADD must be greater than 0-0\r\n".encode()
                elif entry_id <= last_id:
                    return f"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".encode()
                else:
                    data.value.append(stream_entry)
            else:
                return "+Not a valid stream key\r\n".encode()
        else:
            kvPair.add(self.stream_key, entry)
        return f"+{stream_entry.entry_id}\r\n".encode()

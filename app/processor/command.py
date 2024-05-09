from abc import ABC, abstractmethod
from typing import Literal
from typing import List
from app.storage.storage import Entry, kvPair
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
        if kvPair.has(lookup_key):
            return b"+string\r\n"
        return b"+none\r\n"

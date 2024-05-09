from abc import ABC, abstractmethod
from typing import Literal
from typing import List
from app.storage.storage import Entry, kvPair
from typing import Optional


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
        command_to_exec: str = commands[0].strip()
        args = commands[1:]
        if command_to_exec == "ECHO":
            return Echo(args)
        elif command_to_exec == "PING":
            return Ping()
        elif command_to_exec == "GET":
            return Get(args)
        elif command_to_exec == "SET":
            return Set(args)


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
        if not isinstance(self.message, list):
            raise Exception(f"cannot process {self.message}")
        if len(self.message) < 2:
            raise Exception(f"malfprmed key vals {self.message}")
        key: str = self.message[0]
        val: str = self.message[1]
        val_len = len(val)
        entry: Entry = Entry(val, val_len)
        kvPair.add(key, entry)
        return b"+OK\r\n"

    def __init__(self, message) -> None:
        self.message = message


class Get(CommandProcessor):
    def response(self) -> bytes:
        self.message = "".join(self.message)
        val: Optional[Entry] = kvPair.get(self.message)

        if val:
            return f"${val.len}\r\n{val.value}\r\n".encode()
        else:
            return b"$-1\r\n"

    def __init__(self, message) -> None:
        self.message = message

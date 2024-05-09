from abc import ABC, abstractmethod
from typing import Literal
from typing import List


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

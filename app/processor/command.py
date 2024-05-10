from abc import ABC, abstractmethod
from typing import List
from app.storage.storage import Entry, RespDatatypes, StreamEntry, kvPair
from typing import Optional
import time
import enum


class Command(enum.Enum):
    PING = enum.auto()
    ECHO = enum.auto()
    SET = enum.auto()
    GET = enum.auto()
    TYPE = enum.auto()
    XADD = enum.auto()


class CommandProcessor(ABC):
    TERMINATOR: str = "\r\n"

    @abstractmethod
    def response(self) -> bytes:
        pass

    @classmethod
    def parse(cls, input: bytes):
        command = input.decode()
        commands: List[str] = command.split("\r\n")
        # all everything now including bulk strings
        command_to_exec: str = commands[2].strip().upper()
        # adjust to only extract command params from the format (len,param)
        args: List[str] = [val for idx, val in enumerate(commands[4:]) if idx % 2 == 0]
        print(f"command: {command_to_exec}, args = {args}")
        if command_to_exec == Command.ECHO.name:
            return Echo(args)
        elif command_to_exec == Command.PING.name:
            return Ping()
        elif command_to_exec == Command.GET.name:
            return Get(args)
        elif command_to_exec == Command.SET.name:
            return Set(args)
        elif command_to_exec == Command.TYPE.name:
            return Type(args)
        elif command_to_exec == Command.XADD.name:
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
        if not isinstance(self.message, list):
            raise Exception(f"cannot process {self.message}")
        if len(self.message) < 3:
            raise Exception(f"malformed key vals {self.message}")
        self.stream_key: str = self.message[0]
        self.stream_params: List[str] = self.message[1:]

    def response(self) -> bytes:
        stream_id = self.stream_params[0]
        if stream_id == "0-0":
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n".encode()
        sentry_key: str = self.stream_params[1]
        sentry_val: str = self.stream_params[2]
        return self._update_entry(stream_id, sentry_key, sentry_val)

    def _generate_entry_id(self, sentry_t_ms, sentry_seq):
        return f"{sentry_t_ms}-{sentry_seq}"

    def _update_entry(self, stream_id: str, sentry_key: str, sentry_val: str) -> bytes:
        # TODO: Fix this len logic to something more reasonable
        val_len = 2
        curr_id: str = ""
        if data := kvPair.get(self.stream_key):
            # handles the logic where key is found and it a valid stream
            if data.type == RespDatatypes.STREAM.value and isinstance(data.value, list):
                last_entry: StreamEntry = data.value[-1]
                last_id: str = f"{last_entry.t_ms}-{last_entry.seq}"
                stream_entry: StreamEntry = self._get_stream_entry(
                    stream_id, sentry_key, sentry_val, last_entry
                )
                curr_id: str = self._generate_entry_id(
                    stream_entry.t_ms, stream_entry.seq
                )
                if curr_id <= last_id:
                    return f"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".encode()
                else:
                    print(f"stream_entry generated for next row = {stream_entry}")
                    data.value.append(stream_entry)
            else:
                return "+Not a valid stream key\r\n".encode()
        else:
            ## This is equivalent to creating the cache entry for given key for the first time
            stream_entry: StreamEntry = self._get_stream_entry(
                stream_id, sentry_key, sentry_val, None
            )
            entry: Entry = Entry(
                [stream_entry], val_len, ttl=None, type=RespDatatypes.STREAM.value
            )
            kvPair.add(self.stream_key, entry)
            curr_id: str = self._generate_entry_id(stream_entry.t_ms, stream_entry.seq)
        return f"+{curr_id}\r\n".encode()

    def _get_stream_entry(
        self,
        stream_id: str,
        sentry_key: str,
        sentry_val: str,
        curr_entry: Optional[StreamEntry],
    ) -> StreamEntry:
        """
        This function handles the next seq generation based on :
        - if the initial key does not exist
            use the default sequence key [1 if sentry_t_ms == "0" else 0 ] id sentry_seq == "*" else sentry_seq
        - if key exists in cache:
            if provide seq == *:
                generate next seq by incrementing sequence of last key if t_ms is same else just use the previous deafults
            else:
                use the sentry_seq provided
        ### Args:
            - `sentry_t_ms (str)`: _description_
            - `sentry_seq (str)`: _description_
            - `sentry_key (str)`: _description_
            - `sentry_val (str)`: _description_
            - `curr_entry (Optional[StreamEntry])`: _description_

        ### Returns:
            - `StreamEntry`: newly generated stream sequence
        """
        sentry_t_ms: str = str(int(time.time() * 1000))
        sentry_seq: str = "0"
        if stream_id != "*":
            sentry_t_ms, sentry_seq = stream_id.split("-")

        default_seq: int = 1 if sentry_t_ms == "0" else 0
        if not curr_entry:
            return StreamEntry(
                t_ms=sentry_t_ms,
                seq=default_seq if sentry_seq == "*" else int(sentry_seq),
                entry_key=sentry_key,
                entry_value=sentry_val,
            )
        else:
            curr_t_ms: str = curr_entry.t_ms
            curr_seq: int = curr_entry.seq
            next_seq = default_seq if curr_t_ms != sentry_t_ms else curr_seq + 1
            return StreamEntry(
                t_ms=sentry_t_ms,
                seq=int(sentry_seq) if sentry_seq != "*" else next_seq,
                entry_key=sentry_key,
                entry_value=sentry_val,
            )

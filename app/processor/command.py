from abc import ABC, abstractmethod
from typing import List, Tuple
from app.processor.resp_coder import RespCoder
from app.storage.storage import Entry, RespDatatypes, StreamEntry, kvPair
from typing import Optional
import time
import enum
import sys


class Command(enum.Enum):
    PING = enum.auto()
    ECHO = enum.auto()
    SET = enum.auto()
    GET = enum.auto()
    TYPE = enum.auto()
    XADD = enum.auto()
    XRANGE = enum.auto()
    XREAD = enum.auto()


class CommandProcessor(ABC):

    @abstractmethod
    def response(self) -> bytes:
        pass

    @classmethod
    def parse(cls, input: bytes):
        command = input.decode()
        commands: List[str] = command.split(RespCoder.TERMINATOR)
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
        elif command_to_exec == Command.XRANGE.name:
            return XRange(args)
        elif command_to_exec == Command.XREAD.name:
            return XRead(args[1:])


class Ping(CommandProcessor):
    def response(self) -> bytes:
        return f"+PONG{RespCoder.TERMINATOR}".encode()


class Echo(CommandProcessor):
    def response(self) -> bytes:
        if isinstance(self.message, list):
            self.message = "".join(self.message)
        return f"+{self.message}{RespCoder.TERMINATOR}".encode()

    def __init__(self, message) -> None:
        self.message = message


class Set(CommandProcessor):
    def response(self) -> bytes:

        val_len = len(self.val)
        entry: Entry = Entry(self.val, val_len, self.px)
        kvPair.add(self.key, entry)
        return f"+OK{RespCoder.TERMINATOR}".encode()

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
                return f"${val.len}{RespCoder.TERMINATOR}{val.value}{RespCoder.TERMINATOR}".encode()
            else:
                kvPair.remove(key)
        return f"$-1{RespCoder.TERMINATOR}".encode()

    def __init__(self, message) -> None:
        self.message = message


class Type(CommandProcessor):
    def __init__(self, message) -> None:
        self.message = message

    def response(self) -> bytes:
        lookup_key: str = self.message[0]
        val: Optional[Entry] = kvPair.get(lookup_key)
        if val:
            return f"+{val.type}{RespCoder.TERMINATOR}".encode()
        return f"+none{RespCoder.TERMINATOR}".encode()


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
            return f"-ERR The ID specified in XADD must be greater than 0-0{RespCoder.TERMINATOR}".encode()
        sentry_key: str = self.stream_params[1]
        sentry_val: str = self.stream_params[2]
        return self._update_entry(stream_id, sentry_key, sentry_val)

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
                curr_id: str = stream_entry.stream_id
                if curr_id <= last_id:
                    return f"-ERR The ID specified in XADD is equal or smaller than the target stream top item{RespCoder.TERMINATOR}".encode()
                else:
                    print(f"stream_entry generated for next row = {stream_entry}")
                    data.value.append(stream_entry)
            else:
                return f"+Not a valid stream key{RespCoder.TERMINATOR}".encode()
        else:
            ## This is equivalent to creating the cache entry for given key for the first time
            stream_entry: StreamEntry = self._get_stream_entry(
                stream_id, sentry_key, sentry_val, None
            )
            entry: Entry = Entry(
                [stream_entry], val_len, ttl=None, type=RespDatatypes.STREAM.value
            )
            kvPair.add(self.stream_key, entry)
            curr_id: str = stream_entry.stream_id
        return f"+{curr_id}{RespCoder.TERMINATOR}".encode()

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


class XRange(CommandProcessor):
    def __init__(self, message) -> None:
        self.message = message
        self.stream_key: str = self.message[0]
        self.args: List[str] = self.message[1:] if len(self.message) >= 2 else []

    def response(self) -> bytes:
        entries: List[StreamEntry] = []
        if data := kvPair.get(self.stream_key):
            if data.type == RespDatatypes.STREAM.value and isinstance(data.value, list):
                start_range, end_range = self._find_range(data.value)
                print(f"query range is : {start_range} to {end_range}")
                entries = [
                    entry
                    for entry in data.value
                    if self._is_in_range(entry, start_range, end_range)
                ]
                flatenned_entries: List = [entry.flattenned_entry for entry in entries]
                return RespCoder.encode(flatenned_entries).encode()
            else:
                return f"+Not a valid stream key{RespCoder.TERMINATOR}".encode()
        return "[]".encode()

    def _find_range(self, data: List[StreamEntry]) -> Tuple[str, str]:
        default_start_range: str = "0-1"
        default_end_range: str = f"{sys.maxsize}-{sys.maxsize}"
        if len(self.args) > 0:
            start_range: str = (
                data[0].stream_id if self.args[0] == "-" else self.args[0]
            )
            end_range: str = data[-1].stream_id if self.args[1] == "+" else self.args[1]
            return start_range, end_range
        return default_start_range, default_end_range

    def _is_in_range(
        self, entry: StreamEntry, start_range: str, end_range: str
    ) -> bool:
        stream_id: str = entry.stream_id
        return stream_id >= start_range and stream_id <= end_range


class XRead(CommandProcessor):
    def __init__(self, message) -> None:
        self.message = message
        self.stream_keys = []
        self.stream_start = []
        self.stream_keys: List[str] = self.message[: (len(self.message) // 2)]
        self.stream_start: List[str] = self.message[(len(self.message) // 2) :]

    def response(self) -> bytes:
        flattened_stream: List = []
        for stream_key, stream_start in zip(self.stream_keys, self.stream_start):
            flattened_stream.append(self._process_one_stream(stream_key, stream_start))
        return RespCoder.encode(flattened_stream).encode()

    def _process_one_stream(self, stream_key: str, stream_start: str) -> List:
        print(f"processing stream_key: {stream_key}")
        if data := kvPair.get(stream_key):
            if data.type == RespDatatypes.STREAM.value and isinstance(data.value, list):
                start_range, end_range = self._find_range(stream_start, data.value)
                print(f"query range is : {start_range} to {end_range}")
                entries = [
                    entry
                    for entry in data.value
                    if self._is_in_range(entry, start_range, end_range)
                ]
                flatenned_entries: List = [entry.flattenned_entry for entry in entries]
                return [stream_key, flatenned_entries]
            else:
                return [f"Not a valid stream key"]
        return []

    def _find_range(
        self,
        stream_start: str,
        data: List[StreamEntry],
    ) -> Tuple[str, str]:
        start_range: str = stream_start
        end_range: str = data[-1].stream_id
        if stream_start == "-":
            start_range = "0-1"
        elif len(stream_start) == 1:
            start_range = f"{stream_start}-0"
        return start_range, end_range

    def _is_in_range(
        self, entry: StreamEntry, start_range: str, end_range: str
    ) -> bool:
        stream_id: str = entry.stream_id
        return stream_id >= start_range and stream_id <= end_range

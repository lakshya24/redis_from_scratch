from abc import ABC, abstractmethod
import asyncio
import base64
from dataclasses import asdict
from typing import List, Tuple
from app.handler.server_conf import ServerInfo
from app.processor.resp_coder import EMPTY_BYTE, RespCoder
from app.storage.storage import (
    STREAM_CONDITIONALS,
    STREAM_LOCK,
    Entry,
    RespDatatypes,
    StreamEntry,
    kvPair,
)
from typing import Optional
import time
import enum
import sys


class FollowupCode(enum.Enum):
    NO_FOLLOWUP = 0
    SEND_RDB = 1


class Command(enum.Enum):
    PING = enum.auto()
    ECHO = enum.auto()
    SET = enum.auto()
    GET = enum.auto()
    TYPE = enum.auto()
    INFO = enum.auto()
    REPLCONF = enum.auto()
    PSYNC = enum.auto()
    XADD = enum.auto()
    XRANGE = enum.auto()
    XREAD = enum.auto()
    NONE = enum.auto()


class CommandProcessor(ABC):
    command: Command = Command.NONE

    @abstractmethod
    async def response(self) -> Tuple[bytes, bytes]:
        pass

    @classmethod
    def parse(cls, input: bytes, server_info: ServerInfo):
        command = input.decode()
        commands: List[str] = command.split(RespCoder.TERMINATOR)
        # all everything now including bulk strings
        command_to_exec: str = commands[2].strip().upper()
        # adjust to only extract command params from the format (len,param)
        args: List = [val for idx, val in enumerate(commands[4:]) if idx % 2 == 0]
        print(f"command: {command_to_exec}, args = {args}")
        if command_to_exec == Command.ECHO.name:
            return Echo(args)
        elif command_to_exec == Command.PING.name:
            return Ping([])
        elif command_to_exec == Command.GET.name:
            return Get(args)
        elif command_to_exec == Command.SET.name:
            return Set(args)
        elif command_to_exec == Command.TYPE.name:
            return Type(args)
        elif command_to_exec == Command.INFO.name:
            args = [server_info]
            return Info(args)
        elif command_to_exec == Command.REPLCONF.name:
            # args = [server_info]
            return Replconf(args)
        elif command_to_exec == Command.PSYNC.name:
            args = [server_info, args]
            return Psync(args)
        elif command_to_exec == Command.XADD.name:
            return Xadd(args)
        elif command_to_exec == Command.XRANGE.name:
            return XRange(args)
        elif command_to_exec == Command.XREAD.name:
            return XRead(args)


async def get_followup_response(followup_code: FollowupCode) -> bytes:
    if followup_code == FollowupCode.NO_FOLLOWUP:
        return EMPTY_BYTE
    elif followup_code == FollowupCode.SEND_RDB:
        return await Psync.rdb_sync()
    return EMPTY_BYTE


class Ping(CommandProcessor):
    async def response(self) -> Tuple[bytes, bytes]:
        return f"+PONG{RespCoder.TERMINATOR}".encode(), await get_followup_response(
            FollowupCode.NO_FOLLOWUP
        )

    def __init__(self, message) -> None:
        self.message = message


class Echo(CommandProcessor):
    command = Command.ECHO

    async def response(self) -> Tuple[bytes, bytes]:
        if isinstance(self.message, list):
            self.message = "".join(self.message)
        return (
            f"+{self.message}{RespCoder.TERMINATOR}".encode(),
            await get_followup_response(FollowupCode.NO_FOLLOWUP),
        )

    def __init__(self, message) -> None:
        self.message = message


class Set(CommandProcessor):
    command = Command.SET

    async def response(self) -> Tuple[bytes, bytes]:

        val_len = len(self.val)
        entry: Entry = Entry(self.val, val_len, self.px)
        kvPair.add(self.key, entry)
        return f"+OK{RespCoder.TERMINATOR}".encode(), await get_followup_response(
            FollowupCode.NO_FOLLOWUP
        )

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
    command = Command.GET

    async def response(self) -> Tuple[bytes, bytes]:
        key: str = "".join(self.message)
        val: Optional[Entry] = kvPair.get(key)
        if val:
            found_ttl_ms: float = val.ttl_ms
            curr_ttl_ms: float = time.time() * 1000
            delta = curr_ttl_ms - found_ttl_ms
            if delta <= 0 or val.infinite_alive:
                return f"${val.len}{RespCoder.TERMINATOR}{val.value}{RespCoder.TERMINATOR}".encode(), await get_followup_response(
                    FollowupCode.NO_FOLLOWUP
                )
            else:
                kvPair.remove(key)
        return f"$-1{RespCoder.TERMINATOR}".encode(), await get_followup_response(
            FollowupCode.NO_FOLLOWUP
        )

    def __init__(self, message) -> None:
        self.message = message


class Type(CommandProcessor):
    command = Command.TYPE

    def __init__(self, message) -> None:
        self.message = message

    async def response(self) -> Tuple[bytes, bytes]:
        lookup_key: str = self.message[0]
        val: Optional[Entry] = kvPair.get(lookup_key)
        if val:
            return (
                f"+{val.type}{RespCoder.TERMINATOR}".encode(),
                await get_followup_response(FollowupCode.NO_FOLLOWUP),
            )
        return f"+none{RespCoder.TERMINATOR}".encode(), await get_followup_response(
            FollowupCode.NO_FOLLOWUP
        )


class Info(CommandProcessor):
    command = Command.INFO

    def __init__(self, message) -> None:
        self.message = message
        self.server_info: ServerInfo = message[0]
        self.info_keys = ["role", "master_replid", "master_repl_offset"]

    async def response(self) -> Tuple[bytes, bytes]:
        print(f"info messag is : {self.server_info}")
        info_data: str = ""
        server_info = asdict(self.server_info)
        for key in self.info_keys:
            if key == "role":
                val = server_info[key].value
            else:
                val = str(server_info[key])
            info: str = f"{key}:{val}{RespCoder.TERMINATOR}"
            info_data += info
        return RespCoder.encode_as_simple_str(
            info_data
        ).encode(), await get_followup_response(FollowupCode.NO_FOLLOWUP)


class Replconf(CommandProcessor):
    command = Command.REPLCONF
    OK_RESPONSE: str = f"+OK{RespCoder.TERMINATOR}"

    def __init__(self, message) -> None:
        self.message = message
        # self.server_info: ServerInfo = message[0]

    async def response(self) -> Tuple[bytes, bytes]:
        print(f"Replconf request on master")
        return self.OK_RESPONSE.encode(), await get_followup_response(
            FollowupCode.NO_FOLLOWUP
        )


class Psync(CommandProcessor):
    command = Command.PSYNC
    FULLRESYNC: str = "FULLRESYNC"
    EMPTY_RDB_FILE = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

    def __init__(self, message) -> None:
        self.message = message
        self.server_info: ServerInfo = message[0]
        self.args: List = message[1]

    async def response(self) -> Tuple[bytes, bytes]:
        print(f"Psync request on master....")
        if (len(self.args) < 2) or (self.args[0] != "?" and self.args[1] != "-1"):
            print("Invalid psync request")
            return f"+ERR Invalid PSNC request with params {self.args[0]} and {self.args[1]}".encode(), await get_followup_response(
                FollowupCode.NO_FOLLOWUP
            )
        master_replid: str = self.server_info.master_replid
        master_repl_offset: str = str(self.server_info.master_repl_offset)
        return f"+{self.FULLRESYNC} {master_replid} {master_repl_offset}{RespCoder.TERMINATOR}".encode(), await get_followup_response(
            FollowupCode.SEND_RDB
        )

    @classmethod
    async def rdb_sync(cls) -> bytes:
        with open(kvPair.rdb_file, "r") as f:
            print("[*****] parsing file...")
            rdb_content = base64.b64decode(cls.EMPTY_RDB_FILE)
            rdb_length = len(rdb_content)
            response: bytes = f"${rdb_length}\r\n".encode("utf-8") + rdb_content
            print(f"PSYNC Command response: {response}")
            return response


class Xadd(CommandProcessor):
    command = Command.XADD

    def __init__(self, message) -> None:
        self.message = message
        if not isinstance(self.message, list):
            raise Exception(f"cannot process {self.message}")
        if len(self.message) < 3:
            raise Exception(f"malformed key vals {self.message}")
        self.stream_key: str = self.message[0]
        self.stream_params: List[str] = self.message[1:]

    async def response(self) -> Tuple[bytes, bytes]:
        stream_id = self.stream_params[0]
        if stream_id == "0-0":
            return f"-ERR The ID specified in XADD must be greater than 0-0{RespCoder.TERMINATOR}".encode(), await get_followup_response(
                FollowupCode.NO_FOLLOWUP
            )
        sentry_key: str = self.stream_params[1]
        sentry_val: str = self.stream_params[2]
        return await self._update_entry(
            stream_id, sentry_key, sentry_val
        ), await get_followup_response(FollowupCode.NO_FOLLOWUP)

    async def _update_entry(
        self, stream_id: str, sentry_key: str, sentry_val: str
    ) -> bytes:
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
                    await self._notify_stream_add(self.stream_key)
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
            await self._notify_stream_add(self.stream_key)
            curr_id: str = stream_entry.stream_id
        return f"+{curr_id}{RespCoder.TERMINATOR}".encode()

    async def _notify_stream_add(self, stream_key) -> None:
        async with STREAM_LOCK:
            print(f"stream_conditionals: {STREAM_CONDITIONALS}")
            # Notify all the waiting XREAD commands for this key
            if stream_key in STREAM_CONDITIONALS:
                async with STREAM_CONDITIONALS[stream_key]:
                    print(f"Notify all waiting XREAD commands for key: {stream_key}")
                    STREAM_CONDITIONALS[stream_key].notify_all()

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
    command = Command.XRANGE

    def __init__(self, message) -> None:
        self.message = message
        self.stream_key: str = self.message[0]
        self.args: List[str] = self.message[1:] if len(self.message) >= 2 else []

    async def response(self) -> Tuple[bytes, bytes]:
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
                return RespCoder.encode(
                    flatenned_entries
                ).encode(), await get_followup_response(FollowupCode.NO_FOLLOWUP)
            else:
                return (
                    f"+Not a valid stream key{RespCoder.TERMINATOR}".encode(),
                    await get_followup_response(FollowupCode.NO_FOLLOWUP),
                )
        return "[]".encode(), await get_followup_response(FollowupCode.NO_FOLLOWUP)

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
    command = Command.XREAD

    def __init__(self, message) -> None:
        self.message: List[str] = message
        print(f"got xread args:{self.message}")
        self.is_blocking: bool = True if message[0].lower() == "block" else False
        self.block_wait_s: int = int(message[1]) // 1000 if self.is_blocking else -1
        self.stream_keys = []
        self.stream_start = []
        streams: List[str] = message[3:] if self.is_blocking else self.message[1:]
        self.stream_keys: List[str] = streams[: (len(streams) // 2)]
        self.stream_start: List[str] = streams[(len(streams) // 2) :]

    async def response(self) -> Tuple[bytes, bytes]:
        flattened_stream: List = []
        for stream_key, stream_start in zip(self.stream_keys, self.stream_start):
            flatenned_stream_for_key: List = [stream_key]

            entries: List = await self._process_one_stream(stream_key, stream_start)
            last_entry: StreamEntry = entries[-1]
            if self.is_blocking:
                ## Handle the case of blocking reads where in case of infitine timeouts - we discard all entries until next one
                ## or, wait for a specified time before grabbing next entries
                print(f"enabling blocking mode with timeout: {self.block_wait_s} sec")
                if self.block_wait_s == 0:
                    condition = None
                    async with STREAM_LOCK:
                        condition = asyncio.Condition()
                        STREAM_CONDITIONALS[stream_key] = condition
                    async with condition:
                        await condition.wait()

                else:
                    await asyncio.sleep(self.block_wait_s)
                entries.clear()
                next_seq: int = last_entry.seq + 1
                next_stream_start: str = f"{last_entry.t_ms}-{next_seq}"
                entries = await self._process_one_stream(stream_key, next_stream_start)
                if len(entries) == 0:
                    return (
                        RespCoder.NULL_BULK_STRING_BYTES,
                        await get_followup_response(FollowupCode.NO_FOLLOWUP),
                    )

            flatenned_entries: List = [entry.flattenned_entry for entry in entries]
            flatenned_stream_for_key.append(flatenned_entries)
            flattened_stream.append(flatenned_stream_for_key)
        return RespCoder.encode(flattened_stream).encode(), await get_followup_response(
            FollowupCode.NO_FOLLOWUP
        )

    async def _process_one_stream(self, stream_key: str, stream_start: str) -> List:
        print(f"processing stream_key: {stream_key} with stream_start: {stream_start}")
        if data := kvPair.get(stream_key):
            if data.type == RespDatatypes.STREAM.value and isinstance(data.value, list):
                start_range, end_range = self._find_range(stream_start, data.value)
                print(f"query range is : {start_range} to {end_range}")
                entries = [
                    entry
                    for entry in data.value
                    if self._is_in_range(entry, start_range, end_range)
                ]
                return entries
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

from typing import List


class RespCoder:
    TERMINATOR: str = "\r\n"
    NULL_BULK_STRING_BYTES: bytes = b"$-1\r\n"

    @classmethod
    def encode(cls, data) -> str:
        if isinstance(data, str):
            return f"${len(data)}{cls.TERMINATOR}{data}{cls.TERMINATOR}"
        elif isinstance(data, list):
            encoded_str: List = []
            for entry in data:
                encoded_str.append(RespCoder.encode(entry))
            joined_str: str = "".join(encoded_str)
            return f"*{len(encoded_str)}{cls.TERMINATOR}{joined_str}"
        return ""

    @classmethod
    def encode_as_simple_str(cls, data: str) -> str:
        return f"${len(data)}{cls.TERMINATOR}{data}{cls.TERMINATOR}"


PING_REQUEST_STR: str = "ping"
PING_REQUEST_BYTES: bytes = (
    f"*1{RespCoder.TERMINATOR}${len(PING_REQUEST_STR)}{RespCoder.TERMINATOR}{PING_REQUEST_STR}{RespCoder.TERMINATOR}".encode()
)
EMPTY_BYTE: bytes = b""

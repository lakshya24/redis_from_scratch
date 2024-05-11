from typing import List, LiteralString


class RespCoder:
    TERMINATOR: str = "\r\n"
    NULL_BULK_STRING_BYTES: bytes = b"$-1\r\n"

    @classmethod
    def encode(cls, data: List | str) -> str:
        if isinstance(data, str):
            return f"${len(data)}{cls.TERMINATOR}{data}{cls.TERMINATOR}"
        elif isinstance(data, list):
            encoded_str: List = []
            for entry in data:
                encoded_str.append(RespCoder.encode(entry))
            joined_str: str = "".join(encoded_str)
            return f"*{len(encoded_str)}{cls.TERMINATOR}{joined_str}"

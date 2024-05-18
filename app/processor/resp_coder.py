from typing import List, Tuple


class RespCoder:
    TERMINATOR: str = "\r\n"
    NULL_BULK_STRING_BYTES: bytes = b"$-1\r\n"

    @classmethod
    def encode(cls, data) -> str:
        if isinstance(data, int):
            return f":+{data}{cls.TERMINATOR}"
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


def parse_input_array_bytes(data: bytes) -> List[Tuple[bytes, int]]:
    commands = []
    i = 0
    n = len(data)

    while i < n:
        if data[i : i + 1] == b"*":
            command_start = i
            # Read the number of elements in the array
            end_of_line = data.index(b"\r\n", i)
            num_elements = int(data[i + 1 : end_of_line].decode())
            i = end_of_line + 2

            # Read the specified number of elements
            command = []
            for _ in range(num_elements):
                if data[i : i + 1] == b"$":
                    # Read the length of the next string
                    end_of_line = data.index(b"\r\n", i)
                    str_length = int(data[i + 1 : end_of_line].decode())
                    i = end_of_line + 2

                    # Read the string of the specified length
                    argument = data[i : i + str_length].decode()
                    command.append(argument)
                    i += str_length + 2  # Move to the end of the string and skip \r\n
            command_end = i
            commands.append((command, command_end - command_start))
    return commands


PING_REQUEST_STR: str = "ping"
PING_REQUEST_BYTES: bytes = (
    f"*1{RespCoder.TERMINATOR}${len(PING_REQUEST_STR)}{RespCoder.TERMINATOR}{PING_REQUEST_STR}{RespCoder.TERMINATOR}".encode()
)
EMPTY_BYTE: bytes = b""

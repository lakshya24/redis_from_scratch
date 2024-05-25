import os
from typing import List


class RDBFileProcessor:
    NO_CONTENT_RESPONSE: str = "$-1\r\n"

    def __init__(self, filename: str):
        self.filename = filename

    def read_key_from_file(self) -> str:
        response = ""
        if os.path.exists(self.filename):
            printable_strings = self.get_printable_words()
            # print(f"[LG****] printable chars are: {printable_strings}")
            # only get first key
            idx = printable_strings.index("@")
            key = printable_strings[idx + 1]
            # value = printable_strings[idx+2]
            response = f"*1\r\n${len(key)}\r\n{key}\r\n"
        else:
            response = self.NO_CONTENT_RESPONSE
        return response

    def read_value_from_file(self, query_key: str) -> str:
        response = ""
        if os.path.exists(self.filename):
            printable_strings = self.get_printable_words()
            # only get first key
            idx = printable_strings.index("@")
            key = printable_strings[idx + 1]
            value = printable_strings[idx + 2]
            if key == query_key:
                response = f"${len(value)}\r\n{value}\r\n"
            else:
                response = self.NO_CONTENT_RESPONSE
        else:
            response = self.NO_CONTENT_RESPONSE
        return response

    def get_printable_words(self) -> List[str]:
        with open(self.filename, "rb") as f:
            data = f.read()
            # print(data)
            printable_strings = []
            current_word = ""
            for char in data:
                if 32 <= char <= 126:  # Check if printable ASCII
                    current_word += chr(char)
                elif (
                    current_word
                ):  # Non-printable encountered, add current word (if any)
                    printable_strings.append(current_word)
                    current_word = ""  # Reset word for next printable characters
            # Add the last word if it exists
            if current_word:
                printable_strings.append(current_word)
        return printable_strings

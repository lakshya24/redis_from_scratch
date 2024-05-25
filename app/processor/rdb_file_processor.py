import os
from typing import Any, Dict, List


class RDBFileProcessor:
    NO_CONTENT_RESPONSE: str = "$-1\r\n"

    def __init__(self, filename: str):
        self.filename = filename

    def read_keys_from_file(self) -> List[Any]:
        response = ""
        keys: List[Any] = []
        if os.path.exists(self.filename):
            printable_strings = self.get_printable_words()
            # print(f"[***LGGGG****] printable chars are {printable_strings}")
            # print(f"[LG****] printable chars are: {printable_strings}")
            # only get first key
            kv_pair = self._extract_kv_pairs(printable_strings)
            print(f"[****LG****] Kv pairs are : {kv_pair}")
            keys = list(kv_pair.keys())
        return keys

    def read_value_from_file(self, query_key: str) -> str:
        response = ""
        values: List[Any] = []
        if os.path.exists(self.filename):
            printable_strings = self.get_printable_words()
            # only get first key
            kv_pair = self._extract_kv_pairs(printable_strings)
            if query_key in kv_pair:
                value = kv_pair[query_key]
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
                if char == 255:
                    break
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

    def _extract_kv_pairs(self, parsed_list: List[str]):
        start_index = parsed_list.index("@") + 1
        result_dict = {}
        for i in range(start_index, len(parsed_list), 2):
            key = parsed_list[i]
            value = parsed_list[i + 1]
            result_dict[key] = value
        return result_dict

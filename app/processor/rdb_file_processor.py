from datetime import datetime
import os
from typing import Any, Dict, List


def convert_unix_timestamp_to_datetime(timestamp, is_ms=True):
    # Convert milliseconds to seconds
    if is_ms:
        timestamp = timestamp / 1000
    # Create datetime object from Unix epoch
    dt = datetime.fromtimestamp(timestamp)
    return dt


class RDBFileProcessor:
    NO_CONTENT_RESPONSE: str = "$-1\r\n"

    def __init__(self, filename: str):
        self.filename = filename

    def read_keys_from_file(self) -> List[Any]:
        keys: List[Any] = []
        if os.path.exists(self.filename):
            kv_pair, expiry_ts = self.get_printable_words()
            # print(f"[***LGGGG****] printable chars are {printable_strings}")
            # print(f"[LG****] printable chars are: {printable_strings}")
            # only get first key
            keys = list(kv_pair.keys())
        return keys

    def read_value_from_file(self, query_key: str) -> str:
        query_ts = datetime.now()
        response = self.NO_CONTENT_RESPONSE
        if os.path.exists(self.filename):
            kv_pair, expiry_ts = self.get_printable_words()
            for kv, expiry in zip(kv_pair.keys(), expiry_ts):
                if query_key == kv:
                    value = kv_pair[query_key]
                    is_expired = True if (expiry and expiry < query_ts) else False
                    if not is_expired:
                        response = f"${len(value)}\r\n{value}\r\n"
        return response

    def get_printable_words(self) -> tuple[dict[Any, Any], list[Any]]:
        with open(self.filename, "rb") as f:
            data = f.read()
            # print(data)
            expiry_times = []
            idx = data.index(251) + 3  # index of '\xfb'
            kv_pair: Dict[Any, Any] = {}
            while data[idx] != 255:
                offset_fd = -1
                offset_fc = -1
                no_expiry_time = True
                expiry_time = None
                # check if either fd or fc are in the substring, and calculate the corresponding expiry timestamp
                if 252 in data[idx:]:
                    # print("in fc")
                    offset_fc = data[idx:].index(252)
                    # print(f"offset_fc = {offset_fc}")
                    if offset_fc > 2:
                        offset_fc = -1
                    else:  # calculate expiry time in ms
                        no_expiry_time = False
                        ms = 0
                        idx = idx + offset_fc + 1
                        for i in range(8):
                            # print(data[idx], end=" ")
                            ms += data[idx] << (i * 8)
                            idx += 1
                        # print()
                        expiry_time = convert_unix_timestamp_to_datetime(ms)
                        expiry_times.append(expiry_time)
                if 253 in data[idx:]:
                    # print("in fd")
                    offset_fd = data[idx:].index(253)
                    if offset_fd > 2:
                        offset_fd = -1
                    else:  # calculate expiry time in ms
                        no_expiry_time = False
                        sec = 0
                        idx = idx + offset_fd + 1
                        for i in range(4):
                            sec += data[idx] << (i * 8)
                            idx += 1
                        expiry_time = convert_unix_timestamp_to_datetime(sec, False)
                        expiry_times.append(expiry_time)
                if no_expiry_time:
                    expiry_times.append(None)
                # key-value pairs
                idx += 1
                # print(f"key_len index = {idx}", data[idx])
                key_len = int(data[idx])
                # print(f"key_len = {key_len}")
                idx += 1
                key = data[idx : idx + key_len].decode()
                idx += key_len
                val_len = int(data[idx])
                idx += 1
                value = data[idx : idx + val_len].decode()
                kv_pair[key] = value
                idx += val_len
                # print(f"key = {key}",f"value = {value}")

            if len(kv_pair) != len(expiry_times):
                raise Exception("mismatched kv pair and expiry timestamps ")

            return kv_pair, expiry_times

    def _extract_kv_pairs(self, parsed_list: List[str]):
        start_index = parsed_list.index("@") + 1
        result_dict = {}
        for i in range(start_index, len(parsed_list), 2):
            key = parsed_list[i]
            value = parsed_list[i + 1]
            result_dict[key] = value
        return result_dict

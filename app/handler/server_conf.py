from dataclasses import dataclass
from argparse import ArgumentParser, Namespace
from enum import Enum
import random
import string
from typing import Optional


class ServerRole(Enum):
    MASTER = "master"
    SLAVE = "slave"


@dataclass
class ServerInfo:
    port: int
    role: str
    master_address: Optional[str]
    master_port: Optional[int]
    master_replid: str
    master_repl_offset: int


def get_args_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number to listen on"
    )
    parser.add_argument(
        "--replicaof", nargs=2, type=str, required=False, help="replica conf of master"
    )

    return parser


def get_server_info() -> ServerInfo:
    parsed_args: Namespace = get_args_parser().parse_args()
    master_address, master_port, role = None, None, "master"
    master_replid, master_repl_offset = generate_random_string(40), 0
    if parsed_args.replicaof:
        master_address, master_port = parsed_args.replicaof
        role = "slave"
        try:
            master_port = int(master_port)
        except ValueError:
            print("Master's port number is not a valid integer")
    return ServerInfo(
        port=parsed_args.port,
        role=role,
        master_address=master_address,
        master_port=master_port,
        master_replid=master_replid,
        master_repl_offset=master_repl_offset,
    )


def generate_random_string(length: int = 10) -> str:
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.choice(characters) for _ in range(length))
    return random_string

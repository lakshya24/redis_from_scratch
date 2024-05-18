import asyncio
from dataclasses import dataclass, field
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
    role: ServerRole
    master_address: Optional[str]
    master_port: Optional[int]
    master_replid: str
    master_repl_offset: int
    dir: str
    dbfilename: str
    replicas: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = field(
        default_factory=list
    )


def get_args_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number to listen on"
    )
    parser.add_argument(
        "--replicaof", type=str, required=False, help="replica conf of master"
    )
    parser.add_argument(
        "--dir", type=str, default="/tmp/redis-data", help="replconf dir"
    )
    parser.add_argument(
        "--dbfilename", type=str, default="rdbfile", help="replica conf of master"
    )

    return parser


def get_server_info() -> ServerInfo:
    parsed_args: Namespace = get_args_parser().parse_args()
    master_address, master_port, role = None, None, ServerRole.MASTER
    master_replid, master_repl_offset = generate_random_string(40), 0
    if parsed_args.replicaof:
        master_address, master_port = parsed_args.replicaof.split()
        role = ServerRole.SLAVE
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
        dir=parsed_args.dir,
        dbfilename=parsed_args.dbfilename,
    )


def generate_random_string(length: int = 10) -> str:
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.choice(characters) for _ in range(length))
    return random_string

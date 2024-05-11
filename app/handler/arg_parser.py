from dataclasses import dataclass
from argparse import ArgumentParser, Namespace


@dataclass
class ServerInfo:
    port: int
    role: str
    master_address: str
    master_port: int


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
    if parsed_args.replicaof:
        master_address, master_port = parsed_args.replicaof
        role = "slave"
        try:
            master_port = int(master_port)
        except ValueError:
            print("Master's port number is not a valid integer")
    else:
        master_address, master_port = "localhost", parsed_args.port
    return ServerInfo(
        port=parsed_args.port,
        role=role,
        master_address=master_address,
        master_port=master_port,
    )

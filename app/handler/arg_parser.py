from dataclasses import dataclass
from argparse import ArgumentParser, Namespace


@dataclass
class ServerInfo:
    port: int
    role: str = "master"


def get_args_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number to listen on"
    )
    return parser


def get_args() -> ServerInfo:
    parsed_args: Namespace = get_args_parser().parse_args()
    return ServerInfo(port=parsed_args.port)

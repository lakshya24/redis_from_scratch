from dataclasses import dataclass
from argparse import ArgumentParser, Namespace


@dataclass
class RedisServerArgs:
    port: int


def get_args_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number to listen on"
    )
    return parser


def get_args() -> RedisServerArgs:
    parsed_args: Namespace = get_args_parser().parse_args()
    return RedisServerArgs(port=parsed_args.port)

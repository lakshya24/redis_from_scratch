# Uncomment this to pass the first stage
from argparse import ArgumentParser, Namespace
import socket
import asyncio

from app.handler.arg_parser import RedisServerArgs, get_args, get_args_parser
from app.handler.handler import main_with_event_loop


if __name__ == "__main__":
    # asyncio.run(main())
    server_args: RedisServerArgs = get_args()

    asyncio.run(main_with_event_loop(server_args))

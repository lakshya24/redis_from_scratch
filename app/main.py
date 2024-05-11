# Uncomment this to pass the first stage
from argparse import ArgumentParser, Namespace
import socket
import asyncio

from app.handler.server_conf import ServerInfo, get_server_info, get_args_parser
from app.handler.handler import main_with_event_loop


if __name__ == "__main__":
    # asyncio.run(main())
    server_args: ServerInfo = get_server_info()
    try:
        asyncio.run(main_with_event_loop(server_args))
    except KeyboardInterrupt:
        print("Keyboard Interrupt!")

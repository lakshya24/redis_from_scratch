import asyncio
from dataclasses import dataclass
import socket
from typing import Any, List, Optional

from app.handler.master_sync_handelr import (
    ping_master,
    psync_master,
    replconf_master,
    get_master_connection,
)
from app.handler.redis_handler import RedisServer
from app.handler.server_conf import ServerInfo
from app.processor.command import Command, CommandProcessor

REPLICAS: List = []


@dataclass
class REPLICA_CONF:
    conn: socket.socket


async def handle_response(client: socket.socket, addr, server_info: ServerInfo):
    print(f"replicas: {REPLICAS}")
    print(f"serverinfo: {server_info}")
    print(f"listening to address : {addr}")
    loop = asyncio.get_event_loop()
    while req := await loop.sock_recv(client, 1024):
        print("Received request on master....", req, client)
        command: Optional[CommandProcessor] = CommandProcessor.parse(req, server_info)
        if command:
            print(f"Got command as : {command}")
            resp, followup = await command.response()
            print(f"Got response as : {resp}")
            await loop.sock_sendall(client, resp)
            print(f"followup bytes: {followup}")
            if followup:
                print("[*****] no win followup")
                await loop.sock_sendall(client, followup)
            if server_info.role == "master":
                if command.command == Command.REPLCONF:
                    print("appending replicas")
                    REPLICAS.append(REPLICA_CONF(conn=client))
                    print(f"replicas: {REPLICAS}")
                elif command.command == Command.SET:
                    print(f"replicas: {REPLICAS}")
                    for replica in REPLICAS:
                        replica.conn.sendall(req)


async def init_as_slave_with_handshake(server_args: ServerInfo) -> None:
    """## Main method to connect as a slave

    ### Args:
        - `master_addr (str)`: master address
        - `master_port (int)`: master port
    """
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #     sock.connect((master_addr, master_port))
    #     request_msg = "ping"
    #     resp_array = ["\r\n$" + str(len(request_msg)) + "\r\n" + request_msg]
    #     request = "*" + str(len(resp_array)) + resp_array[0] + "\r\n"
    #     print(resp_array)
    #     print(request)
    #     sock.sendall(request.encode())
    #     response = sock.recv(1024)
    #     print(response)
    reader, writer = await get_master_connection(server_args.master_address, server_args.master_port)  # type: ignore
    if reader and writer:
        print("handshaking as slave...")
        await ping_master(reader, writer)
        await replconf_master(reader, writer, 0, server_args.port)
        await replconf_master(reader, writer, 1, server_args.port)
        await psync_master(reader, writer)


async def start_redis_server(config: ServerInfo) -> None:
    server = RedisServer(config)
    print(f"Server is starting on :{config.port}")
    await server.start()


async def main_with_event_loop(server_args: ServerInfo) -> None:
    print("Logs from your program will appear here!")
    if server_args.role == "slave":
        print(" Spinning up in slave mode....")
        master_addr = server_args.master_address
        master_port = server_args.master_port
        if master_addr and master_port:
            await init_as_slave_with_handshake(server_args)
        else:
            raise Exception(
                f"Slave configuration must have a valid master_adddress(found {master_addr}) and master_port(found({master_port}))"
            )

    print(" Spinning up in master mode....")
    server_socket: socket.socket = socket.create_server(
        ("localhost", server_args.port), reuse_port=True
    )
    server_socket.setblocking(False)
    server_socket.listen()
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    while True:
        print("adding new connextions here")
        conn, addr = await loop.sock_accept(server_socket)
        conn.setblocking(False)
        # create_task(client_request_handler(conn, loop, addr, server_args))
        loop.create_task(handle_response(conn, addr, server_args))


##### V1 start ####
# import threading
# def handle_response(conn,addr):
#     pong = "+PONG\r\n"
#     with conn:
#         print(f"Established connection with addr: {addr} ")
#         while True:
#             print(f"Reading data from {addr}" )
#             data = conn.recv(1024)
#             print("Data {}", data)
#             if not data:
#                 break
#             conn.send("+PONG\r\n".encode())

# def main():
#     # You can use print statements as follows for debugging, they'll be visible when running tests.
#     print("Logs from your program will appear here!")

#     server_socket: socket.socket = socket.create_server(("localhost", 6379), reuse_port=True)
#     server_socket.listen()
#     while True:
#         conn, addr = server_socket.accept()
#         thread = threading.Thread(target=handle_response, args=(conn, addr))
#         thread.start()
##### V1 end ####


#### V2 with asyncio.start_server start ####
# async def handle_connection(reader:asyncio.streams.StreamReader,writer:asyncio.streams.StreamWriter)->None:
#     print(type(reader))
#     print(type(writer))
#     while data:=await reader.read(1024):
#         print("Data {}", data)
#         if not data:
#             break
#         writer.write(b"+PONG\r\n")

# async def main():
#     print("Logs from your program will appear here!")
#     server: asyncio.Server = await asyncio.start_server(
#         handle_connection, host="localhost", port=6379, reuse_port=True
#     )
#     async with server:
#         await server.serve_forever()
#### V2 with asyncio.start_server end ####

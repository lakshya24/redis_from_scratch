import asyncio
import socket
from typing import Optional

from app.handler.server_conf import ServerInfo
from app.processor.command import PING_REQUEST_BYTES, CommandProcessor


async def handle_response(client: socket.socket, addr, server_info: ServerInfo):
    print(f"listening to address : {addr}")
    loop = asyncio.get_event_loop()
    while req := await loop.sock_recv(client, 1024):
        print("Received request", req, client)
        command: Optional[CommandProcessor] = CommandProcessor.parse(req, server_info)
        if command:
            print(f"Got command as : {command}")
            resp: bytes = await command.response()
            print(f"Got response as : {resp}")
            await loop.sock_sendall(client, resp)


async def get_master_connection(
    master_address: str, master_port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
    try:
        reader, writer = await asyncio.open_connection(master_address, master_port)
        return reader, writer
    except ConnectionError as e:
        print(f"Error connecting to master server: {e}")
        return None


async def replconf_master(reader, writer, count, port: int):
    replconf: bytes = b""
    if count == 0:
        replconf = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{str(port)}\r\n".encode()
    elif count == 1:
        replconf = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    writer.write(replconf)
    await writer.drain()
    master_response: bytes = await reader.readline()
    print(f"Received response: {master_response.decode()}")


async def ping_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    ping: bytes = PING_REQUEST_BYTES
    writer.write(ping)
    await writer.drain()
    master_response: bytes = await reader.readline()
    print(f"Received response: {master_response.decode()}")


async def init_as_slave(server_args: ServerInfo) -> None:
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
        await ping_master(reader, writer)
        await replconf_master(reader, writer, 0, server_args.port)
        await replconf_master(reader, writer, 1, server_args.port)


async def main_with_event_loop(server_args: ServerInfo) -> None:
    print("Logs from your program will appear here!")
    if server_args.role == "slave":
        print(" Spinning up in slave mode....")
        master_addr = server_args.master_address
        master_port = server_args.master_port
        if master_addr and master_port:
            await init_as_slave(server_args)
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
        conn, addr = await loop.sock_accept(server_socket)
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

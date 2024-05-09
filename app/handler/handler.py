import asyncio
import socket

from app.handler.arg_parser import RedisServerArgs
from app.processor.command import CommandProcessor, Echo, Ping

PING = "*1\r\n$4\r\nping\r\n"
PONG = "+PONG\r\n"


async def handle_response(client: socket.socket, addr):
    print(f"listening to address : {addr}")
    loop = asyncio.get_event_loop()
    while req := await loop.sock_recv(client, 1024):
        print("Received request", req, client)
        command = CommandProcessor.parse(req)
        if command:
            print(f"Got command as : {command}")
            resp = command.response()
            print(f"Got response as : {resp}")
            await loop.sock_sendall(client, resp)


async def main_with_event_loop(server_args: RedisServerArgs) -> None:
    print("Logs from your program will appear here!")
    server_socket: socket.socket = socket.create_server(
        ("localhost", server_args.port), reuse_port=True
    )
    server_socket.setblocking(False)
    server_socket.listen()
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    while True:
        conn, addr = await loop.sock_accept(server_socket)
        loop.create_task(handle_response(conn, addr))


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

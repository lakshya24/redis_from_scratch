import asyncio

from app.processor.command import PING_REQUEST_BYTES


async def get_master_connection(
    master_address: str, master_port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
    try:
        reader, writer = await asyncio.open_connection(master_address, master_port)
        return reader, writer
    except ConnectionError as e:
        print(f"Error connecting to master server: {e}")
        return None


async def psync_master(reader, writer):
    psync = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    writer.write(psync)
    await writer.drain()
    master_response: bytes = await reader.readline()
    print(f"Received response: {master_response.decode()}")


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

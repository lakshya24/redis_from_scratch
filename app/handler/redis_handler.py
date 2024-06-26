import asyncio
import logging
from typing import Optional

from app.handler.master_sync_handelr import ping_master, psync_master, replconf_master
from app.handler.server_conf import ServerInfo, ServerRole
from app.processor.command import Command, CommandProcessor
from app.processor.resp_coder import RespCoder, parse_input_array_bytes

logging.basicConfig(level=logging.INFO)

CHUNK_SIZE: int = 1024


class RedisServer:
    def __init__(self, config: ServerInfo):
        self.config: ServerInfo = config
        self.master_link = None
        self.role = config.role
        if config.role == ServerRole.SLAVE:
            self.master_link = RedisReplica(config)

    async def start(self):
        if self.config.role == ServerRole.SLAVE:
            # await self.master_link.handshake()  # type: ignore
            # Now move to a separate task instead of sequential events
            asyncio.create_task(self.master_link.handshake())  # type: ignore

        server: asyncio.Server = await asyncio.start_server(
            self.handle_client, "localhost", self.config.port
        )
        async with server:
            await server.serve_forever()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"{self.role}:Request send to {addr}")

        try:
            while requestobj := await reader.read(CHUNK_SIZE):
                # async for request in self.readlines(reader):
                if not requestobj:
                    break
                for request_str, offset in parse_input_array_bytes(requestobj):
                    await self.process_request(reader, writer, request_str)
        except ConnectionResetError:
            logging.error(f"{self.role}:Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"{self.role}:Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info("{self.role}:Connection closed")

    async def process_request(self, reader, writer, request_str):
        request = RespCoder.encode(request_str).encode()
        print(f"Request is {request}")
        req_command: Optional[CommandProcessor] = CommandProcessor.get_command(
            request, self.config
        )
        if req_command:
            response, followup = await req_command.response()
            logging.info(f"{self.role}:Sending response: {response}")
            writer.write(response)
            await writer.drain()
            if followup:
                writer.write(followup)

            if self.config.role == ServerRole.MASTER:
                if (
                    req_command.command == Command.REPLCONF
                    and "listening-port" in req_command.message
                ):
                    self.config.replicas.append((reader, writer))
                if req_command.command == Command.SET:
                    print("sending replica request...")
                    await self.propagate_to_replicas(request)

    async def propagate_to_replicas(self, request):
        print(f"Replicas are: {len(self.config.replicas)} ")
        for _, replica_writer in self.config.replicas:
            try:
                print("{self.role}:writing to replica...")
                replica_writer.write(request)
                await replica_writer.drain()
                print("{self.role}:sent to replica...")
            except Exception as e:
                logging.error(f"{self.role}:Failed to connect to replica, error: {e}")


class RedisReplica:
    def __init__(self, config: ServerInfo):
        self.config: ServerInfo = config
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.role = self.config.role.value
        self.offset = 0

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.config.master_address, self.config.master_port
        )

    async def receive_commands(self) -> None:
        print("In receiving commands....")
        assert self.reader and self.writer, "assert reader write failed..."
        addr = self.writer.get_extra_info("peername")
        try:
            while requestobj := await self.reader.read(CHUNK_SIZE):
                for request_str, offset in parse_input_array_bytes(requestobj):

                    request = RespCoder.encode(request_str).encode()
                    print(f"Request is {request}")
                    if not request:
                        print("{self.role}:no requests found....")
                        break
                    logging.info(
                        f"{self.role}:Received master request\r\n>> {request}\r\n"
                    )
                    req_command: Optional[CommandProcessor] = (
                        CommandProcessor.get_command(request, self.config)
                    )
                    print(f"parsed command {req_command}")
                    if req_command:
                        if (
                            req_command.command == Command.REPLCONF
                            and "GETACK" in req_command.message
                        ):
                            req_command.message = [str(self.offset)] + [
                                *req_command.message
                            ]
                            response, followup = await req_command.response()
                            logging.info(f"{self.role}:Sending response: {response}")
                            ## respond to master only for ACKs
                            self.writer.write(response)
                            await self.writer.drain()
                        else:
                            response, followup = await req_command.response()
                        self.offset += offset

        except ConnectionResetError:
            logging.error(f"{ self.role}:Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"{ self.role}:Error handling client {addr}: {e}")

    async def handshake(self):
        if not self.writer or not self.reader:
            await self.connect()
            print("handshaking as slave...")
            assert self.reader and self.writer
            await ping_master(self.reader, self.writer)  # type: ignore
            await replconf_master(self.reader, self.writer, 0, self.config.port)  # type: ignore
            await replconf_master(self.reader, self.writer, 1, self.config.port)  # type: ignore
            await psync_master(self.reader, self.writer)

            print(" now kicking of reads...")
            ## Read rdb file first as partial commands
            res: bytes = await self.reader.readuntil(b"\r\n")
            await self.reader.readexactly(int(res[1:-2]))
            logging.info(f"{self.role}:PSYNC2")
            logging.info(f"{self.role}:Handshake completed")
            ## start recieving commands
            await self.receive_commands()

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

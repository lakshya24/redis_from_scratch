import asyncio
import logging
from typing import  Optional

from app.handler.master_sync_handelr import ping_master, psync_master, replconf_master
from app.handler.server_conf import ServerInfo, ServerRole
from app.processor.command import Command, CommandProcessor
from app.processor.resp_coder import RespCoder, parse_input_array_bytes

logging.basicConfig(level=logging.INFO)

CHUNK_SIZE:int = 1024



class RedisServer:
    def __init__(self, config: ServerInfo):
        self.config: ServerInfo = config
        self.master_link = None
        if config.role == ServerRole.SLAVE:
            self.master_link = RedisReplica(config)

    async def start(self):
        if self.config.role == ServerRole.SLAVE:
            #await self.master_link.handshake()  # type: ignore
            # Now move to a separate task instead of sequential events
            asyncio.create_task(self.master_link.handshake()) # type: ignore

        server: asyncio.Server = await asyncio.start_server(
            self.handle_client, "localhost", self.config.port
        )
        async with server:
            await server.serve_forever()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"Request send to {addr}")

        try:
            while requestobj := await reader.read(CHUNK_SIZE):
                # async for request in self.readlines(reader):
                if not requestobj:
                    break
                for request_str in parse_input_array_bytes(requestobj):
                    request = RespCoder.encode(request_str).encode()
                    print(f"Request is {request}")
                    req_command: Optional[CommandProcessor] = CommandProcessor.parse(
                        request, self.config
                    )
                    if req_command:
                        response, followup = await req_command.response()
                        logging.info(f"Sending response: {response}")
                        writer.write(response)
                        await writer.drain()
                        if followup:
                            print("[*****] no win followup")
                            writer.write(followup)

                        if self.config.role == ServerRole.MASTER:

                            if (
                                req_command.command == Command.REPLCONF
                                and "listening-port" in req_command.message
                            ):
                                print("[***] INITIALIZAING REPLICAS.....")
                                self.config.replicas.append((reader, writer))
                            if req_command.command == Command.SET:
                                print("sending replica request...")
                                await self.propagate_to_replicas(request)

        # while req := await loop.sock_recv(client, 1024):
        # print("Received request on master....", req, client)
        # command: Optional[CommandProcessor] = CommandProcessor.parse(req, server_info)
        # if command:
        #     print(f"Got command as : {command}")
        #     resp, followup = await command.response()
        #     print(f"Got response as : {resp}")
        #     await loop.sock_sendall(client, resp)
        #     print(f"followup bytes: {followup}")
        #     if followup:
        #         print("[*****] no win followup")
        #         await loop.sock_sendall(client, followup)
        #     if server_info.role == "master":
        #         if command.command ==  Command.REPLCONF:
        #             print("appending replicas")
        #             REPLICAS.append(REPLICA_CONF(conn=client))
        #             print(f"replicas: {REPLICAS}")
        #         elif command.command ==  Command.SET:
        #             print(f"replicas: {REPLICAS}")
        #             for replica in REPLICAS:
        #                 replica.conn.sendall(req)

        except ConnectionResetError:
            logging.error(f"Connection reset by peer: {addr}")
        except Exception as e:
            logging.error(f"Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info("Connection closed")

    async def propagate_to_replicas(self, request):
        print(f"Replicas are: {len(self.config.replicas)} ")
        for _, replica_writer in self.config.replicas:
            try:
                print("writing to replica...")
                replica_writer.write(request)
                await replica_writer.drain()
                print("sent to replica...")
            except Exception as e:
                logging.error(f"Failed to connect to replica, error: {e}")


class RedisReplica:
    def __init__(self, config: ServerInfo):
        self.config: ServerInfo = config
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.role = self.config.role.value

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
                for request_str in parse_input_array_bytes(requestobj):
                    request = RespCoder.encode(request_str).encode()
                    print(f"Request is {request}")
                    if not request:
                        print("no requests found....")
                        break
                    logging.info(f"{self.role}:Received master request\r\n>> {request}\r\n")
                    req_command: Optional[CommandProcessor] = CommandProcessor.parse(
                        request, self.config
                    )
                    print(f"parsed command {req_command}")
                    if req_command:
                        response, followup = await req_command.response()
                        logging.info(f"Sending response: {response}")
                        # self.writer.write(response)
                        # await self.writer.drain()
        
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

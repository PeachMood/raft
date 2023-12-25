from asyncio import get_event_loop, Transport, BaseProtocol, Server as AsyncioServer
from logging import Logger, getLogger, NOTSET, basicConfig
from socket import AF_INET

from .protocols import PeerProtocol, ClientProtocol, RaftProtocol
from .utils import split_address


class Server:
    __address: str
    __network: list[str]
    __logger: Logger

    __peer_server: Transport
    __client_server: AsyncioServer

    __peer_protocol: PeerProtocol
    __raft_protocol: RaftProtocol

    def __init__(self, address: str, network: list[str]):
        self.__address = address
        self.__network = network
        self.__create_logger()
        self.__create_protocols()

    def __create_logger(self):
        basicConfig(format='%(levelname)s [%(name)s] %(message)s', level=NOTSET)
        self.__logger = getLogger(f'{self.__address}')

    def __create_protocols(self):
        self.__peer_protocol = PeerProtocol(self.__network, self.__logger)
        self.__raft_protocol = RaftProtocol(self.__address, self.__network, self.__logger)
        self.__peer_protocol.protocol = self.__raft_protocol
        self.__raft_protocol.protocol = self.__peer_protocol

    def __create_client_protocol(self) -> BaseProtocol:
        client_protocol = ClientProtocol(self.__logger)
        client_protocol.protocol = self.__raft_protocol
        return client_protocol

    async def __start(self):
        loop = get_event_loop()
        (host, port) = split_address(self.__address)
        self.__peer_server, _ = await loop.create_datagram_endpoint(lambda: self.__peer_protocol, local_addr=(host, port), family=AF_INET)
        self.__client_server = await loop.create_server(self.__create_client_protocol, host=host, port=port, family=AF_INET)
        self.__logger.info(f'Started serving')

        for address in self.__network:
            await loop.create_datagram_endpoint(lambda: self.__peer_protocol, remote_addr=split_address(address), family=AF_INET)

    def run(self):
        loop = get_event_loop()
        loop.run_until_complete(self.__start())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.__peer_protocol.close()
            self.__peer_server.close()
            self.__client_server.close()

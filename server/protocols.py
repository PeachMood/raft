import pickle
from logging import Logger
from asyncio import DatagramTransport, DatagramProtocol, Protocol, Transport
from typing import Any

from .state import State, Follower
from .utils import split_address, join_address


class PeerProtocol(DatagramProtocol):
    protocol: Any
    __network: list[str]
    __logger: Logger
    __connections: dict[str, DatagramTransport]

    def __init__(self, network: list[str], logger: Logger):
        self.__network = network
        self.__logger = logger
        self.__connections = {}

    def connection_made(self, transport: DatagramTransport):
        address = transport.get_extra_info('peername')
        if address:
            self.__connections[join_address(address)] = transport

    def connection_lost(self, exception: Exception):
        if exception:
            self.__logger.exception(exception)
        self.__connections = {address: connection for address, connection in self.__connections.items() if
                              connection.is_closing()}

    def datagram_received(self, data: bytes, address: tuple[str, int]):
        message = pickle.loads(data)
        self.__logger.info(f'Received message from peer: {message}')
        self.protocol.peer_message_received(message)

    def send_to(self, message: dict[str, Any], address: str):
        data = pickle.dumps(message)
        transport = self.__connections.get(address)
        transport.sendto(data, split_address(address))

    def broadcast(self, message: dict[str, Any]):
        for address in self.__connections:
            self.send_to(message, address)

    def close(self):
        for connection in self.__connections.values():
            connection.close()


class ClientProtocol(Protocol):
    protocol: Any
    __logger: Logger
    __transport: Transport

    def __init__(self, logger: Logger):
        self.__logger = logger

    def connection_made(self, transport: Transport):
        address = transport.get_extra_info('peername')
        self.__logger.info(f'Established connection with {address}')
        self.__transport = transport

    def connection_lost(self, exception: Exception):
        address = self.__transport.get_extra_info('peername')
        self.__logger.exception(f'Lost connection with {address}')

    def data_received(self, data: bytes):
        message = pickle.loads(data)
        self.__logger.info(f'Received message from client: {message}')
        self.protocol.client_message_received(message, self)

    def respond(self, message: dict[str, Any]):
        data = pickle.dumps(message)
        self.__transport.write(data)
        self.__transport.close()


class RaftProtocol:
    protocol: PeerProtocol
    state: State
    __logger: Logger

    def __init__(self, address: str, network: list[str], logger: Logger):
        self.__logger = logger
        self.state = Follower({'address': address, '__network': network, 'protocol': self, 'logger': logger})

    def change_state(self, state: State):
        self.state.teardown()
        self.__logger.info(f'Changed state to {state.__class__.__name__}')
        self.state = state

    def peer_message_received(self, message: dict[str, Any]):
        address = message['address']
        self.state.peer_message_received(message, address)

    def client_message_received(self, message: dict[str, Any], client: ClientProtocol):
        self.state.client_message_received(message, client)

    def respond_to_client(self, message: dict[str, Any], client: ClientProtocol):
        client.respond(message)

    def send_to_peer(self, message: dict[str, Any], address: str):
        self.protocol.send_to(message, address)

    def broadcast_to_peers(self, message: dict[str, Any]):
        self.protocol.broadcast(message)

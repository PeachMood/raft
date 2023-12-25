import json
import pickle
import random
import socket

from server.utils import split_address


class Client:
    __network: list[tuple[str, int]] | None
    __server_address: tuple[str, int] | None
    __socket: socket

    def __init__(self, network: list[tuple[str, int]]):
        self.__network = network
        self.__server_address = None

    def __get_server_address(self):
        if not self.__server_address:
            self.__server_address = random.choice(self.__network)
        return self.__server_address

    def __get_response(self):
        buffer = bytes()
        while True:
            block = self.__socket.recv(128)
            if not block:
                break
            buffer += block
        self.__socket.close()
        return pickle.loads(buffer)

    def __make_attempt(self, data: bytes):
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = self.__get_server_address()
        self.__socket.connect(server_address)
        self.__socket.send(data)
        response = self.__get_response()
        if response['type'] == 'redirect':
            self.__server_address = split_address(response['leader'])
            response = self.__make_attempt(data)
        return response

    def _send_request(self, message: dict[str, any], retry_attempts=1):
        data = pickle.dumps(message)
        response = None
        for attempt in range(retry_attempts):
            response = self.__make_attempt(data)
            if response['success']:
                break
        return response

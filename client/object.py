from typing import Generic, TypeVar, Any

from server.utils import split_address
from .client import Client

K = TypeVar('K')
V = TypeVar('V')


class ReplicatedObject(Client):

    def _get_state(self):
        response = self._send_request({'type': 'get'})
        return response['state']

    def _replicate_state(self, command: str, *arguments: Any):
        self._send_request({'type': 'replicate', 'command': command, 'arguments': arguments}, 3)


class ReplicatedDict(Generic[K, V], ReplicatedObject):
    __data: dict[K, V]

    def __init__(self, network: list[str]):
        super().__init__([split_address(address) for address in network])
        self.__data = {}
        self.__refresh()

    def __refresh(self):
        self.__data = self._get_state()

    def __getitem__(self, key):
        self.__refresh()
        return self.__data[key]

    def __setitem__(self, key, value):
        self._replicate_state('set', key, value)

    def __delitem__(self, key):
        self.__refresh()
        self._replicate_state('delete', key)

    def __repr__(self):
        self.__refresh()
        return super().__repr__()



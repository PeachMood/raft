from asyncio import get_event_loop, TimerHandle
from enum import Enum
from logging import Logger
from random import randrange
from statistics import median_low
from typing import Any

from .log import Log, Entry


class StateName(Enum):
    Follower = 'follower'
    Candidate = 'candidate'
    Leader = 'leader'


class State:
    # Server address
    _address: str
    # Other servers addresses
    _network: list[str]
    # Protocol for sending/handling requests
    _protocol: Any
    # Logger
    _logger: Logger

    # Leader address
    _leader: str
    # Latest term server has seen
    _current_term: int
    # Candidate address that received vote in current term
    _voted_for: str | None
    # Log entries
    _log: Log[Entry]

    def __init__(self, state: dict[str, Any]):
        self._address = state['address']
        self._network = state['__network']
        self._logger = state['logger']
        self._leader = state.get('leader') or None
        self._protocol = state.get('protocol')
        self._current_term = state.get('current_term') or 0
        self._voted_for = state.get('voted_for') or None
        self._log = state.get('log') or Log()

    def __get_state(self):
        state = {
            'address': self._address,
            '__network': self._network,
            'protocol': self._protocol,
            'logger': self._logger,
            'leader': self._leader,
            'current_term': self._current_term,
            'voted_for': self._voted_for,
            'log': self._log,
        }
        return state

    def change_state(self, state_name: StateName):
        data = self.__get_state()
        if state_name == StateName.Follower:
            state = Follower(data)
        elif state_name == StateName.Candidate:
            state = Candidate(data)
        else:
            state = Leader(data)
        self._protocol.change_state(state)

    def teardown(self):
        pass

    def peer_message_received(self, message: dict[str, Any], address: str):
        if message['term'] > self._current_term:
            self._current_term = message['term']
            if not type(self) is Follower:
                self.change_state(StateName.Follower)
                self._protocol.state.peer_message_received(message, address)

    def client_message_received(self, message: dict[str, Any], protocol: Any):
        pass


class Follower(State):
    __election_timer: TimerHandle | None

    def __init__(self, state: dict[str, Any]):
        super().__init__(state)
        self._voted_for = None
        self.__election_timer = None
        self.__restart_election_timer()

    def __restart_election_timer(self):
        if self.__election_timer:
            self.__election_timer.cancel()
        timeout = randrange(1, 4) * 5
        loop = get_event_loop()
        self.__election_timer = loop.call_later(timeout, self.change_state, StateName.Candidate)

    def teardown(self):
        self.__election_timer.cancel()

    def peer_message_received(self, message: dict[str, Any], address: str):
        super().peer_message_received(message, address)
        if message['type'] == 'append_entries':
            self.__peer_append_entries_received(message, address)
        elif message['type'] == 'request_vote':
            self.__request_vote_received(message, address)
        else:
            self._logger.exception(f'[{StateName.Follower}] Received unrecognized message from peer')

    def __peer_append_entries_received(self, message: dict[str, Any], leader: str):
        term_is_current = message['term'] >= self._current_term
        prev_log_term_match = self._log.last_index >= message['prev_log_index'] and self._log[message['prev_log_index']].term == message['prev_log_term']
        success = term_is_current and prev_log_term_match
        if term_is_current:
            self.__restart_election_timer()
        if success:
            self._log.append_entries(message['entries'], message['prev_log_index'])
            self._log.commit(message['leader_commit'])
        self._leader = leader
        self.__send_peer_append_entries_response(success)

    def __send_peer_append_entries_response(self, success: bool):
        append_entries_response = {
            'type': 'append_entries_response',
            'address': self._address,
            'success': success,
            'term': self._current_term,
            'last_index': self._log.last_index
        }
        self._protocol.send_to_peer(append_entries_response, self._leader)

    def __request_vote_received(self, message: dict[str, Any], candidate: str):
        term_is_current = message['term'] >= self._current_term
        can_vote = not self._voted_for or self._voted_for == candidate
        log_is_up_to_date = \
            message['last_log_term'] > self._log.last_term or \
            (message['last_log_term'] == self._log.last_term and message['last_log_index'] >= self._log.last_index)
        vote_granted = term_is_current and can_vote and log_is_up_to_date
        if vote_granted:
            self._voted_for = candidate
            self.__restart_election_timer()
        self.__send_request_vote_response(vote_granted, candidate)

    def __send_request_vote_response(self, vote_granted: bool, candidate: str):
        request_vote_response = {
            'type': 'request_vote_response',
            'address': self._address,
            'term': self._current_term,
            'vote_granted': vote_granted
        }
        self._protocol.send_to_peer(request_vote_response, candidate)

    def client_message_received(self, message: dict[str, Any], protocol: Any):
        self.__send_client_redirect(protocol)

    def __send_client_redirect(self, client: Any):
        redirect = {'type': 'redirect', 'leader': self._leader}
        client.respond(redirect)


class Candidate(Follower):
    # Number of votes for current candidate
    __votes: int

    def __init__(self, state: dict[str, Any]):
        super().__init__(state)
        self._current_term += 1
        self._vote_for = self._address
        self.__votes = 1
        self.__send_request_vote()

    def __send_request_vote(self):
        request_vote = {
            'type': 'request_vote',
            'address': self._address,
            'term': self._current_term,
            'last_log_index': self._log.last_index,
            'last_log_term': self._log.last_term,
        }
        self._protocol.broadcast_to_peers(request_vote)

    def __request_vote_response_received(self, message: dict[str, Any]):
        if message['vote_granted']:
            self.__votes += 1
        majority = self.__votes > (len(self._network) + 1) / 2
        if majority:
            self.change_state(StateName.Leader)

    def peer_message_received(self, message: dict[str, Any], address: str):
        super().peer_message_received(message, address)
        if message['type'] == 'append_entries':
            self.change_state(StateName.Follower)
            self._protocol.peer_message_received(message)
        elif message['type'] == 'request_vote_response':
            self.__request_vote_response_received(message)
        else:
            self._logger.exception(f'[{StateName.Candidate}] Received unrecognized message from peer')


class Leader(State):
    # Index of the next log entry to send to other servers
    __next_index: dict[str, int]
    # Index of highest log entry known to be replicated on server
    __match_index: dict[str, int]
    # Timer for sending append entries rpc
    __append_entries_timer: TimerHandle
    # Clients waiting for response
    __waiting_clients: dict[int, Any]

    def __init__(self, state: dict[str, Any]):
        super().__init__(state)
        self._leader = self._address
        self.__next_index = self.__init_next_index()
        self.__match_index = self.__init_match_index()
        self.__waiting_clients = {}
        self.__send_peer_append_entries()

    def __init_next_index(self):
        next_index = {}
        for address in self._network:
            next_index[address] = self._log.last_index + 1
        next_index[self._address] = self._log.last_index + 1
        return next_index

    def __init_match_index(self):
        match_index = {}
        for address in self._network:
            match_index[address] = 0
        match_index[self._address] = 0
        return match_index

    def __send_peer_append_entries(self):
        for address in self._network:
            prev_log_index = min(self._log.last_index, self.__next_index[address] - 1)
            prev_log_entry = self._log[prev_log_index]
            entries = self._log[self.__next_index[address]: self.__next_index[address] + 100]
            append_entries = {
                'type': 'append_entries',
                'address': self._address,
                'term': self._current_term,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_entry.term,
                'entries': entries,
                'leader_commit': self._log.commit_index
            }
            self._protocol.send_to_peer(append_entries, address)
        timeout = randrange(1, 4) * 1
        loop = get_event_loop()
        self.__append_entries_timer = loop.call_later(timeout, self.__send_peer_append_entries)

    def teardown(self):
        self.__append_entries_timer.cancel()
        for client in self.__waiting_clients.values():
            client.respond({'type': 'result', 'success': False})

    def client_message_received(self, message: dict[str, Any], protocol: Any):
        if message['type'] == 'get':
            self.__client_get_received(protocol)
        elif message['type'] == 'replicate':
            self.__client_replicate_received(message, protocol)
        else:
            self._logger.exception(f'[{StateName.Leader}] Received unrecognized message from client')

    def __client_get_received(self, client: Any):
        state = self._log.state_machine.data.copy()
        result = {'type': 'result', 'success': True, 'state': state}
        client.respond(result)

    def __client_replicate_received(self, message: dict[str, Any], client: Any):
        entry = Entry(self._current_term, message['command'], message['arguments'])
        self._log.append_entries([entry])
        self.__waiting_clients[self._log.last_index] = client
        message = {'success': True, 'last_index': self._log.commit_index}
        self.__peer_append_entries_response_received(message, self._address)

    def peer_message_received(self, message: dict[str, Any], address: str):
        super().peer_message_received(message, address)
        if message['type'] == 'append_entries_response':
            self.__peer_append_entries_response_received(message, address)
        else:
            self._logger.exception(f'[{StateName.Leader}] Received unrecognized message from peer')

    def __peer_append_entries_response_received(self, message: dict[str, Any], follower: str):
        if message['success']:
            self.__match_index[follower] = message['last_index']
            self.__next_index[follower] = message['last_index'] + 1
            self.__match_index[self._address] = self._log.last_index
            self.__next_index[self._address] = self._log.last_index + 1
            majority_index = median_low(self.__match_index.values())
            self._log.commit(majority_index)
            self._logger.info(f'State machine: {self._log.state_machine.data}')
            self.__send_client_replicate_response()
        else:
            self.__next_index[follower] = max(0, self.__next_index[follower] - 1)

    def __send_client_replicate_response(self):
        clients_to_delete = []
        for index, client in self.__waiting_clients.items():
            if index <= self._log.commit_index:
                client.respond({'type': 'result', 'success': True})
                clients_to_delete.append(index)
        for index in clients_to_delete:
            del self.__waiting_clients[index]

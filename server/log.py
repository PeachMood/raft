from collections import UserList, UserDict
from typing import Any


class Entry:
    term: int
    command: str
    arguments: Any

    def __init__(self, term: int, command: str, arguments: Any = None):
        self.term = term
        self.command = command
        self.arguments = arguments

    def __repr__(self):
        return f'(term: {self.term}, command: {self.command}, args: {self.arguments})'


class StateMachine(UserDict):
    last_applied: int

    def __init__(self):
        super().__init__({})
        self.last_applied = 0

    def apply(self, entries: list[Entry], end: int):
        not_applied_entries = entries[self.last_applied + 1:end + 1]
        for entry in not_applied_entries:
            self.last_applied += 1
            if entry.command == 'set':
                (key, value) = entry.arguments
                self.data[key] = value
            elif entry.command == 'delete':
                (key) = entry.arguments
                del self.data[key]


class Log(UserList):
    last_index: int
    last_term: int
    commit_index: int
    state_machine: StateMachine

    def __init__(self):
        super().__init__([Entry(0, 'no_op')])
        self.last_index = len(self.data) - 1
        self.last_term = self.data[self.last_index].term
        self.commit_index = 0
        self.state_machine = StateMachine()

    def __getitem__(self, index: Any):
        if type(index) is slice:
            start = index.start
            stop = index.stop
            return self.data[start:stop:index.step]
        elif type(index) is int:
            return self.data[index]

    def commit(self, leader_commit: int):
        if leader_commit <= self.commit_index:
            return
        self.last_index = len(self.data) - 1
        self.commit_index = min(leader_commit, self.last_index)
        self.state_machine.apply(self.data, self.commit_index)

    def append_entries(self, entries: list[Entry], index: int = None):
        if not index:
            index = self.last_index
        if self.last_index > index:
            self.data = self.data[:index] + entries
        else:
            self.data += entries
        self.last_index = len(self.data) - 1
        self.last_term = self.data[self.last_index].term

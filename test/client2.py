from client import ReplicatedDict

repl_dict = ReplicatedDict(['127.0.0.1:8080', '127.0.0.1:8081', '127.0.0.1:8082'])

print(repl_dict['hello'])

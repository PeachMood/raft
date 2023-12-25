from server import Server

server = Server('127.0.0.1:8080', ['127.0.0.1:8081', '127.0.0.1:8082'])
server.run()



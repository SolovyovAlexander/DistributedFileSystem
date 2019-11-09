import socket
import sys


class Request:
    def __init__(self, request_type: str = 'PING'):
        self.request_type = request_type

    def execute(self, connect):
        pass


class Response:
    def __init__(self, request: Request):
        self.request = request



class MessageProvider:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None
        try:
            self.sock = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
            self.sock.connect((host, port))
        except ConnectionError:
            print("Can't connect to Naming Server", file=sys.stderr)

    def connection_required(self, func):
        def wrapped(self, msg: Request):
            if self.sock is None:
                return None
            else:
                return func

        wrapped(self, func)

    @connection_required
    def send(self, msg: Request):
        self.sock.send(msg.request_type.encode(), 16)
        msg.execute(self.sock)

    @connection_required
    def receive(self, response: Response):
        self.sock.send(response.request.type.encode(), 16)
        response.request.execute(self.sock)


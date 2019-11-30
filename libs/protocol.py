from libs.request_codes import CODES
import json
import socket
from struct import unpack, pack



class Response:
    def __init__(self, status: int, data: dict):
        self.status = status
        self.data = data

    def to_payloads(self) -> list:
        data = json.dumps(self.data).encode()
        payload = [
            pack('I', self.status),
            pack('I', len(data)),
            data
        ]
        return payload


class Request:
    def __init__(self, params: list, command: str = ''):
        self.request_type = command
        self.params_count = 0
        self.params = params
        self.validated_params = None

    def execute(self, response: Response):
        return

    def validate_params(self):
        self.validated_params = {}

    def to_payloads(self) -> list:
        self.validate_params()
        data = json.dumps(self.validated_params).encode()
        try:
            code = CODES[self.request_type]
        except KeyError:
            code = 228
        payload = [
            pack('I', code),
            pack('I', len(data)),
            data
        ]
        return payload


class MessageProvider:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None
        self.sock = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send(self, request: Request) -> Response:

        request_type, data_length, data = request.to_payloads()
        self.sock.send(request_type)
        ok, = unpack('I', self.sock.recv(4))
        assert ok == 0
        self.sock.send(data_length)
        ok, = unpack('I', self.sock.recv(4))
        assert ok == 0
        self.sock.send(data)
        ok, = unpack('I', self.sock.recv(4))
        assert ok == 0

        status, = unpack('I', self.sock.recv(4))
        self.sock.send(pack('I', 0))
        length, = unpack('I', self.sock.recv(4))
        self.sock.send(pack('I', 0))
        data = self.sock.recv(length).decode()
        self.sock.send(pack('I', 0))

        data = json.loads(data)

        return Response(status=status, data=data)





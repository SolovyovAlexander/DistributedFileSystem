import json
import socket
from struct import unpack, pack


class Response:
    STATUSES = {
        200: 'OK',
        201: 'CREATED',
        203: 'NEED CONFIRMATION',
        400: 'BAD REQUEST',
        404: 'NOT FOUND',
        500: 'NO AVAILABLE REPLICAS'
    }

    def __init__(self, status: int, data: dict):
        self.status = status
        self.data = data

    def message(self):
        return Response.STATUSES[self.status]

    def to_payloads(self) -> list:
        data = json.dumps(self.data).encode()
        payload = [
            pack('I', self.status),
            pack('I', len(data)),
            data
        ]
        return payload


class Request:
    REQUESTS = {
        # CLIENT -> NAMING
        'CLIENT_INIT': {},
        'FILE_CREATE': {'file_name': str, 'dir_path': list, 'force': bool},
        'FILE_UPLOAD': {'file_name': str, 'dir_path': list},
        'FILE_DOWNLOAD': {'file_name': str, 'dir_path': list},
        'FILE_DELETE': {'file_name': str, 'dir_path': list},
        'FILE_INFO': {'file_name': str, 'dir_path': list},
        'FILE_MOVE': {'file_name': str, 'dir_path': list, 'destination_path': list},
        'FILE_COPY': {'file_name': str, 'dir_path': list, 'destination_path': list},

        'DIR_CREATE': {'dir_name': str, 'dir_path': list},
        'DIR_DELETE': {'dir_name': str, 'dir_path': list},
        'DIR_OPEN': {'dir_name': str, 'dir_path': list},
        'DIR_READ': {'dir_name': str, 'dir_path': list},
        'RESET': {},

        # NAMING -> STORAGE
        'STORAGE_FILE_CREATE': {'file_name': str, 'dir_path': list},
        'STORAGE_FILE_DELETE': {'file_name': str, 'dir_path': list},
        'STORAGE_FILE_MOVE':   {'file_name': str, 'dir_path': list, 'destination_path': list},
        'STORAGE_FILE_COPY':   {'file_name': str, 'dir_path': list, 'destination_path': list},
        'STORAGE_FILE_INFO':   {'file_name': str, 'dir_path': list},
        'STORAGE_FOLDER_CREATE': {'dir_name': str, 'dir_path': list},
        'STORAGE_FOLDER_DELETE': {'dir_name': str, 'dir_path': list},
        'RESET_REPLICA': {},
        'CONFIRM_FILE_DOWNLOADED': {'hash': str, 'file_name': str, 'dir_path': list},



        # STORAGE -> NAMING
        'STORAGE_INIT': {},
        'GET_REPLICA_SET': {},
        'CONFIRM_FILE_UPLOADED': {'hash': str, 'file_name': str, 'dir_path': list},

        # CLIENT -> STORAGE
        'FILE_DOWNLOAD_DIRECT': {'file_name': str, 'dir_path': list},
        'FILE_UPLOAD_DIRECT': {'file_name': str, 'dir_path': list},

        # STORAGE -> STORAGE (like CLIENT)
        'FILE_REPLICATE': {'file_name': str, 'dir_path': list}
    }

    def __init__(self, command: str, params: dict):
        if command not in Request.REQUESTS:
            raise ValueError('No request of type [%s] defined in protocol' % command)
        self.request_type = command
        self.validated_params = self.validate_params(params)

    @staticmethod
    def get_request_code(command: str) -> int:
        keys = list(Request.REQUESTS.keys())
        for index in range(len(keys)):
            if keys[index] == command:
                return index
        return -1

    @staticmethod
    def get_instruction_by_code(code: int):
        keys = list(Request.REQUESTS.keys())
        return keys[code]

    def validate_params(self, params: dict) -> dict:
        pattern = Request.REQUESTS[self.request_type]
        for p_name, p_type in pattern.items():
            if p_name not in params:
                raise ValueError(
                    'Request [%s] requires parameter [%s]. (passed %s)' % (self.request_type, p_name, params.__str__()))
            if type(params[p_name]) != p_type:
                raise ValueError('Request [%s] requires parameter [%s] of type %s, got %s)' %
                                 (self.request_type, p_name, p_type, type(params[p_name])))
        return params

    def to_payloads(self) -> list:
        data = json.dumps(self.validated_params).encode()
        code = Request.get_request_code(self.request_type)
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
        self.sock = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

    def connect(self):
        self.sock.connect((self.host, self.port))

    def send(self, request: Request) -> Response:
        print('SENDING INSTRUCTION [\033[1;36;6m%s %s\033[0;0;0m] ...' % (request.request_type, request.validated_params.__str__()))
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
        response = Response(status=status, data=data)
        print('  - STATUS: %d [%s]' % (response.status, response.message()))
        print('  - DATA: ', response.data)
        return response

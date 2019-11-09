import socket
import struct
import sys


class Request:
    def __init__(self, request_type: str = 'PING'):
        self.request_type = request_type

    def execute(self, connect):
        pass


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

    # def connection_required(self, func):
    #     def wrapped(self, msg: Request):
    #         if self.sock is None:
    #             return None
    #         else:
    #             return func
    #
    #     wrapped(self, func)
    #
    # @connection_required
    def send_msg(self, request: str):
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(request.encode())) + request.encode()
        self.sock.sendall(msg)

    def recvall(self, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = b''
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    # @connection_required
    def recv_msg(self):
        # Read message length and unpack it into an integer
        raw_msglen = self.recvall(4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # Read the message data
        return self.recvall(msglen)


class Response:
    def __init__(self, request: Request):
        self.request = request


class Initialize(Request):
    def __init__(self, request_type='init'):
        super().__init__(request_type)

    def execute(self, connect: MessageProvider):
        connect.send_msg(self.request_type)
        ss_address = connect.recv_msg().decode()  # address of storage server
        ss_port = connect.recv_msg() .decode() # port of storage server
        print(ss_address, ss_port)
        # ss_conection = MessageProvider(ss_address, int(ss_port))



class FileWrite(Request):
    def __init__(self, filename: str, request_type: str = 'fwrite'):
        super().__init__(request_type)
        self.filename = filename

    def execute(self, connect):
        pass

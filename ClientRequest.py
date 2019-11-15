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

    def execute(self, connect_ns: MessageProvider):
        ss_address, ss_port = self.ns_connection(connect_ns)
        connect_ss = self.ss_connection(ss_address, int(ss_port))

    def ns_connection(self, connect_ns):
        connect_ns.send_msg(self.request_type)
        print('mess sended to ns:', self.request_type)
        ss_address = connect_ns.recv_msg().decode()  # address of storage server
        ss_port = connect_ns.recv_msg().decode()  # port of storage server
        return ss_address, ss_port

    def ss_connection(self, ss_address, ss_port):
        connect_ss = MessageProvider(ss_address, int(ss_port))
        connect_ss.send_msg(self.request_type)
        answer = connect_ss.recv_msg().decode()
        if answer[:2] == 'Ok':
            print(answer)
        elif answer == 'Error':
            print('Error: Something goes wrong')
        else:
            print('There is no answer')
        connect_ss.sock.close()


class FileWrite(Request):
    def __init__(self, filename: str, request_type: str = 'fwrite'):
        super().__init__(request_type)
        self.filename = filename

    def execute(self, connect):
        pass


class FileCreate(Request):
    def __init__(self, filename: str, request_type: str = 'fcreate'):
        super().__init__(request_type)
        self.filename = filename

    def execute(self, connect_ns: MessageProvider):
        status = self.ns_connection(connect_ns)

    def ns_connection(self, connect_ns):
        connect_ns.send_msg(self.request_type + ' ' + self.filename)
        print('mess sended to ns:', self.request_type, self.filename)
        status = connect_ns.recv_msg().decode()
        print(status)
        if status == 'The file already exists. Do you want to delete it and create a new one? y/n':
            client_answer = input()
            if client_answer == 'y' or client_answer == 'n':
                connect_ns.send_msg(client_answer)
            else:
                connect_ns.send_msg('-1')
            print(connect_ns.recv_msg().decode())
        return status

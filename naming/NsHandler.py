import socket
import struct
import sys


class Request:
    def __init__(self, request_type: str = 'PING'):
        self.request_type = request_type

    def execute(self, connect):
        pass


def send_instruction(instruction, host, port):
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

    s.connect((host, port))

    send_msg(s, instruction.encode())

    print("Sending in progress..", instruction)

    s.close()


def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


class ClientConnection:
    def __init__(self, sock):
        self.sock = sock

    def recvall(self, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = b''
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    def recv_msg(self):
        # Read message length and unpack it into an integer
        raw_msglen = self.recvall(4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # Read the message data
        return self.recvall(msglen)


class FileCreate(Request):
    def __init__(self, filename: str, file_tree, request_type: str = 'fcreate'):
        super().__init__(request_type)
        self.filename = filename
        self.file_tree = file_tree

    def execute(self, client_sock):
        connect_client = ClientConnection(client_sock)

        if self.file_tree.file_exist(self.filename):
            send_msg(connect_client.sock, 'The file already exists. Do you want to delete it and create a new one? y/n'.encode())
            client_answer = connect_client.recv_msg().decode()
            if client_answer == 'y':
                self.broadcast_instruction(self.request_type + ' ' + self.filename)
            elif client_answer == 'n':
                pass
            else:
                send_msg(connect_client.sock, 'Something wrong'.encode())
                return
        else:
            self.broadcast_instruction(self.request_type + ' ' + self.filename)
            self.file_tree.add_file(self.filename)
        send_msg(connect_client.sock, 'Ok'.encode())

    def broadcast_instruction(self, instruction):
        ss_servers = self.file_tree.ss_priv_address()
        for server in ss_servers:
            send_instruction(instruction, server['ip'], server['port'])

    def ss_connection(self, connect_ss):
        ...

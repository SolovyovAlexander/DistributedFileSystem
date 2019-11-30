import socket
import struct
import sys
import os
import shutil

NAMING_SERVER_IP = 'localhost'
TOTAL_SPACE = 1024


class Request:
    def __init__(self, request_type: str = 'PING'):
        self.request_type = request_type

    def execute(self, connect):
        pass


def send_instruction(instruction, host, port):
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

    s.connect((host, port))

    send_msg(s, instruction.encode())

    print("Sending in progress..")

    s.close()


def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


def total_size(root):
    total = 0
    for dir, _, files in os.walk(root):
        for fn in files:
            total += os.path.getsize(os.path.join(dir, fn))
    return total


class NsConnection:
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


class Init(Request):
    def __init__(self, request_type: str = 'init'):
        super().__init__(request_type)

    def execute(self, client_sock):
        try:
            print('It should works')
            was_deleted = total_size('storage')
            print('was deleted: ', was_deleted)
            shutil.rmtree('storage')
            os.makedirs('storage')
            free_space = TOTAL_SPACE - total_size('storage')
            answer = 'Ok, available size: ' + str(free_space)
            send_msg(client_sock, answer)
        except:
            send_msg(client_sock, 'Error')


class FileCreate(Request):
    def __init__(self, filename: str, request_type: str = 'fcreate'):
        super().__init__(request_type)
        self.filename = filename

    def execute(self, ns_sock):
        try:
            print('File creation')
            basedir = os.path.dirname(self.filename)
            try:
                os.makedirs(basedir)
            except FileExistsError:
                print('directory already exist')
            try:
                with open(self.filename, 'w+'):# file does not recreate, may be first delete and than create new one

                    send_msg(ns_sock, 'Ok')
            except FileExistsError:
                send_msg(ns_sock, 'File already exist')
        except:
            send_msg(ns_sock, 'Error'.encode())


    def ss_connection(self, connect_ss):
        ...

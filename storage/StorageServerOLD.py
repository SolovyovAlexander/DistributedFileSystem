import socket
import struct
from threading import Thread
import glob

from storage import SsHandler

NAMING_SERVER_IP = 'localhost'
TOTAL_SPACE = 1024

def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def check_collision(file_name, extension, num=1):
    files = glob.glob("./*" + extension)
    flag = 0
    for file in files:
        name = file[2:]
        if (name == file_name):
            flag = 1
            break
    if (flag == 1):
        if (num == 1):
            file_name = file_name[:(len(file_name) - len(extension))]
            file_name = file_name + '_copy' + str(num) + extension
            return check_collision(file_name, extension, num + 1)
        else:
            file_name = file_name[:(len(file_name) - len(extension) - 5 - len(str(num - 1)))]
            file_name = file_name + '_copy' + str(num) + extension
            return check_collision(file_name, extension, num + 1)
    else:
        return file_name


class accept(Thread):
    def run(self) -> None:
        s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

        s.bind(("", 9002))
        s.listen()

        while True:
            sc, address = s.accept()
            instruction = Instruction(sc, address)
            instruction.run()
            sc.close()

        s.close()


class get(Thread):
    def __init__(self, sc, address):
        super().__init__()
        self.sc = sc
        self.address = address

    def run(self) -> None:
        print(self.address)
        file_name = recv_msg(self.sc).decode()
        extension = recv_msg(self.sc).decode()
        file_name = check_collision(file_name, extension)
        print(file_name)

        f = open(file_name, 'wb')
        data = recv_msg(self.sc)
        while data:
            # receive data and write it to file
            f.write(data)
            data = recv_msg(self.sc)

        f.close()

        self.sc.close()


class Instruction(Thread):
    def __init__(self, sock, address):
        super().__init__()
        self.sock = sock
        self.address = address

    def send_msg(self, request: str):
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(request.encode())) + request.encode()
        self.sock.sendall(msg)

    def run(self) -> None:
        print(self.address)
        instruction = recv_msg(self.sock).decode()
        print(instruction)
        if instruction == 'init':
            init = SsHandler.Init()
            init.execute(self.sock)


        elif instruction[:7] == 'fcreate':
            fcreate = SsHandler.FileCreate(instruction.split(' ')[1])
            fcreate.execute(self.sock)
        elif instruction == 'fread':
            ...
        elif instruction == 'fwrite':
            ...
        elif instruction == 'fdelete':
            ...
        elif instruction == 'finfo':
            ...
        elif instruction == 'fcopy':
            ...
        elif instruction == 'fmove':
            ...
        elif instruction == 'dopen':
            ...
        elif instruction == 'dread':
            ...
        elif instruction == 'dmake':
            ...
        elif instruction == 'ddelete':
            ...
        else:
            print('wrong instruction')


if __name__ == '__main__':
    a = accept()
    a.run()

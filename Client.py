import socket
import struct
import sys
import os
from threading import Thread
import ClientRequest


def instruction_resolver(instruction):
    if instruction == 'init':
        return ClientRequest.Initialize()
    elif instruction == 'fcreate':
        ...
    elif instruction == 'fread':
        ...
    elif instruction == 'fwrite':
        return ClientRequest.FileWrite()
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
        return None


class Client(Thread):
    def run(self) -> None:
        connection = ClientRequest.MessageProvider('localhost', 9999)
        instruction = input()
        client_req = instruction_resolver(instruction)
        client_req.execute(connection)




def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


def send_file(filename, host, port):

    f = open(filename, 'rb')
    file_size = os.fstat(f.fileno()).st_size
    file_name = os.path.basename(f.name)
    file_extension = os.path.splitext(filename)[1]
    percent = (1024/file_size)*100
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

    s.connect((host, port))
    send_msg(s,file_name.encode())
    send_msg(s, file_extension.encode())
    data = f.read(1024)
    print("Sending in progress..")
    print("0%")
    how_many_percents = percent
    while data:
        send_msg(s,data)
        if (how_many_percents>=100):
            print("100%")
        else:
            print(how_many_percents, "%")
        how_many_percents = how_many_percents+percent
        data = f.read(1024)
    s.close()

def send_instruction(instruction, host, port):
    s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

    s.connect((host, port))

    send_msg(s,instruction.encode())

    print("Sending in progress..")

    s.close()





if __name__ == '__main__':
    client = Client()
    client.run()
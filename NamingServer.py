import socket
import struct
from threading import Thread
import glob
import json
import NsHandler


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

        s.bind(("", 9001))
        s.listen()
        sc, address = s.accept()

        instruction = Instruction(sc, address)
        instruction.run()

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

    def run(self) -> None:
        print(self.address)
        try:
            instruction = recv_msg(self.sock).decode()
            print(instruction)
            if instruction == 'init':
                # TODO: return address and port of storage server, delete files from every server
                # otherwise send error
                NsHandler.send_msg(self.sock, 'localhost')
                NsHandler.send_msg(self.sock, '9002')

            elif instruction[:7] == 'fcreate' and len(instruction) > 8 and instruction[7] == ' ' and instruction[8] != '':
                file_create = NsHandler.FileCreate(filename=instruction.split(' ')[1], file_tree=file_tree)
                file_create.execute(self.sock)
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
        except Exception:
            print('Can\'t receive data from client')


class File_Tree:
    def __init__(self):
        self.file_tree = json.load(open('file_tree.json'))

    def ss_pub_address(self):
        ss_addresses = []
        for address in self.file_tree['storage_servers']:
            ss_addresses.append({'ip': address['public_ip'], 'port': address['port']})
        return ss_addresses

    def ss_priv_address(self):
        ss_addresses = []
        for address in self.file_tree['storage_servers']:
            ss_addresses.append({'ip': address['private_ip'], 'port': address['port']})
        return ss_addresses

    def file_exist(self, file_name):
        if file_name in self.file_tree['storage_servers'][0]['files']:
            return True
        else:
            return False
    def add_file(self, filename):
        for server in self.file_tree['storage_servers']:
            server['files'].append(filename)
        with open('file_tree.json', 'w') as outfile:
            json.dump(self.file_tree, outfile)

if __name__ == '__main__':
    file_tree = File_Tree()
    PRIMARY_NODE_PRIV = file_tree.ss_priv_address()[0]['ip']
    PRIMARY_NODE_PUB = file_tree.ss_pub_address()[0]['ip']
    while True:
        a = accept()
        a.run()

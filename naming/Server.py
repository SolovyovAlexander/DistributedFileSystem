import json
import socket
from struct import unpack, pack
from threading import Thread, Lock

from libs.request_codes import get_instruction_by_code
from libs.protocol import Response


class FileTree:
    def __init__(self):
        self.file_name = 'file_tree.json'

        json_file = open('file_tree.json', "r")
        json_data = json_file.read()
        self.file_tree = json.loads(json_data)

    def client_init(self):
        result = {
            'FILES': list(self.file_tree['DIRS']['root']['FILES'].keys()),
            'DIRS': list(self.file_tree['DIRS']['root']['DIRS'].keys()),
            'DIR': list(self.file_tree['DIRS'].keys())[0]
        }
        return result

    def dir_open(self, dir_name, path: list):
        current_fs_dir = self.file_tree['DIRS']

        for index in range(len(path)):
            if path[index] in current_fs_dir:
                current_fs_dir = current_fs_dir[path[index]]['DIRS']
            else:
                return None

        if dir_name in current_fs_dir:
            return []
        else:
            return None

    def dir_read(self, dir_name, path: list):

        current_fs_dir = self.file_tree['DIRS']
        parent = {}

        directory = ''
        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = current_fs_dir[key]['DIRS']
                directory = path[index]
            else:
                return None

        if dir_name == directory:
            return {
                'DIRS': list(parent['DIRS'].keys()),
                'FILES': list(parent['FILES'].keys()),
            }
        else:
            return None


class StorageServerInstance:

    def __init__(self, address):
        ip, port = address
        self.ip = ip
        self.port = port


class NamingServer:

    def __init__(self):
        self.file_tree = FileTree()
        self.lock = Lock()

        self.storages = []
        self.client_watcher = SelectClients(self, self.lock)
        self.storag_watcher = SelectStorageServer(self, self.lock)
        self.storag_watcher.start()
        self.client_watcher.start()
        self.storag_watcher.join()
        self.client_watcher.join()

    def init_client(self, request_data: dict) -> Response:
        return Response(200, self.file_tree.client_init())

    def get_primary(self):
        if len(self.storages) > 0:
            return self.storages[0]
        else:
            return None

    def init_storage(self, request_data: dict) -> Response:
        primary = self.get_primary()
        if primary != None:
            return Response(200, {
                'PRIMARY_IP': primary.ip,
                'PRIMARY_PORT': primary.port,
                'FILE_TREE': self.file_tree.file_tree
            })
        else:
            return Response(501, {})

    def open_directory(self, request_data: dict) -> Response:
        try:
            assert 'dir_name' in request_data
            assert 'dir_path' in request_data
        except AssertionError:
            return Response(400, {})

        if request_data['dir_name'] == 'root' and len(request_data['dir_path']) == 1 and request_data['dir_path'][
            0] == '':
            return self.init_client(request_data)

        dir = self.file_tree.dir_open(request_data['dir_name'], request_data['dir_path'])
        if dir is not None:
            return Response(200, {'DIR': request_data['dir_name']})
        else:
            return Response(404, {})

    def read_directory(self, request_data: dict) -> Response:
        try:
            assert 'dir_name' in request_data
            assert 'dir_path' in request_data
        except AssertionError:
            return Response(400, {})

        dir = self.file_tree.dir_read(request_data['dir_name'], request_data['dir_path'])
        if dir is not None:
            return Response(200, {
                'DIR': request_data['dir_name'],
                'DIRS': dir['DIRS'],
                'FILES': dir['FILES'],
            })
        else:
            return Response(404, {})

    def file_create(self, request_data: dict) -> Response:
        if request_data['force']:
            return Response(201, {})
        else:
            return Response(203, {})

    def unknown_instruction(self, request_data: dict) -> Response:
        return Response(400, {'error': 'Unknown Instruction'})

    CLIENT_CALLBACKS = {
        'CLIENT_INIT': init_client,
        'FILE_CREATE': file_create,
        'DIR_OPEN': open_directory,
        'DIR_READ': read_directory,
        'DIR_CREATE': open_directory,
        'DIR_DELETE': open_directory,
        'UNKNOWN': unknown_instruction,
    }

    STORAGE_CALLBACKS = {
        'STORAGE_INIT': init_storage,
        'UNKNOWN': unknown_instruction
    }


class SelectClients(Thread):
    def __init__(self, ns: NamingServer, lock: Lock):
        super().__init__()
        self.ns = ns
        self.lock = lock

    def logger(self, string: str):
        print('[\033[1;34;6m%s\033[0;0;0m] %s' % ('Clients Watcher', string))

    def run(self) -> None:

        port = 9001
        s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

        try:
            s.bind(("", port))
            with self.lock:
                self.logger("Socket is bind to port %s" % port)
                s.listen(5)
                self.logger("Socket is listening")

            while True:
                sc, address = s.accept()

                cct = ClientConnectionThread(sc, address, self.ns, debug=False)
                self.logger('Client \'%s\' connected' % (address,))
                cct.start()

        except ConnectionError as err:
            self.logger('Error: ' % err)
        finally:
            s.close()


class ClientConnectionThread(Thread):
    def __init__(self, sock, address, ns: NamingServer, debug: bool = False):
        super().__init__()
        self.sock = sock
        self.address = address
        self.ns = ns
        self.debug = debug

    def logger(self, string: str):
        ip, port = self.address
        print('[\033[1;33;6m%s:%s\033[0;0;0m] %s' % (ip, port, string))

    def run(self) -> None:
        while True:
            try:
                instruction, = unpack('I', self.sock.recv(4))
                self.sock.send(pack('I', 0))
                length, = unpack('I', self.sock.recv(4))
                self.sock.send(pack('I', 0))
                data = self.sock.recv(length).decode()
                self.sock.send(pack('I', 0))

                self.logger('CLIENT INSTRUCTION [%s]' % get_instruction_by_code(instruction))
                instruction = get_instruction_by_code(instruction)
                data = json.loads(data)

                if instruction in self.ns.CLIENT_CALLBACKS:
                    response = self.ns.CLIENT_CALLBACKS[instruction](self=self.ns, request_data=data)
                    status, length, data = response.to_payloads()
                    if self.debug:
                        print('     CODE: ', response.status)
                        print('     DATA: ', response.data)
                else:
                    status, length, data = self.ns.CLIENT_CALLBACKS['UNKNOWN'](self=self.ns,
                                                                               request_data=data).to_payloads()
                    print('Unknown instruction')

                self.sock.send(status)
                ok, = unpack('I', self.sock.recv(4))
                assert ok == 0
                self.sock.send(length)
                ok, = unpack('I', self.sock.recv(4))
                assert ok == 0
                self.sock.send(data)
                ok, = unpack('I', self.sock.recv(4))
                assert ok == 0
            except ConnectionResetError:
                print('Client %s disconnected' % str(self.address))
                self.sock.close()
                break


class SelectStorageServer(Thread):
    def __init__(self, ns: NamingServer, lock: Lock):
        super().__init__()
        self.ns = ns
        self.lock = lock

    def logger(self, string: str):
        print('[\033[1;32;6m%s\033[0;0;0m] %s' % ('Storage Watcher', string))

    def run(self) -> None:

        port = 9002
        s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

        try:
            s.bind(("", port))
            with self.lock:
                self.logger("Socket is bind to port %s" % port)
                s.listen(5)
                self.logger("Socket is listening")

            while True:
                sc, address = s.accept()
                cct = ClientConnectionThread(sc, address, self.ns)
                self.logger('Storage Server \'%s\' connected.' % (address,))
                cct.start()

        except ConnectionError as err:
            with self.lock:
                self.logger(err.__str__())
        finally:
            s.close()


class StorageServerConnectionThread(Thread):
    def __init__(self, sock, address, ns: NamingServer, debug: bool = False):
        super().__init__()
        self.sock = sock
        self.address = address
        self.ns = ns
        self.ns.storages.append(StorageServerInstance(address))
        self.primary = False
        self.debug = debug

    def logger(self, string: str):
        ip, port = self.address
        if self.primary:
            print('[\033[1;31;6m NODE %s:%s\033[0;0;0m] %s' % (ip, port, string))
        else:
            print('[\033[1;30;6m NODE %s:%s\033[0;0;0m] %s' % (ip, port, string))

    def run(self) -> None:
        while True:
            try:
                instruction, = unpack('I', self.sock.recv(4))
                self.sock.send(pack('I', 0))
                length, = unpack('I', self.sock.recv(4))
                self.sock.send(pack('I', 0))
                data = self.sock.recv(length).decode()
                self.sock.send(pack('I', 0))

                self.logger('STORAGE SERVER INSTRUCTION [%s]' % get_instruction_by_code(instruction))
                instruction = get_instruction_by_code(instruction)
                data = json.loads(data)

                print('instruction in self.ns.STORAGE_CALLBACKS: ', instruction in self.ns.STORAGE_CALLBACKS)
                if instruction in self.ns.STORAGE_CALLBACKS:
                    response = self.ns.STORAGE_CALLBACKS[instruction](self=self.ns, request_data=data)
                    status, length, data = response.to_payloads()
                    if self.debug:
                        print('     CODE: ', response.status)
                        print('     DATA: ', response.data)
                else:
                    status, length, data = self.ns.STORAGE_CALLBACKS['UNKNOWN'](self=self.ns, request_data=data).to_payloads()
                    print('Unknown instruction')

                self.sock.send(status)
                ok, = unpack('I', self.sock.recv(4))
                assert ok == 0
                self.sock.send(length)
                ok, = unpack('I', self.sock.recv(4))
                assert ok == 0
                self.sock.send(data)
                ok, = unpack('I', self.sock.recv(4))
                assert ok == 0
            except ConnectionResetError:
                print('Client %s disconnected' % str(self.address))
                self.sock.close()
                break


if __name__ == '__main__':
    lock = Lock()
    server = NamingServer()

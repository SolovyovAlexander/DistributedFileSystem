import json
import socket
from struct import unpack, pack
from threading import Thread, Lock

from libs.protocol import Response, Request, MessageProvider
from naming_server.FileTree import FileTree


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
            ip, port = primary.address
            return Response(200, {
                'PRIMARY_IP': ip,
                'PRIMARY_PORT': port,
                'FILE_TREE': self.file_tree.file_tree
            })
        else:
            return Response(501, {})

    def open_directory(self, request_data: dict) -> Response:

        if request_data['dir_name'] == 'root' and len(request_data['dir_path']) == 1 and request_data['dir_path'][0] == '':
            return self.init_client(request_data)

        dir = self.file_tree.dir_open(request_data['dir_name'], request_data['dir_path'])
        if dir is not None:
            return Response(200, {'DIR': request_data['dir_name']})
        else:
            return Response(404, {})

    def create_directory(self, request_data: dict) -> Response:
        potential_path = request_data['dir_path'].copy()
        potential_path.append(request_data['dir_name'])

        if self.file_tree.path_exists(potential_path):
            return Response(400, {'ERROR': 'Folder already exists'})
        dir = self.file_tree.insert_dir(request_data['dir_name'], request_data['dir_path'])
        if dir is not None:
            request = Request(command='STORAGE_FOLDER_CREATE', params={'dir_name': request_data['dir_name'], 'dir_path': request_data['dir_path']})
            self.broadcast(request)

            return Response(201, dir)
        else:
            return Response(404, {'ERROR': 'NO SUCH FILEPATH'})

    def delete_directory(self, request_data: dict) -> Response:
        path = request_data['dir_path'].copy()
        path.append(request_data['dir_name'])

        if self.file_tree.path_exists(path):
            dir = self.file_tree.delete_dir(request_data['dir_name'], request_data['dir_path'])
            request = Request(command='STORAGE_FOLDER_DELETE',
                              params={'dir_name': request_data['dir_name'], 'dir_path': request_data['dir_path']})
            self.broadcast(request)
            return Response(200, dir)
        else:
            return Response(404, {'ERROR': 'NO SUCH FILEPATH'})

    def read_directory(self, request_data: dict) -> Response:
        try:
            assert 'dir_name' in request_data
            assert 'dir_path' in request_data
        except AssertionError:
            return Response(400, {})

        directory = self.file_tree.dir_read(request_data['dir_name'], request_data['dir_path'])
        if directory is not None:
            return Response(200, {
                'DIR': request_data['dir_name'],
                'DIRS': directory['DIRS'],
                'FILES': directory['FILES'],
            })
        else:
            return Response(404, {})

    def broadcast(self, request: Request):
        for each_storage_server in self.storages:
            ip, port = each_storage_server.address
            connection = MessageProvider(ip, 9004)
            connection.connect()

            response = connection.send(request)
            assert response.status == 200

    def file_create(self, request_data: dict) -> Response:

        # Prepare broadcast message to storages
        data = request_data.copy()
        del data['force']
        request = Request('STORAGE_FILE_CREATE', params=data)

        if request_data['force']:

            self.broadcast(request)
            self.file_tree.create_file(request_data['file_name'], request_data['dir_path'])
            kek = self.file_tree.dir_read(request_data['dir_path'][-1], request_data['dir_path'])
            return Response(201, kek)
        else:
            if self.file_tree.file_found(request_data['file_name'], request_data['dir_path']):
                return Response(203, {})
            else:
                self.broadcast(request)
                self.file_tree.create_file(request_data['file_name'], path=request_data['dir_path'])
                kek = self.file_tree.dir_read(request_data['dir_path'][-1], request_data['dir_path'])
                return Response(201, kek)

    def file_download(self, request_data: dict) -> Response:
        dir = self.file_tree.file_found(request_data['file_name'], request_data['dir_path'])
        if dir:
            primary = self.get_primary()
            if primary is not None:
                ip, port = primary.address
                connection = MessageProvider(ip, 9004)
                connection.connect()
                file_hash = self.file_tree.get_file_hash(request_data['file_name'], request_data['dir_path'])
                request = Request('CONFIRM_FILE_DOWNLOADED', {
                    'hash': file_hash,
                    'file_name': request_data['file_name'],
                    'dir_path': request_data['dir_path']
                })
                response = connection.send(request)
                if response.status == 200:
                    return Response(200, {'IP': ip, 'PORT': 9003})
                else:
                    return Response(response.status, {"MSG": response.message()})
            else:
                return Response(500, {'ERROR': 'No storage servers are running'})
        else:
            return Response(404, {})

    def file_upload(self, request_data: dict) -> Response:
        path_is_valid = self.file_tree.path_exists(request_data['dir_path'])
        if path_is_valid:
            primary = self.get_primary()
            if primary is not None:
                ip, port = primary.address
                return Response(200, {'IP': ip, 'PORT': 9003})
            else:
                return Response(500, {'ERROR': 'No storage servers are running'})
        else:
            return Response(404, {})

    def file_delete(self, request_data: dict) -> Response:

        # Prepare broadcast message to storages
        data = request_data.copy()
        request = Request('STORAGE_FILE_DELETE', params=data)
        try:
            self.broadcast(request)
        except AssertionError:
            return Response(500, {})
        result = self.file_tree.delete_file(request_data['file_name'], request_data['dir_path'])
        if result is None:
            return Response(404, {})

        kek = self.file_tree.dir_read(request_data['dir_path'][-1], request_data['dir_path'])
        return Response(200, kek)

    def file_move(self, request_data: dict) -> Response:

        request = Request('STORAGE_FILE_MOVE', params=request_data)
        try:
            self.broadcast(request)
        except AssertionError:
            return Response(500, {})

        filehash = self.file_tree.get_file_hash(request_data['file_name'], request_data['dir_path'])
        self.file_tree.delete_file(request_data['file_name'], request_data['dir_path'])
        res = self.file_tree.insert_file(hash=filehash, file_name=request_data['file_name'], path=request_data['destination_path'])

        return Response(200, res)

    def file_copy(self, request_data: dict) -> Response:

        request = Request('STORAGE_FILE_COPY', params=request_data)
        try:
            self.broadcast(request)
        except AssertionError:
            return Response(500, {})

        filehash = self.file_tree.get_file_hash(request_data['file_name'], request_data['dir_path'])
        res = self.file_tree.insert_file(hash=filehash, file_name=request_data['file_name'],
                                         path=request_data['destination_path'])

        return Response(200, res)

    def unknown_instruction(self, request_data: dict) -> Response:
        return Response(400, {'error': 'Unknown Instruction'})

    def reset(self, request_data: dict) -> Response:

        request = Request('RESET_REPLICA', params={})
        self.broadcast(request)
        data = self.file_tree.reset()

        return Response(200, data)

    def get_replica_set(self, request_data: dict) -> Response:
        replicas = []
        # Since first storage is a primary
        for r in self.storages[1:]:
            ip, port = r.address
            replicas.append({'IP': ip, 'PORT': 9003})
        return Response(200, {'REPLICAS': replicas})

    def confirm_file_uploaded(self, request_data: dict) -> Response:
        self.file_tree.insert_file(request_data['file_name'], request_data['hash'], request_data['dir_path'])

        return Response(200, {})

    def file_info(self, request_data: dict) -> Response:
        primary = self.get_primary()
        if primary is not None:
            ip, port = primary.address
            connection = MessageProvider(ip, 9004)
            connection.connect()
            request = Request('STORAGE_FILE_INFO', {
                'file_name': request_data['file_name'],
                'dir_path': request_data['dir_path']
            })
            response = connection.send(request)
            if response.status == 200:
                response.data['name'] = request_data['file_name']
                return Response(200, response.data)
            else:
                return Response(response.status, {"MSG": response.message()})
        else:
            return Response(500, {})

    CLIENT_CALLBACKS = {
        'CLIENT_INIT': init_client,
        'FILE_CREATE': file_create,
        'FILE_DOWNLOAD': file_download,
        'FILE_UPLOAD': file_upload,
        'FILE_DELETE': file_delete,
        'FILE_MOVE': file_move,
        'FILE_COPY': file_copy,
        'FILE_INFO': file_info,
        'DIR_OPEN': open_directory,
        'DIR_READ': read_directory,
        'DIR_CREATE': create_directory,
        'DIR_DELETE': delete_directory,
        'RESET': reset,
        'UNKNOWN': unknown_instruction,
    }

    STORAGE_CALLBACKS = {
        'STORAGE_INIT': init_storage,
        'GET_REPLICA_SET': get_replica_set,
        'CONFIRM_FILE_UPLOADED': confirm_file_uploaded,
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

                self.logger('CLIENT INSTRUCTION [%s]' % Request.get_instruction_by_code(instruction))
                instruction = Request.get_instruction_by_code(instruction)
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
                cct = StorageServerConnectionThread(sc, address, self.ns)
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
        self.ns.storages.append(self)
        self.primary = False
        self.debug = debug

    def logger(self, string: str):
        ip, port = self.address
        print('[\033[1;30;6mNODE %s:%s\033[0;0;0m] %s' % (ip, port, string))

    def run(self) -> None:
        while True:
            try:
                instruction, = unpack('I', self.sock.recv(4))
                self.sock.send(pack('I', 0))
                length, = unpack('I', self.sock.recv(4))
                self.sock.send(pack('I', 0))
                data = self.sock.recv(length).decode()
                self.sock.send(pack('I', 0))

                self.logger('STORAGE SERVER INSTRUCTION [%s]' % Request.get_instruction_by_code(instruction))
                instruction = Request.get_instruction_by_code(instruction)
                data = json.loads(data)

                if instruction in self.ns.STORAGE_CALLBACKS:
                    response = self.ns.STORAGE_CALLBACKS[instruction](self=self.ns, request_data=data)
                    status, length, data = response.to_payloads()
                    if self.debug:
                        print('     CODE: ', response.status)
                        print('     DATA: ', response.data)
                else:
                    status, length, data = self.ns.STORAGE_CALLBACKS['UNKNOWN'](self=self.ns,
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
            except ConnectionResetError as err:
                self.logger('Storage server disconnected [%s]' % str(err))
                self.sock.close()
                self.ns.storages.remove(self)
                self.logger('Storage Servers online: ' + self.ns.storages.__str__())
                break


if __name__ == '__main__':
    lock = Lock()
    server = NamingServer()

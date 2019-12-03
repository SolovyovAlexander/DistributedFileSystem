import json
from struct import unpack, pack
from threading import Thread, Lock
from time import sleep
from libs.helper import *

from libs.protocol import MessageProvider, Response, Request


class NamingListener(Thread):
    def __init__(self, ss, lock: Lock):
        super().__init__()
        self.ss = ss
        self.lock = lock

    def logger(self, string: str):
        print('[\033[1;35;6m%s\033[0;0;0m] %s' % ('Naming Server Command Listener', string))

    def run(self) -> None:

        port = 9004
        s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

        try:
            s.bind(("", port))
            with self.lock:
                self.logger("Socket is bind to port %s" % port)
                s.listen(5)
                self.logger("Socket is listening")

            while True:
                sc, address = s.accept()

                cct = NamingServerInstructionThread(sc, address, self.ss, debug=False)
                self.logger('Naming Server \'%s\' connected' % (address,))
                cct.start()

        except ConnectionError as err:
            self.logger('Error: ' % err)
        finally:
            s.close()


class NamingServerInstructionThread(Thread):
    def __init__(self, sock, address, ss, debug: bool = False):
        super().__init__()
        self.sock = sock
        self.address = address
        self.ss = ss
        self.debug = debug

    def logger(self, string: str):
        ip, port = self.address
        print('[\033[1;33;6m%s\033[0;0;0m] %s' % ('NAMING SERVER CONNECTION #%s' % port, string))

    def run(self) -> None:
        try:
            instruction, = unpack('I', self.sock.recv(4))
            self.sock.send(pack('I', 0))
            length, = unpack('I', self.sock.recv(4))
            self.sock.send(pack('I', 0))
            data = self.sock.recv(length).decode()
            self.sock.send(pack('I', 0))

            self.logger('INSTRUCTION [%s]' % Request.get_instruction_by_code(instruction))
            instruction = Request.get_instruction_by_code(instruction)
            data = json.loads(data)

            if instruction in self.ss.NAMING_CALLBACKS:
                response = self.ss.NAMING_CALLBACKS[instruction](self=self.ss, request_data=data)
                status, length, data = response.to_payloads()
                if self.debug:
                    print('     CODE: ', response.status)
                    print('     DATA: ', response.data)
            else:
                status, length, data = self.ss.NAMING_CALLBACKS['UNKNOWN'](self=self.ss,
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
            self.sock.close()
            self.logger('Naming Server finished and disconnected.')
        except ConnectionResetError:
            self.logger('Naming Server disconnected.')
            self.sock.close()


class StorageServer:
    def __init__(self, naming_address, naming_port):
        super().__init__()
        self.naming_address = naming_address
        self.naming_port = naming_port
        self.naming_connection = None
        self.file_tree = {}
        self.up_to_date = False
        self.primary = False

        lock = Lock()
        self.client_listener = SelectClients(self, lock)
        self.client_listener.start()
        self.naming_listener = NamingListener(self, lock)
        self.naming_listener.start()

    def connect(self):
        for i in range(3):
            try:
                self.naming_connection = MessageProvider(self.naming_address, self.naming_port)
                self.naming_connection.connect()
                print('Successfully connected to Naming Server')
                break
            except ConnectionError as error:
                print('Cannot connect to naming server', error, file=sys.stderr)
                sleep(3)

    def send_req_to_naming(self, request: Request):

        if self.naming_connection is None:
            self.connect()

        if self.naming_connection is not None:
            try:
                return self.naming_connection.send(request)
            except ValueError as error:
                print('    Error: ' + error.__str__())
        return None

    def naming_reset(self, request_data) -> Response:
        import shutil
        shutil.rmtree('storage/root/', ignore_errors=True)
        os.makedirs('storage/root/')
        return Response(200, {})

    def naming_confirm_file_downloaded(self, request_data):
        # TODO check file in DFS, compare with file tree
        return Response(200, {})

    def storage_file_create(self, request_data):
        path = 'storage/' + str('/'.join(request_data['dir_path']) + '/' + request_data['file_name'])
        fp = open(path, 'a+')
        fp.close()
        return Response(200, {})

    def storage_file_delete(self, request_data):
        path = 'storage/' + str('/'.join(request_data['dir_path']) + '/' + request_data['file_name'])
        try:
            os.remove(path)
        except FileNotFoundError as err:
            pass
        finally:
            return Response(200, {})

    def storage_file_move(self, request_data):

        source_path = request_data['dir_path']
        source_path.append(request_data['file_name'])

        dest_path = request_data['destination_path']
        dest_path.append(request_data['file_name'])

        source_path = 'storage/' + '/'.join(source_path)
        dest_path = 'storage/' + '/'.join(dest_path)

        import shutil
        try:
            shutil.move(source_path, dest_path)
        except FileNotFoundError:
            return Response(404, {})

        return Response(200, {})

    def storage_file_copy(self, request_data):

        source_path = request_data['dir_path']
        source_path.append(request_data['file_name'])

        dest_path = request_data['destination_path']
        dest_path.append(request_data['file_name'])

        source_path = 'storage/' + '/'.join(source_path)
        dest_path = 'storage/' + '/'.join(dest_path)

        import shutil
        shutil.copy(source_path, dest_path)

        return Response(200, {})

    def storage_folder_create(self, request_data):
        path = 'storage/' + str('/'.join(request_data['dir_path']) + '/' + request_data['dir_name'])
        os.makedirs(path)
        return Response(200, {})

    def storage_folder_delete(self, request_data):
        import shutil
        path = 'storage/' + str('/'.join(request_data['dir_path']) + '/' + request_data['dir_name'])
        try:
            shutil.rmtree(path, ignore_errors=True)
        except FileNotFoundError:
            pass
        finally:
            return Response(200, {})

    def storage_file_info(self, request_data):
        import shutil
        path = 'storage/' + str('/'.join(request_data['dir_path']) + '/' + request_data['file_name'])
        data = {
            'size': os.path.getsize(path),
            'hash': get_file_hash(path),
            'time': get_file_timestamp(path)
        }
        return Response(200, data)

    NAMING_CALLBACKS = {
        'CONFIRM_FILE_DOWNLOADED': naming_confirm_file_downloaded,
        'STORAGE_FILE_CREATE': storage_file_create,
        'STORAGE_FILE_DELETE': storage_file_delete,
        'STORAGE_FILE_MOVE': storage_file_move,
        'STORAGE_FILE_COPY': storage_file_copy,
        'STORAGE_FILE_INFO': storage_file_info,
        'STORAGE_FOLDER_CREATE': storage_folder_create,
        'STORAGE_FOLDER_DELETE': storage_folder_delete,
        'RESET_REPLICA': naming_reset
    }


class SelectClients(Thread):
    def __init__(self, ns: StorageServer, lock: Lock):
        super().__init__()
        self.ss = ns
        self.lock = lock

    def logger(self, string: str):
        print('[\033[1;34;6m%s\033[0;0;0m] %s' % ('Clients Watcher', string))

    def run(self) -> None:

        port = 9003
        s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)

        try:
            s.bind(("", port))
            with self.lock:
                self.logger("Socket is bind to port %s" % port)
                s.listen(5)
                self.logger("Socket is listening")

            while True:
                sc, address = s.accept()

                cct = ClientConnectionThread(sc, address, self.ss, debug=False)
                self.logger('Client \'%s\' connected' % (address,))
                cct.start()

        except ConnectionError as err:
            self.logger('Error: ' % err)
        finally:
            s.close()


class ClientConnectionThread(Thread):
    def __init__(self, sock, address, ss: StorageServer, debug: bool = False):
        super().__init__()
        self.sock = sock
        self.address = address
        self.ss = ss
        self.debug = debug

    def logger(self, string: str):
        ip, port = self.address
        print('[\033[1;33;6m%s:%s\033[0;0;0m] %s' % (ip, port, string))

    def run(self) -> None:
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

            if instruction == 'FILE_DOWNLOAD_DIRECT':
                path = 'storage/' + str('/'.join(data['dir_path']) + '/' + data['file_name'])
                fp = open(path, 'rb')
                send_file(self.sock, fp)

            if instruction == 'FILE_UPLOAD_DIRECT':
                path = 'storage/' + str('/'.join(data['dir_path']) + '/' + data['file_name'])
                fp = open(path, 'wb+')

                recv_file(self.sock, fp)

                request = Request('CONFIRM_FILE_UPLOADED', {'hash': get_file_hash(path), 'file_name': data['file_name'],
                                                            'dir_path': data['dir_path']})
                response = self.ss.send_req_to_naming(request)

                assert response.status == 200

                request = Request('GET_REPLICA_SET', params={})
                response = self.ss.naming_connection.send(request)

                for each_replica in response.data['REPLICAS']:
                    client_like_connection = MessageProvider(each_replica['IP'], each_replica['PORT'])
                    client_like_connection.connect()
                    request = Request('FILE_REPLICATE', {'file_name': data['file_name'], 'dir_path': data['dir_path']})
                    response = client_like_connection.send(request)

                    fp = open(path, 'rb')
                    send_file(client_like_connection.sock, fp)

            if instruction == 'FILE_REPLICATE':
                path = 'storage/' + str('/'.join(data['dir_path']) + '/' + data['file_name'])
                fp = open(path, 'wb+')

                recv_file(self.sock, fp)

        except ConnectionResetError:
            self.logger('Client %s disconnected' % str(self.address))
            self.sock.close()


if __name__ == '__main__':
    import sys

    # assert 3 == len(sys.argv)
    # sys.argv[1], sys.argv[2]
    storage = StorageServer('3.125.80.251', 9002)
    request = Request('STORAGE_INIT', {})
    response = storage.send_req_to_naming(request)
    if response.status == 200:
        storage.file_tree = response.data['FILE_TREE']

import os
import sys
from time import sleep

from libs.protocol import MessageProvider
from storage.StorageRequests import instruction_resolver


class StorageServer:
    def __init__(self, naming_address, naming_port):
        self.naming_address = naming_address
        self.naming_port = naming_port
        self.connection = None

    # Initialize for NS
    # Get file tree with hashes
    def initialize(self):
        ...

    def file_create(self):
        ...

    @staticmethod
    def get_file_hash(file):
        import hashlib
        hasher = hashlib.md5()
        with open(file, 'rb') as afile:
            buf = afile.read()
            hasher.update(buf)
        print(hasher.hexdigest(), file)

    def connect(self):
        for i in range(3):
            try:
                self.connection = MessageProvider(self.naming_address, self.naming_port)
                print('Successfully connected to Naming Server')
                break
            except ConnectionError as error:
                print('Cannot connect to naming server', error, file=sys.stderr)
                sleep(3)

    def send_req_to_naming(self, command):

        if self.connection is None:
            self.connect()

        print('INSTRUCTION [\033[1;36;6m%s\033[0;0;0m]' % command)
        if self.connection is not None:
            command = command.split(' ')
            instruction = command[0]
            params = command[1:]
            client_req = instruction_resolver(instruction, params)
            try:
                response = self.connection.send(client_req)
                print('    STATUS: ', response.status)
                print('    DATA: ', response.data)
                return response
            except ValueError as error:
                print('    Error: ' + error.__str__())
        return None


if __name__ == '__main__':

    import sys

    # assert 3 == len(sys.argv)
    # sys.argv[1], sys.argv[2]
    storage = StorageServer('localhost', 9002)
    response = storage.send_req_to_naming('STORAGE_INIT')

    for path, sub_dirs, files in os.walk('storage\\'):
        for name in files:
            StorageServer.get_file_hash(os.path.join(path, name))

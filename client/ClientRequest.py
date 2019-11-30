from DistributedFileSystem.libs.protocol import Request, Response


class Initialize(Request):
    def __init__(self, params: list):
        super().__init__(params)
        self.request_type = 'CLIENT_INIT'


class FileWrite(Request):
    def __init__(self, params: list):
        super().__init__(params)
        self.request_type = 'FILE_WRITE'
        self.params_count = 2


class FileCreate(Request):
    def __init__(self, params: list, force=False):
        super().__init__(params)
        self.request_type = 'FILE_CREATE'
        self.force = force
        self.params_count = 1
        self.params = params

    def validate_params(self):
        if self.params_count != len(self.params):
            raise ValueError('Invalid parameters number')
        self.validated_params = {
            'filename': self.params[0],
            'force': self.force
        }


class DirOpen(Request):

    def __init__(self, params: list):
        super().__init__(params)
        self.request_type = 'DIR_OPEN'
        self.params_count = 2
        self.params = params

    def validate_params(self):
        if self.params_count != len(self.params):
            raise ValueError('Invalid parameters number')
        self.validated_params = {
            'dir_name': self.params[0],
            'dir_path': self.params[1].split(','),
        }


class DirRead(Request):

    def __init__(self, params: list):
        super().__init__(params)
        self.request_type = 'DIR_READ'
        self.params_count = 2
        self.params = params

    def validate_params(self):
        if self.params_count != len(self.params):
            raise ValueError('Invalid parameters number')
        self.validated_params = {
            'dir_name': self.params[0],
            'dir_path': self.params[1].split(','),
        }


class UnknownCommand(Request):

    ...


class Exit(Request):

    ...

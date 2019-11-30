from DistributedFileSystem.libs.protocol import Request


class Reset(Request):
    def __init__(self, params: list):
        super().__init__(params)
        self.request_type = 'RESET'
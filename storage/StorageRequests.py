from DistributedFileSystem.libs.protocol import Request


class Initialize(Request):
    def __init__(self, params: list):
        super().__init__(params)
        self.request_type = 'STORAGE_INIT'


class UnknownCommand(Request):
    ...


def instruction_resolver(instruction, params: list) -> Request:
    if instruction == 'STORAGE_INIT':
        return Initialize(params)
    else:
        return UnknownCommand(params=params, command=instruction)

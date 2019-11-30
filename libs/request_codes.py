CODES = {
    'CLIENT_INIT': 0,
    'FILE_CREATE': 1,
    'FILE_WRITE' : 2,
    'DIR_OPEN': 10,
    'DIR_READ': 11,



    'STORAGE_INIT': 100
}


def get_instruction_by_code(code):
    for k, v in CODES.items():
        if v == code: return k

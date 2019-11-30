from time import sleep
from tkinter import *
from tkinter import messagebox, filedialog

from client.ClientRequest import Initialize, FileCreate, FileWrite, DirOpen, DirRead, Exit, UnknownCommand
from libs.protocol import MessageProvider, Request


def instruction_resolver(instruction, params: list) -> Request:
    if instruction == 'CLIENT_INIT':
        return Initialize(params=params)
    elif instruction == 'FILE_CREATE':
        return FileCreate(params=params)
    elif instruction == 'FILE_CREATE_FORCE':
        return FileCreate(params=params, force=True)
    elif instruction == 'fread':
        ...
    elif instruction == 'fwrite':
        return FileWrite(params=params)
    elif instruction == 'fdelete':
        ...
    elif instruction == 'finfo':
        ...
    elif instruction == 'fcopy':
        ...
    elif instruction == 'fmove':
        ...
    elif instruction == 'DIR_OPEN':
        return DirOpen(params=params)
    elif instruction == 'DIR_READ':
        return DirRead(params=params)
    elif instruction == 'dmake':
        ...
    elif instruction == 'ddelete':
        ...
    elif instruction == 'exit':
        return Exit(params=params)
    else:
        return UnknownCommand(params=params, command=instruction)


class PopupWindow(object):
    def __init__(self, master):
        top = self.top = Toplevel(master)
        self.l = Label(top, text="Hello World")
        self.l.pack()
        self.e = Entry(top)
        self.e.pack()
        self.b = Button(top, text='Ok', command=self.cleanup)
        self.b.pack()

    def cleanup(self):
        self.value = self.e.get()
        self.top.destroy()


class Client:

    def __init__(self):
        super().__init__()
        self.connection = None
        self.current_directory_path = []
        self.file_tree = {}

    def CONNECT(self, event=None):
        for i in range(3):
            try:
                self.connection = MessageProvider('localhost', 9001)
                print('Successfully connected to Naming Server')
                break
            except ConnectionError as error:
                print('Cannot connect to naming server', error, file=sys.stderr)
                sleep(3)

    def SEND_INSTRUCTION(self, command):

        if self.connection is None:
            self.CONNECT()

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


class MainWindow:
    def __init__(self, master: Tk):
        self.master = master
        self.client = Client()
        self.current_directory_path = []

        self.master.title("Distributed File Systems")

        self.dir_actions = Frame(root)
        self.b0 = Button(self.dir_actions, text="CLIENT INIT")
        self.b6 = Button(self.dir_actions, text="<-------")

        self.b6.pack(side="top", fill=X)
        self.b0.pack(side="top", fill=X)

        self.b0.bind("<Button-1>", self.clicked_client_init)
        self.b6.bind("<Button-1>", self.back_dir)

        self.dir_actions.grid(column=4, row=0, sticky=N + S + W + E)

        self.file_actions = Frame(root)
        self.b1 = Button(self.file_actions, text="CREATE FILE")
        self.b2 = Button(self.file_actions, text="DOWNLOAD FILE")
        self.b3 = Button(self.file_actions, text="UPLOAD FILE")

        self.b1.bind("<Button-1>", self.clicked_file_create)
        self.b2.bind("<Button-1>", self.clicked_file_upload)
        self.b3.bind("<Button-1>", self.clicked_file_upload)

        self.b1.pack(side="top", fill=X)
        self.b2.pack(side="top", fill=X)
        self.b3.pack(side="top", fill=X)

        self.file_actions.grid(column=4, row=1, sticky=N + S + W + E)

        self.directories_storage = []
        self.directories = Listbox(root, selectmode=SINGLE)
        self.directories.insert(0, *self.directories_storage)
        self.directories.bind('<Double-Button>', self.clicked_dir)
        self.directories.grid(column=0, row=0, columnspan=3, sticky=N + S + W + E)

        self.filenames = []
        self.files = Listbox(root, selectmode=SINGLE)
        self.files.insert(0, *self.filenames)
        self.files.grid(column=0, row=1, columnspan=3, sticky=N + S + W + E)

        self.status_label = Label(text="STATUS: ", bg='lightgray', font=("Arial Bold", 11))
        self.status_label.grid(column=0, row=2, sticky=N + S + W + E)

        self.status = Label(text="None", bg='gray', font=("Arial Bold", 11))
        self.status.grid(column=1, row=2, columnspan=2, sticky=N + S + W + E)


    def clicked_client_init(self, event):

        res = "CLIENT_INIT"
        response = self.client.SEND_INSTRUCTION(res)

        if response is not None:
            if response.status == 200:
                self.status.configure(text='OK', bg='green')
                self.directories.delete(0, self.directories.size())
                self.files.delete(0, self.files.size())
                self.directories.insert(0, *response.data['DIRS'])
                self.files.insert(0, [])
                self.current_directory_path = [response.data['DIR']]
        else:
            self.status.configure(text='NO CONNECTION', bg='red')

    def clicked_file_upload(self, event):
        ...

    def clicked_file_create(self, event):

        w = PopupWindow(root)
        self.b1["state"] = "disabled"
        root.wait_window(w.top)
        self.b1["state"] = "normal"

        print(w.value)

        instruction = "FILE_CREATE "
        response = self.client.SEND_INSTRUCTION(instruction)
        if response is not None:
            if response.status == 203:
                self.status.configure(text='NEED CONFIRMATION', bg='yellow')
                answer = messagebox.askokcancel('File already exist',
                                                'File already exist, Do you want to replace it?')
                if answer is True:
                    res = "FILE_CREATE_FORCE " + str(w.value)
                    response = self.client.SEND_INSTRUCTION(res)
                    if response.status == 201:
                        self.status.configure(text='CREATED', bg='green')
        else:
            self.status.configure(text='NO CONNECTION', bg='red')

    def clicked_dir(self, event):

        instruction = 'DIR_OPEN ' + self.directories.get(ACTIVE) + ' ' + ','.join(self.current_directory_path)
        response = self.client.SEND_INSTRUCTION(instruction)

        if response.status == 200:
            self.current_directory_path.append(response.data['DIR'])
            instruction = 'DIR_READ ' + self.directories.get(ACTIVE) + ' ' + ','.join(self.current_directory_path)
            response = self.client.SEND_INSTRUCTION(instruction)
            if response.status == 200:

                self.directories.delete(0, self.directories.size())
                self.directories.insert(0, *response.data['DIRS'])

                self.files.delete(0, self.files.size())
                self.files.insert(0, *response.data['FILES'])

    def back_dir(self, event):

        if len(self.current_directory_path) > 1:
            instruction = 'DIR_OPEN ' + self.current_directory_path[len(self.current_directory_path)-2] + ' ' + ','.join(self.current_directory_path[0:len(self.current_directory_path)-2])
            response = self.client.SEND_INSTRUCTION(instruction)

            if response.status == 200:
                del self.current_directory_path[-1]
                del self.current_directory_path[-1]
                self.current_directory_path.append(response.data['DIR'])
                instruction = 'DIR_READ ' + self.current_directory_path[len(self.current_directory_path)-1] + ' ' + ','.join(self.current_directory_path)
                response = self.client.SEND_INSTRUCTION(instruction)
                if response.status == 200:

                    self.directories.delete(0, self.directories.size())
                    self.directories.insert(0, *response.data['DIRS'])

                    self.files.delete(0, self.files.size())
                    self.files.insert(0, *response.data['FILES'])


if __name__ == '__main__':
    root = Tk()
    main = MainWindow(root)
    root.mainloop()

import socket
from struct import unpack
from time import sleep
from tkinter import *
from tkinter import messagebox, filedialog
from libs.protocol import MessageProvider, Request
from libs.helper import *


class PopupWindow(object):
    def __init__(self, master):
        top = self.top = Toplevel(master)
        self.l = Label(top, text="Select a Name")
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

    def connect(self, event=None):
        for i in range(3):
            try:
                self.connection = MessageProvider('localhost', 9001)
                self.connection.connect()
                print('Successfully connected to Naming Server')
                break
            except ConnectionError as error:
                print('Cannot connect to naming server', error, file=sys.stderr)
                sleep(3)


class MainWindow:
    def __init__(self, master: Tk):
        self.master = master
        self.client = Client()
        self.current_directory_path = []

        self.master.title("Distributed File Systems")

        self.dir_actions = Frame(root)
        self.btn_init = Button(self.dir_actions, text="INIT [RESET]", background='red')
        self.btn_back = Button(self.dir_actions, text="<-------")
        self.btn_create_dir = Button(self.dir_actions, text="CREATE DIR")
        self.btn_delete_dir = Button(self.dir_actions, text="DELETE DIR")

        self.btn_back.pack(side="top", fill=X)
        self.btn_create_dir.pack(side="top", fill=X)
        self.btn_delete_dir.pack(side="top", fill=X)
        self.btn_init.pack(side="bottom", fill=X)

        self.btn_create_dir.bind("<Button-1>", self.clicked_create_dir)
        self.btn_init.bind("<Button-1>", self.reset_dfs)
        self.btn_back.bind("<Button-1>", self.back_dir)
        self.btn_delete_dir.bind("<Button-1>", self.clicked_delete_dir)

        self.dir_actions.grid(column=4, row=0, sticky=N + S + W + E)

        self.file_actions = Frame(root)
        self.b1 = Button(self.file_actions, text="CREATE FILE")
        self.b8 = Button(self.file_actions, text="DELETE FILE")
        self.b2 = Button(self.file_actions, text="DOWNLOAD FILE")
        self.b3 = Button(self.file_actions, text="UPLOAD FILE")

        self.b1.bind("<Button-1>", self.clicked_file_create)
        self.b2.bind("<Button-1>", self.clicked_file_download)
        self.b3.bind("<Button-1>", self.clicked_file_upload)
        self.b8.bind("<Button-1>", self.clicked_file_delete)

        self.b1.pack(side="top", fill=X)
        self.b2.pack(side="top", fill=X)
        self.b3.pack(side="top", fill=X)
        self.b8.pack(side="top", fill=X)

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

        self.clicked_client_init(None)

    # GLOBAL ACTIONS
    def reset_dfs(self, event):
        request = Request(command='RESET', params={})
        response = self.client.connection.send(request)

        if response is not None:
            if response.status == 200:
                self.status.configure(text='OK', bg='green')
                self.directories.delete(0, self.directories.size())
                self.files.delete(0, self.files.size())
                self.files.insert(0, *response.data['FILES'])

                self.directories.delete(0, self.directories.size())
                self.directories.insert(0, *response.data['DIRS'])

                self.current_directory_path = [response.data['DIR']]
        else:
            self.status.configure(text='NO CONNECTION', bg='red')

    def clicked_client_init(self, event):

        if self.client.connection is None:
            self.client.connect(None)

        request = Request(command='CLIENT_INIT', params={})
        response = self.client.connection.send(request)

        if response.status == 200:
            self.status.configure(text='OK', bg='green')
            self.directories.delete(0, self.directories.size())
            self.files.delete(0, self.files.size())
            self.directories.insert(0, *response.data['DIRS'])
            self.files.insert(0, *response.data['FILES'])
            self.current_directory_path = [response.data['DIR']]
        else:
            self.status.configure(text='ERROR', bg='red')

    # DIRECTORY ACTIONS
    def clicked_dir(self, event):

        request = Request('DIR_OPEN',
                          {'dir_name': self.directories.get(ACTIVE), 'dir_path': self.current_directory_path}, )
        response = self.client.connection.send(request)

        if response.status == 200:
            self.current_directory_path.append(response.data['DIR'])
            request = Request('DIR_READ',
                              {'dir_name': self.directories.get(ACTIVE), 'dir_path': self.current_directory_path}, )
            response = self.client.connection.send(request)
            if response.status == 200:
                self.directories.delete(0, self.directories.size())
                self.directories.insert(0, *response.data['DIRS'])

                self.files.delete(0, self.files.size())
                self.files.insert(0, *response.data['FILES'])

    def back_dir(self, event):

        if len(self.current_directory_path) > 1:
            request = Request('DIR_OPEN', {
                'dir_name': self.current_directory_path[len(self.current_directory_path) - 2],
                'dir_path': self.current_directory_path[0:len(self.current_directory_path) - 2]}
                              )
            response = self.client.connection.send(request)

            if response.status == 200:
                del self.current_directory_path[-1]
                del self.current_directory_path[-1]
                self.current_directory_path.append(response.data['DIR'])
                request = Request('DIR_READ', {
                    'dir_name': self.current_directory_path[len(self.current_directory_path) - 1],
                    'dir_path': self.current_directory_path}, )
                response = self.client.connection.send(request)
                if response.status == 200:
                    self.directories.delete(0, self.directories.size())
                    self.directories.insert(0, *response.data['DIRS'])

                    self.files.delete(0, self.files.size())
                    self.files.insert(0, *response.data['FILES'])

    def clicked_create_dir(self, event):
        w = PopupWindow(root)
        self.b1["state"] = "disabled"
        root.wait_window(w.top)
        self.b1["state"] = "normal"

        request = Request('DIR_CREATE',
                          params={'dir_name': str(w.value), 'dir_path': self.current_directory_path})
        response = self.client.connection.send(request)

        if response.status == 201:
            self.status.configure(text='CREATED', bg='green')

            self.directories.delete(0, self.directories.size())
            self.directories.insert(0, *response.data['DIRS'])

            self.files.delete(0, self.files.size())
            self.files.insert(0, *response.data['FILES'])
        else:
            self.status.configure(text=response.message(), bg='red')

    def clicked_delete_dir(self, event):
        request = Request(command='DIR_DELETE', params={
            'dir_name': self.directories.get(ACTIVE),
            'dir_path': self.current_directory_path
        })
        response = self.client.connection.send(request)
        if response.status == 200:
            self.directories.delete(0, self.directories.size())
            self.directories.insert(0, *response.data['DIRS'])

            self.files.delete(0, self.files.size())
            self.files.insert(0, *response.data['FILES'])

    # FILE ACTIONS
    def clicked_file_create(self, event):

        w = PopupWindow(root)
        self.b1["state"] = "disabled"
        root.wait_window(w.top)
        self.b1["state"] = "normal"

        request = Request('FILE_CREATE',
                          params={'file_name': str(w.value), 'dir_path': self.current_directory_path, 'force': False})
        response = self.client.connection.send(request)
        if response is not None:
            if response.status == 203:
                self.status.configure(text='NEED CONFIRMATION', bg='yellow')
                answer = messagebox.askokcancel('File already exist',
                                                'File already exist, Do you want to replace it?')
                if answer is True:
                    request = Request('FILE_CREATE',
                                      params={'file_name': str(w.value), 'dir_path': self.current_directory_path,
                                              'force': True})
                    response = self.client.connection.send(request)

            if response.status == 201:
                self.status.configure(text='CREATED', bg='green')

                self.directories.delete(0, self.directories.size())
                self.directories.insert(0, *response.data['DIRS'])

                self.files.delete(0, self.files.size())
                self.files.insert(0, *response.data['FILES'])

        else:
            self.status.configure(text='NO CONNECTION', bg='red')

    def clicked_file_download(self, event):
        request = Request(command='FILE_DOWNLOAD', params={
            'file_name': self.files.get(ACTIVE),
            'dir_path': self.current_directory_path
        })
        response = self.client.connection.send(request)

        if response.status == 200:
            self.status.configure(text='OK', bg='green')

            sock = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
            sock.connect((response.data['IP'], response.data['PORT']))
            request = Request('FILE_DOWNLOAD_DIRECT',
                              {'file_name': self.files.get(ACTIVE), 'dir_path': self.current_directory_path})

            dirpath = filedialog.askdirectory()

            request_type, data_length, data = request.to_payloads()
            sock.send(request_type)
            ok, = unpack('I', sock.recv(4))
            assert ok == 0
            sock.send(data_length)
            ok, = unpack('I', sock.recv(4))
            assert ok == 0
            sock.send(data)
            ok, = unpack('I', sock.recv(4))
            assert ok == 0
            file = open(dirpath + '/' + self.files.get(ACTIVE), 'wb')
            recv_file(sock, file)
            sock.close()

        else:
            self.status.configure(text=response.message(), bg='red')

    def clicked_file_upload(self, event):
        fp = filedialog.askopenfile(mode='rb')
        request = Request(command='FILE_UPLOAD', params={
            'file_name': fp.name.split('/')[-1],
            'dir_path': self.current_directory_path
        })
        response = self.client.connection.send(request)

        if response.status == 200:
            self.status.configure(text='OK', bg='green')

            sock = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
            sock.connect((response.data['IP'], response.data['PORT']))
            request = Request('FILE_UPLOAD_DIRECT',
                              {'file_name': fp.name.split('/')[-1], 'dir_path': self.current_directory_path})

            request_type, data_length, data = request.to_payloads()
            sock.send(request_type)
            ok, = unpack('I', sock.recv(4))
            assert ok == 0
            sock.send(data_length)
            ok, = unpack('I', sock.recv(4))
            assert ok == 0
            sock.send(data)
            ok, = unpack('I', sock.recv(4))
            assert ok == 0

            send_file(sock, fp)
            sock.close()
            fp.close()

            sleep(2)
            request = Request('DIR_READ',
                              {'dir_name': self.current_directory_path[-1], 'dir_path': self.current_directory_path}, )
            response = self.client.connection.send(request)
            if response.status == 200:
                self.directories.delete(0, self.directories.size())
                self.directories.insert(0, *response.data['DIRS'])

                self.files.delete(0, self.files.size())
                self.files.insert(0, *response.data['FILES'])

        else:
            self.status.configure(text=response.message(), bg='red')

    def clicked_file_delete(self, event):
        request = Request(command='FILE_DELETE', params={
            'file_name': self.files.get(ACTIVE),
            'dir_path': self.current_directory_path
        })
        response = self.client.connection.send(request)
        if response.status == 200:
            self.directories.delete(0, self.directories.size())
            self.directories.insert(0, *response.data['DIRS'])

            self.files.delete(0, self.files.size())
            self.files.insert(0, *response.data['FILES'])


if __name__ == '__main__':
    root = Tk()
    main = MainWindow(root)
    root.mainloop()

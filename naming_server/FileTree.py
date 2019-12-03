import json


class FileTree:
    def __init__(self):
        self.file_name = 'file_tree.json'

        json_file = open(self.file_name, "r")
        json_data = json_file.read()
        self.file_tree = json.loads(json_data)

    def client_init(self):
        result = {
            'FILES': list(self.file_tree['DIRS']['root']['FILES'].keys()),
            'DIRS': list(self.file_tree['DIRS']['root']['DIRS'].keys()),
            'DIR': list(self.file_tree['DIRS'].keys())[0]
        }
        return result

    def reset(self):
        print(self.file_tree)
        self.file_tree['DIRS']['root']['FILES'] = {}
        self.file_tree['DIRS']['root']['DIRS'] = {}

        json_file = open(self.file_name, "w")
        print(self.file_tree)
        json.dump(self.file_tree, json_file)
        json_file.close()

        return self.client_init()

    def create_file(self, file_name, path: list):
        current_fs_dir = self.file_tree['DIRS']
        parent = {}

        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = current_fs_dir[key]['DIRS']
            else:
                return None

        parent['FILES'][file_name] = 'd41d8cd98f00b204e9800998ecf8427e'  # EMPTY HASH
        open(self.file_name, 'w').close()
        json_file = open(self.file_name, "w")
        json.dump(self.file_tree, json_file)
        json_file.close()

    def delete_file(self, file_name, path: list):
        current_fs_dir = self.file_tree['DIRS']
        parent = {}

        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = current_fs_dir[key]['DIRS']
            else:
                return None

        if file_name not in parent['FILES']:
            return None
        del parent['FILES'][file_name]
        open(self.file_name, 'w').close()
        json_file = open(self.file_name, "w")
        json.dump(self.file_tree, json_file)
        json_file.close()

    def insert_file(self, file_name, hash, path: list):
        current_fs_dir = self.file_tree['DIRS']
        parent = {}

        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = current_fs_dir[key]['DIRS']
            else:
                return None

        parent['FILES'][file_name] = hash
        open(self.file_name, 'w').close()
        json_file = open(self.file_name, "w")
        json.dump(self.file_tree, json_file)
        json_file.close()

        result = {
            'FILES': list(parent['FILES'].keys()),
            'DIRS': list(parent['DIRS'].keys()),
            'DIR': path[-1]
        }
        return result

    def insert_dir(self, dir_name, path: list):
        current_fs_dir = self.file_tree['DIRS']
        parent = {}
        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = parent['DIRS']
            else:
                return None

        current_fs_dir[dir_name] = {'FILES': {}, 'DIRS': {}}
        open(self.file_name, 'w').close()
        json_file = open(self.file_name, "w")
        json.dump(self.file_tree, json_file)
        json_file.close()

        result = {
            'FILES': list(parent['FILES'].keys()),
            'DIRS': list(parent['DIRS'].keys()),
            'DIR': path[-1]
        }
        return result

    def delete_dir(self, dir_name, path: list):
        current_fs_dir = self.file_tree['DIRS']
        parent = {}
        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = parent['DIRS']
            else:
                return None

        del current_fs_dir[dir_name]
        open(self.file_name, 'w').close()
        json_file = open(self.file_name, "w")
        json.dump(self.file_tree, json_file)
        json_file.close()

        result = {
            'FILES': list(parent['FILES'].keys()),
            'DIRS': list(parent['DIRS'].keys()),
            'DIR': path[-1]
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

    def file_found(self, file_name, path: list):
        current_fs_dir = self.file_tree['DIRS']
        parent = {}

        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = current_fs_dir[key]['DIRS']
            else:
                return False
        if file_name == '':
            return False
        if file_name in parent['FILES']:
            return True
        else:
            return False

    def path_exists(self, path: list):
        current_fs_dir = self.file_tree['DIRS']
        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                current_fs_dir = current_fs_dir[key]['DIRS']
            else:
                return False
        return True

    def get_file_hash(self, file_name, path: list):
        current_fs_dir = self.file_tree['DIRS']
        parent = {}

        for index in range(len(path)):
            if path[index] in current_fs_dir:
                key = path[index]
                parent = current_fs_dir[key]
                current_fs_dir = current_fs_dir[key]['DIRS']
            else:
                return None

        if file_name in parent['FILES']:
            return parent['FILES'][file_name]
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

import io
from io import TextIOWrapper
from io import BufferedReader
from petrel_client.client import Client
import copy
import os


class DumpWriterFakeBufIO:
    def __init__(self, client, path, is_str_data=False):
        self.client = client
        self.path = path
        self.is_str_data = is_str_data

    def write(self, content):
        if self.is_str_data:
            try:
                str_data = str(content)
                if str_data[-1] != '\n':
                    str_data += '\n'
                self.client.put(self.path, bytes(str_data.encode("utf-8")))
            except:
                raise TypeError("check your data, require string")
        else:
            assert isinstance(content, bytes)
            self.client.put(self.path, content)

    def __enter__(self):
        return self

    def __exit__(self, *argv):
        pass


class BufferedReaderWrapper(BufferedReader):
    def read(self, *args, **kwargs):
        try:
            return super(BufferedReaderWrapper, self).read(*args, **kwargs)
        except ValueError:
            return b''


class TextIOWrapperWrapper(TextIOWrapper):
    def readline(self, *args, **kwargs):
        try:
            return super(TextIOWrapperWrapper, self).readline(*args, **kwargs)
        except ValueError:
            return ""

def singleton(cls):
    _instance = {}
    def _singleton(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]
    return _singleton

@singleton
class PetrelHelper:
    def __init__(self, conf_file=None, data_root=None):
        if conf_file is None:
            cur_file_path = os.path.realpath(__file__)
            cur_dir = os.path.dirname(cur_file_path)
            conf_file = os.path.join(cur_dir, "petrel_oss.conf")
            if not os.path.exists(conf_file):
                raise FileNotFoundError("Please give a valid conf file for PetrelSDK!")
        self.conf_file = conf_file
        self.client = Client(conf_file)
        self._raw_stream_data_buf = None
        self.data_root = None
        self.file_entrys = None
        if data_root is not None:
            self.data_root = data_root
            self.file_entrys = self.list_dir(self.data_root, with_info=True)

    @staticmethod
    def fix_path(path_str):
        try:
            st_ = str(path_str)
            if "s3://" in st_:
                return  st_
            if "s3:/" in st_:
                st_ = "s3://" + st_.strip('s3:/')
                return st_
            else:
                st_ = "s3://" + st_
                return st_
        except:
            raise TypeError

    def exist(self, file_path, refresh=False):
        if self.data_root is None:
            raise IndexError("No initialized path set!")
        if refresh:
            self.file_entrys = self.list_dir(self.data_root, with_info=True)
        pure_name = self.fix_path(file_path).strip("s3://")
        if pure_name in self.file_entrys.keys():
            return True
        else:
            return False

    def list_dir(self, s3_dir, with_info=False):
        s3_dir = self.fix_path(s3_dir)
        files_iter = self.client.get_file_iterator(s3_dir)
        if with_info:
            file_list = {p: k for p, k in files_iter}
        else:
            file_list = [p for p, k in files_iter]
        return file_list

    def open(self, url, code='r'):
        url = self.fix_path(url)
        # print("[debug]url=", url)
        if code == 'r':
            stream = self.client.get(url, enable_stream=True)
            # stream.set_socket_timeout(5)
            if stream is None:
                raise FileNotFoundError("Please check your s3 path!")
            self._raw_stream_data_buf = copy.deepcopy(stream._raw_stream.data)
            buf_reader = BufferedReaderWrapper(io.BytesIO(self._raw_stream_data_buf))
            return TextIOWrapperWrapper(buf_reader)
        if code == 'rb':
            stream = self.client.get(url, enable_stream=True)
            buf_reader = BufferedReaderWrapper(stream._raw_stream)
            return buf_reader
        if code == 'w':
            return DumpWriterFakeBufIO(self.client, url, is_str_data=True)
        if code == 'wb':
            return DumpWriterFakeBufIO(self.client, url)
        else:
            raise NotImplementedError


if __name__ == '__main__':
    print("===============test binary===============")
    import pickle
    bin_data = [1, 2, 2]
    p = PetrelHelper("/home/xlju/petreloss.conf")
    test_file = "s3://mybucket/test_write_bin.bin"
    with p.open(test_file, 'wb') as f:
        pickle.dump(bin_data, f)
    with p.open(test_file, 'rb') as f:
        dd = pickle.load(f)
        print(dd)

    # test txt
    print("===============test txt===============")
    test_file = "s3://mybucket/test_write_txt.txt"
    with p.open(test_file, 'w') as f:
        f.write("hhhhhhhhh\n tttt\n")
    with p.open("test_file", 'r') as f:
        a = f.readlines()
        print(a)
    with p.open("test_file", 'r') as f:
        a = f.readline()
        print(a)

'''
Examples
'''

# def PetrelByteStreamProxy(file_name):
#     if file_name.startswith('s3'):
#         from petrel_helper import PetrelHelper
#         ph = PetrelHelper()
#         f = ph.open(file_name, 'rb')
#     else:
#         f = open(file_name, 'rb')
#     return f

# def PetrelBytesProxy(file_name):
#     f = PetrelByteStreamProxy(file_name)
#     return f.read()
    
# def PetrelTextProxy(file_name):
#     if file_name.startswith('s3'):
#         from petrel_helper import PetrelHelper
#         ph = PetrelHelper()
#         f = ph.open(file_name, 'r')
#         return f
#     else:
#         return open(file_name, 'r')
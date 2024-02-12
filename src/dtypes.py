from abc import ABC, abstractmethod
import struct


def from_string(json_columns):
    res = []
    for dt in json_columns:
        match dt['type']:
            case 'Smallint':
                res.append(Smallint())
            case 'Integer':
                res.append(Integer())
            case 'Numeric':
                precision = dt['precision']
                scale = dt['scale']
                res.append(Numeric(precision, scale))
            case 'Varchar':
                precision = dt['precision']
                res.append(Varchar(precision))
            case _:
                raise Exception
    return res


class Dtype(ABC):
    """
    Abstract class for all avaiable data types in tomdb-2

    """

    @abstractmethod
    def serialize(self, value):
        pass

    @abstractmethod
    def get_byte_size(self):
        pass

    @abstractmethod
    def deserialize(self, item: bytes):
        pass

    def get_precision(self):
        return getattr(self, 'precision', None)

    def get_scale(self):
        return getattr(self, 'scale', None)

    def __str__(self):
        name = getattr(self, 'name', None)
        return f'{name}'


class Integer(Dtype):

    def __init__(self):
        self.name = 'Integer'
        self.bytes = 4

    def serialize(self, value: int):
        return struct.pack('i', value)

    def get_byte_size(self):
        return self.bytes

    def deserialize(self, item: bytes):
        return struct.unpack('i', item)


class Smallint(Dtype):

    def __init__(self):
        self.name = 'Smallint'
        self.bytes = 2

    def serialize(self, value: int):
        return struct.pack('h', value)

    def get_byte_size(self):
        return self.bytes

    def deserialize(self, item: bytes):
        return struct.unpack('h', item)[0]


class Numeric(Dtype):

    def __init__(self, precision: int, scale: int):
        self.name = 'Numeric'
        self.precision = precision
        self.scale = scale

    def serialize(self, value: float):
        value = round(value, self.precision)
        return struct.pack('f', value)

    def get_byte_size(self):
        return self.precision

    def deserialize(self, item: bytes):
        return struct.unpack('f', item)[0]


class Varchar(Dtype):

    def __init__(self, bytes: int):
        self.name = f'Varchar'
        self.precision = bytes

    def serialize(self, value: str):
        byte_string = value.encode('utf-8')
        length = min(len(byte_string), self.precision)
        packed = struct.pack(f'{length}s', byte_string[:length])
        packed += b'\00' * (self.precision - length)
        return packed

    def get_byte_size(self):
        return self.precision

    def deserialize(self, item: bytes):
        string = item.split(b'\x00', 1)[0].decode('utf-8')
        return string

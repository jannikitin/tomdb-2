"""
This module provides types annotation for right data encoding
"""
import io
from abc import ABC, abstractmethod
import struct
from typing import List, Dict, Any
from exceptions import InvalidType, Overflow


# Data type adding algorithm:
# 1. Declare dtype class, derived from Dtype, or, if it array-like structure, from corresponding single type
# 2. Teach "from_string" function how to define it from JSON
# 3. Recheck all methods should be implimented


class Dtype(ABC):
    """
    Abstract class for all avaiable data types in tomdb-2

    """

    @abstractmethod
    def serialize(self, value) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, item: io.BytesIO):
        pass

    @abstractmethod
    def validate(self, item) -> None:
        pass

    def get_precision(self):
        return getattr(self, 'precision', None)

    def get_scale(self):
        return getattr(self, 'scale', None)

    def __str__(self):
        name = getattr(self, 'name', None)
        return f'{name}'

    def get_byte_size(self):
        byte_size = getattr(self, 'bytes', False)
        if not byte_size:
            byte_size = self.get_precision()
        return byte_size

    @classmethod
    def from_string(cls, json_columns: List[Dict[Any, Any]]) -> List:
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
                case 'Char':
                    precision = dt['precision']
                    res.append(Char(precision))
                case 'Integer[]':
                    res.append(IntegerArray())
                case _:
                    raise Exception
        return res


class Integer(Dtype):
    """
    Four-bytes integer
    """

    def __init__(self):
        self.name = 'Integer'
        self.bytes = 4

    def serialize(self, value: int) -> bytes:
        return struct.pack('i', value)

    def deserialize(self, buffer: io.BytesIO) -> int:
        return struct.unpack('i', buffer.read(self.bytes))[0]

    def validate(self, item: int) -> None:
        if isinstance(item, int):
            if -(2 ** 31) < item > (2 ** 31) - 1:
                raise Overflow(f'{item} is not int32')
        else:
            raise InvalidType(f'{item} is not int32 type')


class IntegerArray(Integer):

    def __init__(self):
        super().__init__()
        self.name = 'Integer[]'
        self.bytes = 4

    def serialize(self, value: List[int]) -> bytes:
        """
        Returns bytes in form len(arr) * 4 + 1 * 4: first goes array length,
        then - encoded integers one-by one
        """
        ser_array = b''
        ser_array += struct.pack('i', len(value))
        for item in value:
            ser_array += struct.pack('i', item)
        return ser_array

    def deserialize(self, buffer: io.BytesIO) -> List[int]:
        length = struct.unpack('i', buffer.read(self.bytes))[0]
        arr = []
        for _ in range(length):
            arr.append(struct.unpack('i', buffer.read(self.bytes))[0])
        return arr

    def validate(self, item: List[int]) -> None:
        if not isinstance(item, list):
            raise InvalidType(f'{item} is not a array-like object')
        for i, num in enumerate(item):
            try:
                super().validate(num)
            except Overflow as e:
                raise Overflow(msg=e.args[0], pos=i)
            except InvalidType:
                raise InvalidType(msg='Array contains incorrect data', pos=i)


class Smallint(Dtype):
    """
    Two-bytes integer
    """

    def __init__(self):
        self.name = 'Smallint'
        self.bytes = 2

    def serialize(self, value: int) -> bytes:
        return struct.pack('h', value)

    def deserialize(self, buffer: io.BytesIO) -> int:
        return struct.unpack('h', buffer.read(self.bytes))[0]

    def validate(self, item: int) -> None:
        if isinstance(item, int):
            if -(2 ** 15) < item > (2 ** 15) - 1:
                raise Overflow(f'{item} is not int32')
        else:
            raise InvalidType(f'{item} is not int32 type')


class Numeric(Dtype):
    """
    Float number with varying precision and scale. Precision by default is 4, scale by default is 2
    """

    def __init__(self, precision: int = 4, scale: int = 2):
        self.name = 'Numeric'
        self.precision = precision
        self.scale = scale

    def serialize(self, value: float) -> bytes:
        value = round(value, self.precision)
        return struct.pack('f', value)

    def get_byte_size(self) -> int:
        return self.precision

    def deserialize(self, buffer: io.BytesIO) -> float:
        return struct.unpack('f', buffer.read(self.precision))[0]

    def validate(self, item: float) -> None:
        if not isinstance(item, float) and not isinstance(item, int):
            raise InvalidType(f'{item} is float nor int')


class Varchar(Dtype):
    """
    An array of characters with max length of precision. 32 by default
    """

    def __init__(self, precision: int = 32, encode: str = 'utf-8'):
        self.name = f'Varchar'
        self.precision = precision
        self.encode = encode

    def serialize(self, value: str) -> bytes:
        byte_string = value.encode(self.encode)
        length = min(len(byte_string), self.precision)
        packed = struct.pack(f'{length}s', byte_string[:length])
        packed += b'\00' * (self.precision - length)
        return packed

    def deserialize(self, buffer: io.BytesIO) -> str:
        string = buffer.read(self.precision).split(b'\x00', 1)[0].decode(self.encode)
        return string

    def validate(self, item: str) -> None:
        pass


class Char(Dtype):
    MAX_SIZE = 255

    def __init__(self, precision: int = 1, encode: str = 'utf-8'):
        self.name = 'Char'
        self.precision = min(precision, Char.MAX_SIZE)
        self.encode = encode

    def serialize(self, value: str) -> bytes:
        byte_string = value.encode(self.encode)
        packed = struct.pack(f'{self.precision}s', byte_string)
        return packed

    def deserialize(self, buffer: io.BytesIO) -> str:
        string = buffer.read(self.precision).decode(self.encode)
        return string

    def validate(self, item) -> None:
        pass
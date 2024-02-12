import struct
import io

# Определяем таблицу данных
table = [
    [1, 'str', 2.33, [1, 2, 3]],
    [2, 'str_str', 3.41, [2.4, 5, 6]]
]


# Функция для сериализации одной записи
def serialize_record(record):
    # Подготавливаем буфер для бинарных данных
    buffer = io.BytesIO()

    # id (smallint)
    buffer.write(struct.pack('h', record[0]))

    # string (varchar(10))
    string_bytes = record[1].encode('utf-8')
    string_length = min(len(string_bytes), 10)
    buffer.write(struct.pack(f'{string_length}s', string_bytes[:10]))
    buffer.write(b'\x00' * (10 - string_length))  # Дополняем нулями до 10 байт

    # price (numeric(4,2))
    buffer.write(struct.pack('f', record[2]))

    # arr (numeric[])
    array = record[3]
    buffer.write(struct.pack('I', len(array)))  # Сначала записываем длину массива
    for item in array:
        buffer.write(struct.pack('f', item))  # Затем записываем элементы массива

    return buffer.getvalue()


# Функция для десериализации одной записи из бинарного потока
def deserialize_record(buffer):
    # Читаем id (smallint)
    id, = struct.unpack('h', buffer.read(2))

    # Читаем string (varchar(10))
    string = buffer.read(10).split(b'\x00', 1)[0].decode('utf-8')

    # Читаем price (float)
    price, = struct.unpack('f', buffer.read(4))

    # Читаем массив (начинаем с длины массива)
    arr_length, = struct.unpack('I', buffer.read(4))
    arr = [struct.unpack('f', buffer.read(4))[0] for _ in range(arr_length)]

    return [id, string, price, arr]


# Функция для чтения всего файла и десериализации его содержимого
def read_table_from_binary_file(filename):
    table = []
    with open(filename, 'rb') as bin_file:
        # Подготавливаем буфер для чтения данных
        buffer = io.BytesIO(bin_file.read())

        while buffer.tell() < len(buffer.getvalue()):
            record = deserialize_record(buffer)
            table.append(record)
    return table


# Читаем таблицу из бинарного файла и выводим результат
format = {'columns':
            [{'name': 'int_obj', 'type': 'smallint'},
            {'name': 'strings', 'type': 'varchar', 'length': '10'},
            {'name': 'floats', 'type': 'numeric', 'length': '4'},
             {'name': 'array', 'type': 'array', 'element_type': 'numeric', 'length': '2'}]}
file = 'config.json'
recorder.json_dump(file, format)
cfg = recorder.json_read(file)
row = [1,'fds',9.22, [1.0,222.0,43.0]]
s_row = recorder.serializer(row, cfg)
with open('table.bin', 'ab') as f:
    f.write(s_row)

t = read_table_from_binary_file('table.bin')
print(t)
#write row
import io
import struct
import json


def serializer(row, cfg):
    columns = cfg['columns']
    sizes = [4, 10, 4, 4]
    buffer = io.BytesIO()
    buffer.write(struct.pack('h', row[0]))

    b_encoded = row[1].encode('utf-8')
    s_length = min(len(b_encoded), sizes[1])
    buffer.write(struct.pack(f'{s_length}s', b_encoded))
    buffer.write(b'\x00' * (10 - s_length))

    buffer.write(struct.pack('f', row[2]))

    buffer.write(struct.pack('I', len(row[3])))
    for item in row[3]:
        buffer.write(struct.pack('f', item))

    return buffer.getvalue()

def deserializer(buffer):
    pass


def json_dump(f, d):
    js_obj = json.dumps(d)
    with open(f, 'w') as f:
        f.write(js_obj)


def read_config(f):
    d = json.load(f)
    return d


def json_read(f):
    with open(f, 'r') as f:
        obj = json.load(f)
    return obj
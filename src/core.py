"""
Core component of tomdb-2 ecosystem.
This module performs basic IO operations, files with .tdb extension reading and writing, appending data, etc.

"""
import io
from pathlib import Path
import os
from typing import List, Any

from table import Table
from console import CONNECTION
from fileop import FileOperator, JsonOperator
from dtypes import Dtype, from_string


class IBin:
    """
    IBin performs binary IO oeprations for specific .tdb extension

    """
    @classmethod
    def serialize(cls, row: List[Any], dtypes: List[Dtype]) -> bytes:
        """

        :param row: data to serialize (List)
        :param dtypes: list of corresponding dtypes for each item in row. len(row) == len(dtypes)
        :return: returns buffer of serialized items (heterogeneous row of table)
        """
        row_zip = zip(row, dtypes)
        buffer = io.BytesIO()
        for item in row_zip:
            ser_item = item[1].serialize(value=item[0])
            buffer.write(ser_item)
        return buffer.getvalue()

    @classmethod
    def deserialize(cls, buffer: io.BytesIO, dtypes: List[Dtype]) -> List:
        row_length = 0
        for dt in dtypes:
            row_length += dt.get_byte_size()
        result = []
        for dt in dtypes:
            result.append(dt.deserialize(buffer.read(dt.get_byte_size())))

        return result


def add_table_to_schema(table: Table) -> None:
    destination_path = f'../bin/{CONNECTION}/{table.schema}/{table.name}/'
    table_config_path = destination_path + f'{table.uid}.json'
    table_log_path = destination_path + f'{table.uid}.log'
    table_bin_path = destination_path + f'{table.uid}.tdb'
    if not os.path.exists(destination_path):
        Path(destination_path).mkdir()
    with open(file=table_bin_path, mode='wb') as f:
        dtypes = table.dtypes.values()
        for row in table.data:
            row = IBin.serialize(row, dtypes)
            f.write(row)
    JsonOperator.generate_json(table_config_path, table)
    with open(file=table_log_path, mode='w') as f:
        f.write('table_created')


def read_table(path):
    json = os.path.dirname(path) + '/' + os.path.basename(path)[0:-4] + '.json'
    with open(path, 'rb') as f:
        buffer = io.BytesIO(f.read())
    json_obj = JsonOperator.get_json(json)
    data = []
    columns = [col['name'] for col in (col for col in json_obj['columns'])]
    pure_dtypes = from_string(json_obj['columns'])
    dtypes = {col: dtype for col, dtype in zip(columns, pure_dtypes)}
    while buffer.tell() < len(buffer.getvalue()):
        row = IBin.deserialize(buffer, pure_dtypes)
        data.append(row)

    return Table(data=data, schema=json_obj['schema'], uid=json_obj['uid'], name=json_obj['name'],
                 dtypes=dtypes, columns=columns)



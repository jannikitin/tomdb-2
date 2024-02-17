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
from fileop import JsonOperator
from dtypes import Dtype, from_string
from exceptions import ObjectNotFound


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
        """
        :param buffer: readed bin fyle as io.BytesIO object
        :param dtypes: list of corresponding to each index position dtypes
        """
        result = []
        for dt in dtypes:
            result.append(dt.deserialize(buffer))

        return result
# EOC


def add_table_to_schema(table: Table) -> None:
    """
    Include: json, log and bin data files creation and appending table in schema. If schema doesn't exist,
    it'll be throw an error

    """
    destination_path = f'../bin/{CONNECTION}/{table.schema}/{table.name}/'
    table_config_path = destination_path + f'{table.uid}.json'
    table_log_path = destination_path + f'{table.uid}.log'
    table_bin_path = destination_path + f'{table.uid}.tdb'
    # Create directory for new table in schema folder
    if not os.path.exists(destination_path):
        Path(destination_path).mkdir()
    with open(file=table_bin_path, mode='wb') as f:  # Retrieve data from table row by row -
        dtypes = table.dtypes.values()  # - and serialize it with corresponding data type
        for row in table.data:
            row = IBin.serialize(row, dtypes)
            f.write(row)
    JsonOperator.generate_table_json_config(table_config_path, table_bin_path, table)
    with open(file=table_log_path, mode='w') as f:
        f.write('table_created')
    JsonOperator.append_table_in_schema_config(table_config_path)


def read_table(schema: str, table: str):
    try:
        path = find_table_in_json(schema, table)
    except ObjectNotFound as e:
        raise e
    # Understand JSON-configure path from table path
    json_path = os.path.dirname(path) + '/' + os.path.basename(path)[0:-4] + '.json'
    with open(path, 'rb') as f:  # read table to buffer
        buffer = io.BytesIO(f.read())
    json_obj = JsonOperator.get_json(json_path)
    data = []
    columns = [col['name'] for col in (col for col in json_obj['columns'])]
    pure_dtypes = from_string(json_obj['columns'])
    dtypes = {col: dtype for col, dtype in zip(columns, pure_dtypes)}
    # Deserialize data sequentialy row-by-row
    while buffer.tell() < len(buffer.getvalue()):
        row = IBin.deserialize(buffer, pure_dtypes)
        data.append(row)

    return Table(data=data, schema=json_obj['schema'], uid=json_obj['uid'], name=json_obj['name'],
                 dtypes=dtypes, columns=columns)


def find_table_in_json(schema: str, table: str) -> str:
    schema_path = f'../bin/tsk/{schema}/config.json'
    try:
        json_obj = JsonOperator.get_json(schema_path)
    except FileNotFoundError as e:
        raise ObjectNotFound(f'Object {schema} does not exist. Path {schema_path} does not recognizeble')
    tables = json_obj['tables']
    for t in tables:
        if t['table_name'] == table:
            return t['bin_path']
    raise ObjectNotFound(f'Object {table} does not exist')

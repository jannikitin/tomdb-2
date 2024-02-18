"""
Core component of tomdb-2 ecosystem.
This module performs basic IO operations, files with .tdb extension reading and writing, appending data, etc.

"""
import io
from pathlib import Path
import os
from typing import List, Any, Dict

from table import Table
from console import CONNECTION, SCHEMA
from fileop import JsonOperator
from dtypes import Dtype
from exceptions import ObjectNotFound


class IOBin:
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


class TableOperator:

    def __init__(self, table: Table = None):
        self.table = table

    def add_table_to_schema(self) -> None:
        """
        Include: json, log and bin data files creation and appending table in schema. If schema doesn't exist,
        it'll be throw an error

        """
        # Create directory for new table in schema folder
        if not os.path.exists(self.table.folder_path):
            Path(self.table.folder_path).mkdir()
        with open(file=self.table.bin_path, mode='wb') as f:  # Retrieve data from table row by row
            dtypes = list(self.table.dtypes.values())  # and serialize it with corresponding data type
            for row in self.table:
                row = IOBin.serialize(row, dtypes)
                f.write(row)
        JsonOperator.generate_table_json_config(self.table.json_cfg_path, self.table.bin_path, self.table)
        with open(file=self.table.log_path, mode='w') as f:
            f.write('table_created')
        JsonOperator.append_table_in_schema_config(self.table.json_cfg_path)

    def read_table(self, name: str):
        try:
            cfg_path = self.find_table_in_json(name)
        except ObjectNotFound as e:
            raise e
        # Understand JSON-configure path from table path
        json_obj = JsonOperator.get_json(cfg_path)
        columns, pure_dtypes, dtypes = self.parse_table_attrs(json_obj['columns'])
        data = []
        # Deserialize data sequentialy row-by-row
        with open(json_obj['bin_path'], 'rb') as f:  # read table to buffer
            buffer = io.BytesIO(f.read())
        while buffer.tell() < len(buffer.getvalue()):
            row = IOBin.deserialize(buffer, pure_dtypes)
            data.append(row)

        self.table = Table(data=data, schema=json_obj['schema'], uid=json_obj['uid'], name=json_obj['name'],
                           dtypes=dtypes, columns=columns, validate=False)

    @staticmethod
    def parse_table_attrs(json_columns: List[Dict[Any, Any]]) -> tuple:
        """
        Retrieve columns and corresponding data types from json_columns list
        """
        cols = [col['name'] for col in (col for col in json_columns)]
        pure_dtypes = Dtype.from_string(json_columns)
        dtypes = {col: dtype for col, dtype in zip(cols, pure_dtypes)}
        return cols, pure_dtypes, dtypes

    @staticmethod
    def find_table_in_json(name) -> str:
        schema_path = f'../bin/tsk/{SCHEMA}/config.json'
        try:
            json_obj = JsonOperator.get_json(schema_path)
        except FileNotFoundError as e:
            raise ObjectNotFound(f'Object {SCHEMA} does not exist. Path {schema_path} does not recognizeble')
        tables = json_obj['tables']
        for t in tables:
            if t['table_name'] == name:
                return t['cfg_path']
        raise ObjectNotFound(f'Object {name} does not exist')

    def drop_table(self, table: str):
        try:
            cfg_path = self.find_table_in_json(table)
        except ObjectNotFound as e:
            return e
        folder = os.path.dirname(cfg_path)
        if os.name == 'posix':
            os.system(f'rm -r {folder}')
        JsonOperator.delete_table(SCHEMA, table)

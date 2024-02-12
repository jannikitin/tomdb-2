import io
import json
import os
from typing import List, Dict, Any
import struct

from table import Table
from dtypes import Dtype


class FileOperator:
    """
    This module gives access to the ecosystem of tomdb-2 to all config files with .tdb extension and other,
    not related with table data files

    """
    # version - 10 bytes string, current_uid_position(free) - 5 bytes string, current_connection_name - 32 bytes string'
    CURRENT_CONFIG = '../bin/tsk/config.tdb'

    @classmethod
    def generate_connection_config(cls):
        if os.path.exists(cls.CURRENT_CONFIG):
            return
        buffer = io.BytesIO()

        buffer.write(struct.pack('5s', '0.0.1'.encode('utf-8')))
        buffer.write(b'\00' * 5)
        buffer.write(struct.pack('5s', '00001'.encode('utf-8')))
        buffer.write(struct.pack('3s', 'tsk'.encode('utf-8')))
        buffer.write(b'\00' * 29)

        with open(cls.CURRENT_CONFIG, 'wb') as f:
            f.write(buffer.getvalue())

    @classmethod
    def update_version(cls, MJ=0, MN=0, PATCH=0):
        pass

    @classmethod
    def generate_uid(cls):
        with open(cls.CURRENT_CONFIG, 'rb') as f:
            buffer = io.BytesIO(f.read())
        _ = buffer.read(10)
        uid = buffer.read(5).split(b'\x00')[0].decode('utf-8')
        uid = int(uid)
        uid += 1
        s_uid = '0' * (4 - len(str(uid))) + str(uid)
        return s_uid


class JsonOperator:
    """
    This module provides access to all json-configs and help to retrieve usefull data from them
    """

    JSON_TABLE_FORM = {'uid': str,
                       'name': str,
                       'columns': List[Dict[str, Any]],
                       'schema': str,
                       'path': str}
    COLUMNS_FORM = {'name': str, 'type': str, 'precision': int, 'scale': int}
    JSON_SCHEMA_FORM = {}

    @classmethod
    def get_json(cls, path: str):
        if not os.path.exists(path):
            raise FileExistsError
        with open(path, 'r') as f:
            json_obj = json.load(f)
        return json_obj

    @classmethod
    def generate_json(cls, path: str, table: Table) -> None:
        json_template = cls.JSON_TABLE_FORM.copy()
        json_template['uid'] = table.uid
        json_template['name'] = table.name
        json_template['columns'] = cls.generate_json_columns_encryption(table.dtypes)
        json_template['schema'] = table.schema
        json_template['path'] = path
        json_obj = json.dumps(json_template)
        with open(path, 'w') as f:
            f.write(json_obj)

    @classmethod
    def generate_json_columns_encryption(cls, dtypes: Dict[str, Dtype]) -> List:
        columns = []
        for key in dtypes:
            columns_template = cls.COLUMNS_FORM.copy()
            columns_template['name'] = key
            columns_template['type'] = dtypes[key].name
            columns_template['precision'] = dtypes[key].get_precision()
            columns_template['scale'] = dtypes[key].get_scale()

            columns.append(columns_template)
        return columns
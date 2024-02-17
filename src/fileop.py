import os
from typing import List, Dict, Any
from pathlib import Path

import json

from table import Table
from dtypes import Dtype


class JsonOperator:
    """
    This module provides access to all json-configs and help to retrieve usefull data from them
    """

    JSON_TABLE_FORM: Dict[Any, Any] = {'uid': str,
                                       'name': str,
                                       'columns': List[Dict[str, Any]],
                                       'schema': str,
                                       'cfg_path': str,
                                       'bin_path': str}
    COLUMNS_FORM: Dict[Any, Any] = {'name': str, 'type': str, 'precision': int, 'scale': int}
    NEW_JSON_SCHEMA_FORM: Dict[Any, Any] = {'uid': str, 'name': str, 'tables': []}
    JSON_TABLES_IN_SCHEMA_FORM: Dict[Any, Any] = {'table_uid': str, 'table_name': str, 'cfg_path': str, 'bin_path': str}
    JSON_CONNECTION_SETUP_FORM: Dict[Any, Any] = {'current_available_uid': str,
                                                  '__version__': str}
    CURRENT_CONFIG = '../bin/tsk/config.json'
    UID_LENGTH = 5

    @classmethod
    def get_json(cls, path: str) -> Dict:
        try:
            with open(path, 'r') as f:
                json_obj = json.load(f)
        except FileNotFoundError as e:
            raise e
        return json_obj

    @classmethod
    def write_json(cls, path: str, obj: dict):
        json_obj = json.dumps(obj)
        with open(path, 'w') as f:
            f.write(json_obj)

    @classmethod
    def generate_table_json_config(cls, json_path: str, bin_path: str, table: Table) -> None:
        json_template = cls.JSON_TABLE_FORM.copy()
        json_template['uid'] = table.uid
        json_template['name'] = table.name
        json_template['columns'] = cls.generate_json_columns_encryption(table.dtypes)
        json_template['schema'] = table.schema
        json_template['cfg_path'] = json_path
        json_template['bin_path'] = bin_path

        json_obj = json.dumps(json_template)
        with open(json_path, 'w') as f:
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

    @classmethod
    def generate_schema_json_config(cls, name: str):
        temp_schema = cls.NEW_JSON_SCHEMA_FORM.copy()
        temp_schema['name'] = name
        temp_schema['uid'] = cls.generate_uid()
        folder = f'../bin/tsk/{name}/'
        if not os.path.exists(folder):
            Path(folder).mkdir()
        cls.write_json(f'{folder}config.json', temp_schema)

    @classmethod
    def append_table_in_schema_config(cls, table_config: str):
        table_cfg = cls.get_json(table_config)
        schema_path = f'../bin/tsk/{table_cfg["schema"]}/config.json'
        schema_cfg = cls.get_json(schema_path)
        temp_tables_form = cls.JSON_TABLES_IN_SCHEMA_FORM.copy()
        temp_tables_form['table_uid'] = table_cfg['uid']
        temp_tables_form['table_name'] = table_cfg['name']
        temp_tables_form['cfg_path'] = table_cfg['cfg_path']
        temp_tables_form['bin_path'] = table_cfg['bin_path']

        schema_cfg['tables'].append(temp_tables_form)
        cls.write_json(schema_path, schema_cfg)

    @classmethod
    def generate_connection_json_setup_config(cls, version: str, uid_pos='00001'):
        json_temp = cls.JSON_CONNECTION_SETUP_FORM.copy()
        json_temp['current_available_uid'] = uid_pos
        json_temp['__version__'] = version
        with open(cls.CURRENT_CONFIG, 'w') as f:
            json_obj = json.dumps(json_temp)
            f.write(json_obj)

    @classmethod
    def generate_uid(cls):
        cfg = cls.get_json(cls.CURRENT_CONFIG)
        uid = cfg['current_available_uid']
        new_uid = '0' * (cls.UID_LENGTH - len(str(int(uid)))) + str(int(uid) + 1)
        cls.generate_connection_json_setup_config(uid_pos=new_uid, version=cfg['__version__'])
        return uid

    @classmethod
    def delete_table(cls, schema: str, table_name: str):
        cfg_path = f'../bin/tsk/{schema}/config.json'
        cfg = cls.get_json(cfg_path)
        for i, table in enumerate(cfg['tables']):
            if table['table_name'] == table_name:
                del (cfg['tables'][i])
        cls.write_json(cfg_path, cfg)

import functools

import tabulate

from typing import List, Dict, Any
from dtypes import Dtype
from pandas import Series
from exceptions import Overflow, InvalidType


def validator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not kwargs['validate']:
            return func(*args, *kwargs)
        print('validiruem')
        dtypes: Dict[Any, Dtype] = kwargs['dtypes']
        columns = kwargs['columns']
        for row in kwargs['data']:
            for i, col in enumerate(columns):
                try:
                    dtypes[col].validate(row[i])
                except Overflow as e:
                    raise e
                except InvalidType as e:
                    raise e
        return func(*args, **kwargs)
    return wrapper


class Table:

    @validator
    def __init__(self, data: List[Any], schema: str, uid: str,
                 name: str, dtypes: Dict[Any, Dtype], columns: List['str'], validate: bool = False):
        self.data: List[Series] = data
        self.schema = schema
        self.uid = uid
        self.name = name
        self.dtypes = dtypes
        self.columns = columns

    @property
    def shape(self):
        return len(self.columns), len(self.data)

    def print(self, j = 5):
        j = self.shape[0] if j > self.shape[1] else j
        headers = self.columns
        print(tabulate.tabulate(self.data[:j], headers))

    def __str__(self):
        return f'Name: {self.name}\nColumns: {self.columns}, dtypes: {self.dtypes}, size: {self.shape}'

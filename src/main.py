import pandas as pd

from table import Table
from fileop import JsonOperator
import core
import dtypes
import numpy as np
import time
from console import SCHEMA


def generate_test_table(table_name='table-1', i=1000, schema=SCHEMA, valideta=False):
    chars = [str(2) for _ in range(i)]
    ints = [i for i in range(i)]
    var10 = [str(i) * 9 for i in range(i)]
    floats = np.linspace(start=1, stop=100, num=i).tolist()
    int_arrays = [[i for i in range(10)] for _ in range(i)]
    data = [chars, ints, var10, floats, int_arrays]
    columns = ['col1', 'col2', 'col3', 'col4', 'col5']
    dtypes1 = {x: dtype for x, dtype in
               zip(columns, [dtypes.Char(1), dtypes.Integer(),
                             dtypes.Varchar(10), dtypes.Numeric(precision=4, scale=2), dtypes.IntegerArray()])}
    data = [[x[i] for x in data] for i in range(i)]
    start = time.time()
    t = Table(data=data, schema=schema, uid=JsonOperator.generate_uid(),
              name=table_name, dtypes=dtypes1, columns=columns, validate=valideta)
    end = time.time()
    print(f'{valideta} tomdb-2 validation: {end - start}')
    return t


if __name__ == "__main__":
    pass
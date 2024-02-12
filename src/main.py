from table import Table
from fileop import FileOperator
import core
import dtypes

if __name__ == "__main__":
    data = [[1, 'a', 2.22], [2, 'b', 3.456], [3, 'c', 56.01]]
    columns = ['col1', 'col2', 'col3']
    dtypes = {x: dtype for x, dtype in
              zip(columns, [dtypes.Smallint(), dtypes.Varchar(10), dtypes.Numeric(precision=4, scale=2)])}
    table = Table(data=data, schema='tsk-default', uid=FileOperator.generate_uid(),
                  name='table-1', dtypes=dtypes, columns=columns)
    core.add_table_to_schema(table)
    table2 = core.read_table('/home/janni/projects/tomdb-2/tomdb-2/bin/tsk/tsk-default/table-1/0001.tdb')
    print(table2.data)

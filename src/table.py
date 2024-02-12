class Table:

    def __init__(self, data, schema, uid, name, dtypes, columns):
        """

        :param data:
        :param schema:
        :param uid:
        :param name:
        """
        self.data = data
        self.schema = schema
        self.uid = uid
        self.name = name
        self.dtypes = dtypes
        self.columns = columns

    def __str__(self):
        return f'Name: {self.name}\nColumns: {self.columns}, dtypes: {self.dtypes}'

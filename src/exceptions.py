class ObjectNotFound(Exception):

    def __init__(self, msg='ObjectNotFound'):
        super().__init__(msg)


class DataTypeException(Exception):

    def __init__(self, msg):
        super().__init__(msg)


class InvalidType(DataTypeException):

    def __init__(self, msg, pos=None):
        if pos:
            msg += f' at position {pos}'
        super().__init__(msg)


class Overflow(DataTypeException):

    def __init__(self, msg, pos=None):
        if pos:
            msg += f' at position {pos}'
        super().__init__(msg)


class TableConsistencyError(Exception):

    def __init__(self, msg):
        super().__init__(msg)

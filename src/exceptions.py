class ObjectNotFound(Exception):

    def __init__(self, msg='ObjectNotFound'):
        super().__init__(msg)


class InvalidType(Exception):

    def __init__(self, msg, pos=None):
        if pos:
            msg += f' at position {pos}'
        super().__init__(msg)


class Overflow(Exception):

    def __init__(self, msg, pos=None):
        if pos:
            msg += f' at position {pos}'
        super().__init__(msg)
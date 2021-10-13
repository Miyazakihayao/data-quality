# encoding=utf-8

class ValidateError(Exception):
    """
    exception for validation
    """
    pass


class PluginNotFoundError(Exception):
    """
    exception for plugin
    """
    pass


class RuleNotFoundError(Exception):
    """
    exception for rule
    """
    pass


class TransferError(Exception):
    """
    exception for Transfer
    """
    pass


class HiveError(Exception):
    """
    exception for Hive
    """
    pass


class DataError(Exception):
    """
    bissness error
    """

    def __init__(self, value):
        self.value = "Bissness Error: " + value

    def __str__(self):
        return self.value


class MysqlError(Exception):
    """
    exception for Mysql
    """
    pass


class DeclareError(Exception):
    """
    exception for Declare
    """
    pass


class SqlalchemyError(Exception):
    """
    exception for Mysql
    """
    pass


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

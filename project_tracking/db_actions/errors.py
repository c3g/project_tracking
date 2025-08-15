"""
Custom exceptions for the db_actions package.
"""

class Error(Exception):
    """Generic error for db_action"""
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        super().__init__()
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        """Convert the error to a dictionary."""
        rv = dict(self.payload or ())
        rv['DB_ACTION_ERROR'] = self.message
        return rv

class DidNotFindError(Error):
    """DidNotFindError"""
    def __init__(self, message=None, table=None, attribute=None, query=None):
        super().__init__(message)
        if message:
            self.message = message
        else:
            self.message = f"'{table}' with '{attribute}' '{query}' doesn't exist in the database"

class RequestError(Error):
    """RequestError"""
    def __init__(self, message=None, argument=None):
        super().__init__(message)
        if message:
            self.message = message
        else:
            self.message = f"For current request, '{argument}' is required"

class UniqueConstraintError(Error):
    """UniqueConstraintError"""
    def __init__(self, message=None, entity=None, attribute=None, value=None):
        super().__init__(message)
        if message:
            self.message = message
        else:
            self.message = f"'{entity}' with '{attribute}' '{value}' already exists in the database and '{attribute}' has to be unique"

class EnumValueError(Error):
    """EnumValueError"""
    def __init__(self, message=None, enum=None, value=None):
        super().__init__(message)
        if message:
            self.message = message
        else:
            self.message = f"'{value}' is not a valid value for '{enum}'. Valid values are: {', '.join(enum.__members__.keys())}"

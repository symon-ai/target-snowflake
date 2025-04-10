"""Exceptions used by pipelinewise-export-snowflake"""


class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""


class UnexpectedValueTypeException(Exception):
    """Exception to raise when record value type doesn't match the expected schema type"""


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records"""


class FileFormatNotFoundException(Exception):
    """Exception to raise when name file format not found"""


class InvalidFileFormatException(Exception):
    """Exception to raise when name file format is not compatible"""


class UnexpectedMessageTypeException(Exception):
    """Exception to raise when provided message doesn't match the expected type"""


class PrimaryKeyNotFoundException(Exception):
    """Exception to raise when primary key not found in the record message"""

class AccessControlException(Exception):
    """Exception to raise when Snowflake privilege is missing"""

# We skip extra error handling for this error in Symon and use it directly. 
# PrimaryKeyNotFoundException, AccessControlExceltion, FileFormatNotFoundException have been replaced with SymonException in the code.
class SymonException(Exception):
    """Exception to raise for Symon Export to skip extra error handling"""
    def __init__(self, message, code, details=None):
        super().__init__(message)
        self.code = code
        self.details = details

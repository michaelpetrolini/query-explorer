from enum import Enum


class ExceptionType(Enum):
    NO_TABLE_FOUND = "No source table has been found for the following dependency"
    WILDCARD_MULTIPLE_SOURCES = "A wildcard with an alias is pointing to multiple tables"
    ALIAS_WITH_NO_SOURCES = "Found no dependencies for the following column"


class LogicError(Exception):
    def __init__(self, exception_type: ExceptionType, obj):
        self.exception_type = exception_type
        self.obj = obj

    def __str__(self):
        return f"{self.exception_type.value}: {self.obj}"

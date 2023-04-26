from enum import Enum


class ExceptionType(Enum):
    NO_TABLE_FOUND = "No source table has been found for the following dependency"
    NO_ALIAS_TABLE_FOUND = "No source table with a corresponding alias has been found for the following dependency"
    MULTIPLE_ALIAS_TABLES_FOUND = "Multiple source tables with a corresponding alias have been found for the " \
                                  "following dependency"
    MULTIPLE_ALIAS_COLUMNS_FOUND = "Multiple columns with a corresponding alias have been found for the following " \
                                   "dependency"
    WILDCARD_MULTIPLE_SOURCES = "A wildcard with an alias is pointing to multiple tables"
    ALIAS_WITH_NO_SOURCES = "Found no dependencies for the following column"
    TWIN_NOT_FOUND = "No related column found in the union logic for the following column name"


class LogicError(Exception):
    def __init__(self, exception_type: ExceptionType, obj):
        self.exception_type = exception_type
        self.obj = obj

    def __str__(self):
        return f"{self.exception_type.value}: {self.obj}"

from typing import NewType


class UInt64(int):
    """
    Unsigned Int 64bit type
    """

class Int64(int):
    """
    Int 64bit type
    """


class UInt32(int):
    """
    Unsigned Int 32bit type
    """


class Int32(int):
    """
    Int 32bit type
    """


class UInt16(int):
    """
    Unsigned Int 64bit type
    """


class Int16(int):
    """
    Int 64bit type
    """


class UInt8(int):
    """
    Unsigned Int 64bit type
    """


class Int8(int):
    """
    Int 64bit type
    """



class String(str):
    """
    String type
    """



class Date(str):
    """
    Date type (as string)
    """



class DateTime(str):
    """
    DateTime type (as string
    """



class Float64(float):
    """
    Float 64bit type
    """



class Float32(float):
    """
    Float 64bit type
    """


TYPES_PRIORITY = {
    'String': 13,
    'Float64': 12,
    'Float32': 11,
    'UInt64': 10,
    'UInt32': 9,
    'UInt16': 8,
    'UInt8': 7,
    'Int64': 6,
    'Int32': 5,
    'Int16': 4,
    'Int8': 3,
    'DateTime': 2,
    'Date': 1
}


__all__ = [
    'String',
    'Float64',
    'Float32',
    'UInt64',
    'UInt32',
    'UInt16',
    'UInt8',
    'Int64',
    'Int32',
    'Int16',
    'Int8',
    'DateTime',
    'Date',
    'TYPES_PRIORITY'
]
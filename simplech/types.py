from typing import NewType



UInt64 = NewType('UInt64', int)
Int64 = NewType('Int64', int)
UInt32 = NewType('UInt32', int)
Int32 = NewType('Int32', int)
UInt16 = NewType('UInt16', int)
Int16 = NewType('Int16', int)
UInt8 = NewType('UInt8', int)
Int8 = NewType('Int8', int)
String = NewType('String', str)
Date = NewType('String', str)
DateTime = NewType('String', str)
Float64 = NewType('Float64', float)
Float32 = NewType('Float32', float)




TYPES_PRIORITY = (
    String,
    Float64,
    Float32,
    UInt64,
    UInt32,
    UInt16,
    UInt8,
    Int64,
    Int32,
    Int16,
    Int8,
    DateTime,
    Date
)

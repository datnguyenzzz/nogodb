# A Sorted String Table (SST) Implementation in Golang

Tables are exclusively opened for reading or created for writing, but never both simultaneously.
During the initialization process for creating a table, clients will have several options to choose 
from based on their specific use cases.

## Table Formats 

### 1. BlockedBasedTable Format 

_Inspired by [LevelDB file format](https://github.com/google/leveldb/blob/main/doc/table_format.md)_

#### Format 

```
<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[meta block 1: filter block]                  
[meta block 2: index block]
[meta block 3: compression dictionary block] 
[meta block 4: range deletion block]         
[meta block 5: stats block]                   
...
[meta block K: future extended block]  
[metaindex block]
[Footer]                               (fixed size; starts at file_size - sizeof(Footer))
<end_of_file>
```

The file contains internal pointers, called `BlockHandles`, containing the following information:
```
offset: int64
size:   int64
```

Footer formats. Note that much of the existing footer parsing code assumes that the version (for non-legacy formats) 
and magic number are at the end.

```
metaindex handle (varint64 offset, varint64 size)
index handle     (varint64 offset, varint64 size)
<padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
checksum: CRC over footer data (4 bytes)
footer version (4 bytes)
table_magic_number (8 bytes)
```
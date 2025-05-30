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
offset: varint64-encoded
size:   varint64-encoded
```

#### Key Aspects (*Not up-to-date yet*)
- A `Reader` eagerly loads the footer, `meta index` block and `meta properties` block,
because the data contained in those blocks is needed on every read, and even before reading. 

- Each block consists of some data and a **5 bytes** trailer: a 1 byte for block type and
4 bytes for the CRC checksum. The checksum is computed over the compressed data and the first byte of the trailer
and is serialized as little-endian. The block type gives the per-block compression used; each block is compressed
independently.

- The sequence of key/value pairs in the file are stored in sorted order and partitioned into a sequence of data blocks. 
These blocks come one after another at the beginning of the file

- A `meta index block` contains one entry for every meta block, where the key is the name of the meta block and the value 
is a `BlockHandle` pointing to that meta block.

- `Index blocks` are used to look up a data block containing the range including a lookup key. It is a binary search data structure
Instead of a single index block, the sstable can have a two-level index (this is used to prevent a single huge index block). 
A two-level index consists of a sequence of lower-level index blocks with block handles for data blocks
followed by a single top-level index block with block handles for the lower-level index blocks. The format will be 
```
[index block - 1st level]
[index block - 1st level]
...
[index block - 1st level]
[index block - 2nd level]
```

-  `Meta Properties Block` contains a bunch of properties. The key is the name of the property. The value is the property.

- Footer formats. Note that much of the existing footer parsing code assumes that the version (for non-legacy formats) 
and magic number are at the end.

```
metaindex handle (varint64 offset, varint64 size)
index handle     (varint64 offset, varint64 size)
<padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
checksum: CRC over footer data (4 bytes)
footer version (4 bytes)
table_magic_number (8 bytes)
```
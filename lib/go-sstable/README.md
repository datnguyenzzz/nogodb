# A Sorted String Table (SST) Implementation in Golang

Tables are exclusively opened for reading or created for writing, but never both simultaneously.
During the initialization process for creating a table, clients will have several options to choose 
from based on their specific use cases.

## Table Formats 

### 1. Row Oriented BlockedBasedTable Format 

_Inspired by [LevelDB file format](https://github.com/google/leveldb/blob/main/doc/table_format.md)_

#### a. Table Format 

```
<beginning_of_file>
[data blockData 1]
[data blockData 2]
...
[data blockData N]
[meta blockData 1: filter blockData]                  
[meta blockData 2: index blockData]     
...
[meta blockData K: future extended blockData]  
[metaindex blockData]
[Footer]                               (fixed size; starts at file_size - sizeof(Footer))
<end_of_file>
```

The file contains internal pointers, called `BlockHandles`, containing the following information:
```
offset: int64
size:   int64
```

A `metaindex` blockData contains one entry for every meta blockData, where the key is the name of the meta blockData 
and the value is a BlockHandle pointing to that meta blockData. 
```
filterKey        : BlockHandle(FilterBlock)
2ndLevelIndexKey : BlockHandle(2ndLevelIndex)
```

Each blockData consists of some data and a 5 byte trailer: a 1 byte blockData type and a
4 byte checksum. The checksum is computed over the compressed data and the first
byte of the trailer (i.e. the blockData type), and is serialized as little-endian.
The blockData type gives the per-blockData compression used; each blockData is compressed
independently

Illustration of a physical blockData trailer:
```
+---------------------------+-------------------+
| compression type (1-byte) | checksum (4-byte) |
+---------------------------+-------------------+

The checksum is a CRC-32 computed using Castagnoli's polynomial. Compression 
type also included in the checksum.
```

Footer formats. Note that much of the existing Footer parsing code assumes that the version (for non-legacy formats) 
and magic number are at the end.

```
metaindex handle (varint64 offset, varint64 size)
index handle     (varint64 offset, varint64 size)
<padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
checksum: CRC over Footer data (4 bytes)
Footer version (4 bytes)
table_magic_number (8 bytes)
```

#### b. Data Block Format 

A Data Block is consist of one or more key/value entries and a blockData trailer. Block entry shares key prefix with its preceding 
key until a restart point reached. A blockData should contains at least one restart point. First restart point are always zero.

For example, if two adjacent keys are `"deck"` and `"dock"`, then the second key would be encoded as 
`{1,"ock"}`. The shared prefix length is varint encoded. The remainder string and the value are encoded as a varint-encoded length 
followed by the literal contents. To continue the example, suppose that the key `"dock"` mapped to the value
`"v2"`. The encoded key/value entry would be: `"\x01\x03\x02dockv2"`.

Every blockData has a restart interval I. Every I'th key/value entry in that blockData is called a restart point, and shares no key prefix with the previous entry.
Continuing the example above, if the key after `"dock"` was `"duck"`, but was part of a restart point, 
then that key would be encoded as `{0, "duck"}` instead of `{1, "uck"}`.

Illustration of a typical data blockData:

```
  + restart point                  + restart point (depends on restart interval)
 /                                /
+---------------+---------------+---------------+---------------+---------+
| blockData entry 1 | blockData entry 2 |      ...      | blockData entry n | trailer |
+---------------+---------------+---------------+---------------+---------+
```

```
          +---- key len ----+
         /                   \
+-------+---------+-----------+---------+--------------------+--------------+----------------+
| shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
+-----------------+---------------------+--------------------+--------------+----------------+

Block entry shares key prefix with its preceding key:
Conditions:
    restart_interval=2
    entry one  : key=deck,value=v1
    entry two  : key=dock,value=v2
    entry three: key=duck,value=v3
The entries will be encoded as follow:

  + restart point (offset=0)                                                 + restart point (offset=16)
 /                                                                          /
+-----+-----+-----+----------+--------+-----+-----+-----+---------+--------+-----+-----+-----+----------+--------+
|  0  |  4  |  2  |  "deck"  |  "v1"  |  1  |  3  |  2  |  "ock"  |  "v2"  |  0  |  4  |  2  |  "duck"  |  "v3"  |
+-----+-----+-----+----------+--------+-----+-----+-----+---------+--------+-----+-----+-----+----------+--------+
 \                                   / \                                  / \                                   /
  +----------- entry one -----------+   +----------- entry two ----------+   +---------- entry three ----------+
```
```
The blockData trailer will contains two restart points:

+------------+-----------+--------+
|     0      |    16     |   2    |
+------------+-----------+---+----+
 \                      /     \
  +-- restart points --+       + restart points length

  +-- 4-bytes --+
 /               \
+-----------------+-----------------+-----------------+------------------------------+
| restart point 1 |       ....      | restart point n | restart points len (4-bytes) |
+-----------------+-----------------+-----------------+------------------------------+
```

#### c. Index blockData 

An index blockData is a blockData with N key/value entries, and they share the similar format
with the data blockData. It helps to faster locate the data blockData that might have a requested key

The index blockData `i'th` value is the encoded blockData handle of the `i'th` data blockData.
And the index blockData `i'th` key is a string `>=` last key in that data blockData 
and `<` the first key in the successive data blockData. The index blockData restart 
interval is `1`: every entry is a restart point.

By default, we use a two-level index. It consists of a sequence of lower-level 
index blocks with blockData handles for data blocks followed by a single top-level 
index blockData with blockData handles for the lower-level index blocks. Value of the 
top-level index blockData is the encoded blockData handle of the lower-level blocks,

```
2 Level Block format when stored in the stable storage:
[index blockData - 1st level]
[index blockData - 1st level]
...
[index blockData - 1st level]
[index blockData - 2nd level]
```
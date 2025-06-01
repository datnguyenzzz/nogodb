# A Sorted String Table (SST) Implementation in Golang

Tables are exclusively opened for reading or created for writing, but never both simultaneously.
During the initialization process for creating a table, clients will have several options to choose 
from based on their specific use cases.

## Table Formats 

### 1. BlockedBasedTable Format 

_Inspired by [LevelDB file format](https://github.com/google/leveldb/blob/main/doc/table_format.md)_

#### a. Table Format 

```
<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[meta block 1: filter block]                  
[meta block 2: index block]     
[meta block 3: stats block]             
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

#### b. Data Block Format 

Block is consist of one or more key/value entries and a block trailer. Block entry shares key prefix with its preceding 
key until a restart point reached. A block should contains at least one restart point. First restart point are always zero.

For example, if two adjacent keys are `"deck"` and `"dock"`, then the second key would be encoded as 
`{1,"ock"}`. The shared prefix length is varint encoded. The remainder string and the value are encoded as a varint-encoded length 
followed by the literal contents. To continue the example, suppose that the key `"dock"` mapped to the value
`"v2"`. The encoded key/value entry would be: `"\x01\x03\x02dockv2"`.

Every block has a restart interval I. Every I'th key/value entry in that block is called a restart point, and shares no key prefix with the previous entry.
Continuing the example above, if the key after `"dock"` was `"duck"`, but was part of a restart point, 
then that key would be encoded as `{0, "duck"}` instead of `{1, "uck"}`.

Illustration:

```
  + restart point              + restart point (depends on restart interval)
 /                                /
+---------------+---------------+---------------+---------------+---------+
| block entry 1 | block entry 2 |      ...      | block entry n | trailer |
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
The block trailer will contains two restart points:

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
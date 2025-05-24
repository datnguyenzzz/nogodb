## What is go-wal 

`go-wal` is a Golang implementation of a [write ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) data structure.


## Features
* Disk based, support large data volume
* Append only write, which means that sequential writes do not require disk seeking, which can dramatically speed up disk I/O
* Support batch write, all data in a batch will be written in a single disk seek
* Support concurrent write and read, all functions are thread safe

## Format and data layout 

_Inspired by the implementation from [RocksDB](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Page-File-Format) and [LevelDB](https://github.com/google/leveldb)_

### Page file format 

A segment file consists of a sequence of variable length records. Records are grouped by `BlockSize`(by default is `32KB`). 
If a certain record cannot fit into the leftover space, then the leftover space is padded with empty (null) data. 
The writer writes and the reader reads in chunks of `BlockSize`.

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       <---  BlockSize ------>|<--  BlockSize ------>|

  rn = variable size records
  P = Padding
```

### Record Format

```
+---------+-----------+-----------+----------------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Log number (8B)| Payload   |
+---------+-----------+-----------+----------------+--- ... ---+

CRC = 32-bit hash computed over the payload using CRC checksum
Size = Length of the payload data
Type = Type of record (ZeroType, FullType, FirstType, LastType, MiddleType )
       The type is used to group a bunch of records together to represent
       blocks that are larger than BlockSize
Log number = 64bit log file number, so that we can distinguish between
             records written by the most recent log writer vs a previous one.
Payload = Byte stream as long as specified by the payload size
```

Records are initially written into a memory buffer. To optimize memory usage and avoid waste during memory allocation, 
an enhanced bytes buffer pool, [go-bytesbufferpool](https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-bytesbufferpool) 
has been utilised. Once the record is stored in the memory buffer, it is subsequently written to the OS buffer. Eventually, the record 
is asynchronously flushed into stable storage, a process managed by the OS kernel.

The Write-Ahead Log (WAL) includes a background job with a configurable task that periodically flushes the data file to disk.
Additionally, the WAL provides a function for clients to manually flush the data file to disk, ensuring higher reliability 
at the cost of reduced throughput.
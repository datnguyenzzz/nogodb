## Go-cache - a block cache built for nogoDB

Cache implements nogoDB's block cache. It supports 2 method for 
page cache replacement: LRU and Clock-pro. The underline hash tables implementation 
is based on the Concurrent Dynamic-Sized Nonblocking Hash Tables

Blocks are keyed by an (fileNum, offset) pair. The fileNum and offset
refer to an sstable file number and the offset of the block within the file.
Because sstables are immutable and file numbers are never reused,
(fileNum,offset) are unique for the lifetime of a nogoDB instance.

## Overview 

### Concurrent Dynamic-Sized Nonblocking Hash Tables

Set(key, value):
   1. hash(key) = `bucket_id`
   2. Append (key,value) into `bucket_id` in sorted order

Get(key):
   1. hash(key) = `bucket_id`
   2. Binary search on `bucket_id` to find key

Async. Grow operation when a bucket get overflowed
 
```
+--------------+------+
|   bucket 1   |      |
+--------------+------+
|    ...       |      |
+--------------+------+
| bucket I % N |      | -- key % 2N = I -
+--------------+------+                  |
|    ...       |      |                  |           
+--------------+------+                  |
|   bucket N   |      |                  |
+--------------+------+                  |
|    ...       |      |                  |
+--------------+------+                  |
|   bucket I   |      | <-- append key --
+--------------+------+
|    ...       |      |
+--------------+------+
|   bucket 2N  |      |
+--------------+------+
```

Async. Shrink operation when a bucket get oversized

```
+--------------+------+
|   bucket 1   |      |
+--------------+------+
|    ...       |      |
+--------------+------+
|   bucket I   |      | <- append key ---
+--------------+------+                  |
|    ...       |      |                  |           
+--------------+------+                  |
|  bucket N/2  |      |                  |
+--------------+------+                  |
|    ...       |      |                  |
+--------------+------+                  |
| bucket I+N/2 |      | --- All keys ----
+--------------+------+
|    ...       |      |
+--------------+------+
|   bucket N   |      |
+--------------+------+
```

### Replacement method
#### LRU 
#### Clock Pro
- CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
  https://en.wikipedia.org/wiki/Adaptive_replacement_cache.
  It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ),
  much like the CLOCK page replacement algorithm is an approximation of LRU.
  The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html


## Current support replacement method
- [ ] LRUCache
- [ ] ClockCache
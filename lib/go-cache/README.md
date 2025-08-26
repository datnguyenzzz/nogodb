## Go-cache - a block cache built for nogoDB

Cache implements nogoDB's block cache. It supports 2 method for 
page cache replacement: LRU and Clock-pro. The underline hash tables implementation 
is based on: "Dynamic-Sized Nonblocking Hash Tables", by Yujie Liu,
Kunlong Zhang, and Michael Spear. 

Blocks are keyed by an (fileNum, offset) pair. The fileNum and offset
refer to an sstable file number and the offset of the block within the file.
Because sstables are immutable and file numbers are never reused,
(fileNum,offset) are unique for the lifetime of a nogoDB instance.

### Current support replacement method
- [ ] LRUCache
- [ ] ClockCache
  - CLOCK-Pro is a patent-free alternative to the Adaptive Replacement Cache,
  https://en.wikipedia.org/wiki/Adaptive_replacement_cache.
  It is an approximation of LIRS ( https://en.wikipedia.org/wiki/LIRS_caching_algorithm ), 
  much like the CLOCK page replacement algorithm is an approximation of LRU.
  The original paper: http://static.usenix.org/event/usenix05/tech/general/full_papers/jiang/jiang_html/html.html

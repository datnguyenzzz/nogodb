## What is go-wal 

`go-wal` is a Golang implementation of a [write ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) data structure

## Key Features
* Disk based, support large data volume
* Append only write, which means that sequential writes do not require disk seeking, which can dramatically speed up disk I/O
* Support batch write, all data in a batch will be written in a single disk seek
* Support concurrent write and read, all functions are thread safe
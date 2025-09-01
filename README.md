### NogoDB: A Key-Value Storage using Fragmented Log-Structured Merge Tree

>_**Disclaimer**: This project is under development and crafted by human ingenuity—not AI. I won’t let G/AI steal all the fun!_

NogoDB is a key-value embedded storage system inspired by LevelDB and RocksDB. It builds upon RocksDB's file formats while 
incorporating several enhancements, such as range deletion tombstones, table-level bloom filters, and updates to the MANIFEST format.

While key-value stores like LevelDB and RocksDB deliver excellent write throughput, they are often hindered by 
high write amplification—a known issue stemming from the Log-Structured Merge Tree (LSM) data structure that underpins 
these systems. 

To address this tradeoff, NogoDB leverages an advanced data structure called 
Fragmented Log-Structured Merge Trees (FLSM). FLSM introduces the concept of "guards" to organize logs more effectively, 
minimizing data rewriting within the same level and significantly reducing write amplification.

## Architecture (Plan Ahead)
```mermaid
graph TB
%% Subgraphs with padding for spacing
    subgraph PublicAPI["Public API Layer"]
        style PublicAPI padding:15px
        NogoDB_API["NogoDB API"]:::api
    end

    subgraph Core["Core Processing Layer"]
        style Core padding:15px
        WritePath["Write Path"]:::core
        ReadPath["Read/Iterator Path"]:::core
        MergeOps["Merge Operations"]:::core
    end

    subgraph Memory["In-Memory Layer"]
        style Memory padding:15px
        BlockCache["Block Cache"]:::memory
        MemTable["MemTable"]:::memory
        BloomFilter["Block Bloom Filter"]:::memory
        BlockIndex["Block Index"]:::memory
    end

    subgraph Storage["On-Disk Layer"]
        style Storage padding:15px
        SSTFiles["SST Files"]:::storage
        WALog["Write-Ahead Log (WAL)"]:::storage
        ManFile["MANIFEST"]:::storage
    end

    subgraph BackgroundOps["Background Operations"]
        style BackgroundOps padding:15px
        Compaction["Compaction Job"]:::bg
        FlushOps["Flush Operations"]:::bg
    end

    subgraph FileSystem["File System Layer"]
        style FileSystem padding:15px
        FSAbstract["File System Abstraction"]:::fs
    end

%% Main flow connections (consistent arrow direction left to right)
    NogoDB_API --> WritePath
    NogoDB_API --> ReadPath
    NogoDB_API --> MergeOps

    WritePath --> MemTable
    WritePath --> WALog

    MergeOps --> MemTable
    MergeOps --> SSTFiles

    ReadPath --> BlockCache
    ReadPath --> MemTable

    BlockCache <-.-> BloomFilter
    BlockCache <-.-> BlockIndex
    BlockCache <-.-> SSTFiles
    BlockIndex <-.-> SSTFiles
    BloomFilter <-.-> SSTFiles

    SSTFiles --> Compaction
    MemTable --> FlushOps
    FlushOps --> SSTFiles

    SSTFiles -->|Buffered Write| FSAbstract
    WALog -->|Buffered Write| FSAbstract
    SSTFiles -.-> ManFile

%% Click events only on existing nodes
    click NogoDB_API "xyz"
    click WritePath "xyz"
    click ReadPath "xyz"
    click MemTable "xyz"
    click BlockCache "xyz"
    click BloomFilter "xyz"
    click BlockIndex "xyz"
    click SSTFiles "xyz"
    click WALog "xyz"
    click Compaction "xyz"
    click FlushOps "xyz"
    click FSAbstract "xyz"

%% Styles for groups
    classDef api fill:#FFE5CC,stroke:#FF9933,stroke-width:2px;
    classDef core fill:#CCE5FF,stroke:#3399FF,stroke-width:2px;
    classDef memory fill:#E5CCFF,stroke:#9933FF,stroke-width:2px;
    classDef storage fill:#CCFFCC,stroke:#33FF33,stroke-width:2px;
    classDef bg fill:#E5E5E5,stroke:#666666,stroke-width:2px;
    classDef fs fill:#FFE5E5,stroke:#FF3333,stroke-width:2px;
    
    subgraph legend1 [Legend]
        L1["Public API Layer"]:::api
        L2["Core Processing Layer"]:::core
        L3["In-Memory Layer"]:::memory
    end
    subgraph legend2 [Legend]
        L4["On-Disk Layer"]:::storage
        L5["Background Operations"]:::bg
        L6["File System Layer"]:::fs
    end
```

## Internal component
- [`Status: Done`] [Adaptive radix tree - Serve as an in-memory storage](lib/go-adaptive-radix-tree/README.md)
- [`Status: Done`] [Blocked Bloom Filter with bit pattern](lib/go-blocked-bloom-filter/README.md)
- [`Status: Done`] [Write Ahead Log](lib/go-wal/README.md)
- [`Status: Done`] [Cache - A hash map for caching block](lib/go-block-cache/README.md)
- [`Status: In Progress`] [Sort String Table](lib/go-sstable/README.md)
- [`Status: Not Yet Started`] [Virtual File System](lib/go-fs)

## Test Coverage

| Package | Coverage |
|---------|----------|
| go-adaptive-radix-tree | 53.8% |
| go-block-cache | 84.0% |
| go-blocked-bloom-filter | 90.7% |
| go-bytesbufferpool | 90.5% |
| go-context-aware-lock | 0.0% |
| go-fs | 0.0% |
| go-sstable | 28.3% |
| go-wal | 16.4% |

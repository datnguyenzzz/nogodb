### NogoDB: A Key-Value Storage using Fragmented Log-Structured Merge Tree

<img width="400" height="400" alt="nogodb_logo" src="https://github.com/user-attachments/assets/b28f2968-8e9e-4e4a-a98b-f1fc4ad6dfa0" />

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
        BloomFilter["Bloom Filter"]:::memory
        BlockIndex["Indexes"]:::memory
        BlockData["Data"]:::memory
    end

    subgraph Storage["On-Disk Layer"]
        style Storage padding:15px
        SSTFiles["SST Files"]:::storage
        WALog["Write-Ahead Log (WAL)"]:::storage
        ManFile["MANIFEST"]:::storage
    end

    subgraph BackgroundOps["Background Operations"]
        style BackgroundOps padding:15px
        Compaction["Compaction"]:::bg
        FlushOps["Flush"]:::bg
        Pacer["Pacing Controller"]:::bg
    end

    subgraph FileSystem["File System Layer"]
        style FileSystem padding:15px
        FSAbstract["File System Abstraction"]:::fs
        RemoteStore["Remote Object Store"]:::fs
        LocalFile["Local File Store"]:::fs
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
    BlockCache <-.-> BlockData
    BlockData <-.-> SSTFiles
    BlockIndex <-.-> SSTFiles
    BloomFilter <-.-> SSTFiles

    MemTable --> FlushOps
    FlushOps --> Pacer
    Compaction --> Pacer
    Pacer --> SSTFiles

    SSTFiles -->|Buffered Write| FSAbstract
    WALog -->|Buffered Write| FSAbstract
    FSAbstract <-.-> RemoteStore
    FSAbstract <-.-> LocalFile
    
    SSTFiles -.-> ManFile

%% Click events only on existing nodes
    click NogoDB_API "xyz"
    click WritePath "xyz"
    click ReadPath "xyz"
    click MemTable "https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-adaptive-radix-tree"
    click BlockCache "https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-block-cache"
    click BloomFilter "https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-blocked-bloom-filter"
    click BlockIndex "xyz"
    click BlockData "xyz"
    click SSTFiles "https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-sstable"
    click WALog "https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-wal"
    click Compaction "xyz"
    click FlushOps "xyz"
    click Pacer "https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-adaptive-rate-limiter"
    click FSAbstract "https://github.com/datnguyenzzz/nogodb/tree/master/lib/go-fs"
    click RemoteStore "xyz"
    click LocalFile "xyz"

%% Styles for groups
    classDef api fill:#FFE5CC,stroke:#FF9933,stroke-width:2px;
    classDef core fill:#CCE5FF,stroke:#3399FF,stroke-width:2px;
    classDef memory fill:#E5CCFF,stroke:#9933FF,stroke-width:2px;
    classDef storage fill:#CCFFCC,stroke:#33FF33,stroke-width:2px;
    classDef bg fill:#E5E5E5,stroke:#666666,stroke-width:2px;
    classDef fs fill:#FFE5E5,stroke:#FF3333,stroke-width:2px;
```






## Test Coverage

| Package | Coverage |
|---------|----------|
| go-adaptive-radix-tree | ![Coverage](https://img.shields.io/badge/coverage-54-yellow) |
| go-adaptive-rate-limiter | ![Coverage](https://img.shields.io/badge/coverage-53-yellow) |
| go-block-cache | ![Coverage](https://img.shields.io/badge/coverage-82-green) |
| go-blocked-bloom-filter | ![Coverage](https://img.shields.io/badge/coverage-90-green) |
| go-bytesbufferpool | ![Coverage](https://img.shields.io/badge/coverage-90-green) |
| go-context-aware-lock | ![Coverage](https://img.shields.io/badge/coverage-0-red) |
| go-fs | ![Coverage](https://img.shields.io/badge/coverage-41-red) |
| go-sstable | ![Coverage](https://img.shields.io/badge/coverage-35-red) |
| go-wal | ![Coverage](https://img.shields.io/badge/coverage-16-red) |

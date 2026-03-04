## Columnar block format 

Referrence: https://www.pdl.cmu.edu/PDL-FTP/Database/pax.pdf

### Block layout
Every columnar block begins with a header describing the structure and schema of the block. Then columns values are encoded in sequence. The block ends with a single padding byte.

The block header begins with:
- Version number (1 byte)
- The number of columns in the block (2 bytes)
- The number of rows in the block (4 bytes)

Then follows a column-header for each column. Each column header encodes the data type (1 byte) and the offset into the block where the column data begins (4 bytes). Depends on each data type we have different way to endcode the column data. 

```
+-----------+----------------+---------------------+
| Vers (1B) | # columns (2B) | # of rows (4B).     |
+-----------+----------------+---------------------+
| DataType (1B) | Page offset points to col 0 (4B) |              
+-----------+--------------------------------------+
| DataType (1B) | Page offset points to col 1 (4B) |            
+-----------+--------------------------------------+
| ...	    | ...                                  |
+-----------+--------------------------------------+
| Type (1B) | Page offset points to col N-1 (4B)   |            
+-----------+--------------------------------------+
|  column 0 data                                ...
+--------------------------------------------------+
|  column 1 data                                ...
+--------------------------------------------------+
| ...
+--------------------------------------------------+
|  column n-1 data                              ...
+-------------+------------------------------------+
| Unused (1B) |
+-------------+
```

Columns data order 

| No | Type                            | Data type               |
| -- | ------------------------------- | ----------------------- |
| 0  | InternalKey.UserKey             | Prefix-compressed bytes |
| 1  | InternalKey.UserKey suffix MVCC | Bytes                   |
| 2  | InternalKey.Trailer             | Uint                    |
| 3  | Value                           | Bytes                   |
| .. | ...                             | ...                     |

For example we have 3 entries:

```json
[
    {"key": "apple.123.1", "value": "green"},
    {"key": "banana.456.2", "value": "yellow"},
    {"key": "rashberry.789.3", "value": "red"}
]
```

Then the data layout after encoding will be following (for illustration purpose only)

```go
1   v1 | 4 | 3
2   0 | 6
3   1 | 7
4   2 | 8
5   3 | 9
6   apple|banana|rashberry 
7   123|456|789 
8   1|2|3 
9   green|yellow|yellow     
```

### Encoding detail methods 
1. Prefix-compressed bytes data type 

__to be updated__

2. Bytes 

__to be updated__

3. Uint

__to be updated__
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

The trailing padding byte exists to allow columns to represent the end of
column data using a pointer to the byte after the end of the column. The
padding byte ensures that the pointer will always fall within the allocated
memory. Without the padding byte, such a pointer would be invalid according
to Go's pointer rules.

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

Layout
```
+-----------------------------------------------+-------+-------+-----
| Uint Encoded of [len(buf_1), len(buf_2), ...] | buf_1 | buf_2 | ...
+-----------------------------------------------+-------+-------+-----
```

3. Uint column encoder (any unsigned integer with 8, 16, 32, 64 bit)

- The low bits indicate how many bytes per integer are used, with allowed values 0, 1, 2, 4, or 8.

- A base (8-byte) value is encoded separately and each encoded value is a delta from that base.

Layout:
```
+----------+----------------+--------------+--------------+--------------+
| Type(1B) | Min Value (8B) | Delta 1 (xB) | Delta 2 (xB) | .....
+----------+----------------+--------------+--------------+--------------+
```
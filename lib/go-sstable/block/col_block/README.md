## Columnar block format 

Referrence: https://www.pdl.cmu.edu/PDL-FTP/Database/pax.pdf

### Data layout
Every columnar block begins with a header describing the structure and schema of the block. Then columns values are encoded in sequence. The block ends with a single padding byte.

The block header begins with:
- Version number (1 byte)
- The number of columns in the block (2 bytes)
- The number of rows in the block (4 bytes)

Then follows a column-header for each column. Each column header encodes the data type (1 byte) and the offset into the block where the column data begins (4 bytes).

```
+-----------+
| Vers (1B) |
+-------------------+--------------------------------+
| # columns (2B)    | # of rows (4B)                 |
+-----------+-------+---------------------+----------+
| Type (1B) | Page offset (4B)                | Col 0
+-----------+---------------------------------+
| Type (1B) | Page offset (4B)                | Col 1
+-----------+---------------------------------+
| ...	    | ...                             |
+-----------+---------------------------------+
| Type (1B) | Page offset (4B)                | Col n-1
+-----------+----------------------------------
|  column 0 data                                ...
+----------------------------------------------
|  column 1 data                                ...
+----------------------------------------------
| ...
+----------------------------------------------
|  column n-1 data                              ...
+-------------+--------------------------------
| Unused (1B) |
+-------------+
```

The trailing padding byte exists to allow columns to represent the end of column data, using a pointer to the byte after the end of the column. The padding byte ensures that the pointer will always fall within the allocated memory. 
## A Golang implementation of Blocked Bloom Filters with Bit Patterns

### References:
- https://save-buffer.github.io/bloom_filter.html

### Recap of the standard Bloom filter:
- A Bloom filter is a space-efficient probabilistic data structure for membership queries.
- It uses a large bit array (`m` bits) and `k` independent hash functions.
- To add an element: compute k hashes, set bits at those positions to 1.
- To query: check if all those bits are 1 (probing); if yes, element might be present (false positives possible), else definitely not present.
- Cons: 
  - multiple (k) hashes computations per element
  - scattered bit accesses cause cache misses.

### Motivations
- Standard Bloom filters suffer from cache inefficiency because bits are scattered across a large bit array.
- Multiple hash functions per key increase CPU cost.
- To improve speed and cache locality, Blocked Bloom Filters divide the bit array into small blocks (e.g., cache-line-sized blocks).
- Each key maps to 1 block, reducing cache misses.
- Instead of computing `k` hash functions, a single hash selects a block, and a second hash selects a precomputed bit pattern 
representing which bits to set inside that block.

### What are Bit Patterns?
- A bit pattern is a precomputed bit vector of length `m (block size)`, with exactly `k bits (probes)` set to 1.
- These patterns represent the positions where bits would be set for an element inside a block.
- When adding an element:
  - Use the first hash to select the block.
  - Use the second hash to select a bit pattern from the table.
  - Set the bits of the block by OR-ing the selected pattern into it.
- When querying:
  - Use the same two hashes to select block and pattern.
  - Check if the block contains all bits of the pattern (bitwise AND).
  - If yes, element might be present; if no, definitely absent.
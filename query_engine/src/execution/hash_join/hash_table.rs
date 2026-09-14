use anyhow::Result;

use crate::{
    arrow::{
        Array, DataType, RecordBatch,
        array::{PrimitiveArray, StringArray},
    },
    execution::hash_join::{hash_combine, hash64},
};

/// Bits 0-48: 48-bit Adjacency Array offset pointer
/// Bits 49-64: 16-bit Register-Blocked Bloom Filter
#[derive(Clone, Copy)]
pub struct DirectorySlot(pub u64);

impl DirectorySlot {
    pub fn new(offset: usize, bloom_filter_mask: u16) -> Self {
        assert!(
            offset < (1 << 48),
            "Payload offset exceeded 48-bit address bounds!"
        );
        Self(((offset as u64) << 16) | (bloom_filter_mask as u64))
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn offset(&self) -> usize {
        (self.0 >> 16) as usize
    }

    pub fn bloom_filter(&self) -> u16 {
        self.0 as u16
    }

    pub fn union_bloom_filter(&mut self, tag: u16) {
        self.0 |= tag as u64;
    }
}

// Uses all C(16, 4) four-bit tags, padded to 2^11 entries with a
// deterministic uniform sample. The power-of-two table makes tag selection a
// shift plus a single 4 KiB lookup.
const DISTINCT_BLOOM_TAGS: u32 = 1820;
const BLOOM_TAGS: [u16; 2048] = make_bloom_tags();

const fn make_bloom_tags() -> [u16; 2048] {
    let mut res: [u16; 2048] = [0; 2048];
    let mut id = 0;
    let mut a = 0;
    while a < 16 {
        let mut b = a + 1;
        while b < 16 {
            let mut c = b + 1;
            while c < 16 {
                let mut d = c + 1;
                while d < 16 {
                    res[id] = ((1 << a) | (1 << b) | (1 << c) | (1 << d)) as u16;
                    id += 1;
                    d += 1;
                }
                c += 1;
            }
            b += 1;
        }
        a += 1;
    }

    let mut state: u32 = 0x9E3779B9;
    while id < res.len() {
        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        res[id] = res[((state >> 8) % DISTINCT_BLOOM_TAGS) as usize];
        id += 1;
    }

    res
}

fn bloom_tag(hash: u64) -> u16 {
    let id = (hash & 0x7FF) as usize;
    BLOOM_TAGS[id]
}

fn could_contain(entry: u16, hash: u64) -> bool {
    let tag = bloom_tag(hash);
    // !( tag & ~entry )
    (tag & !entry) == 0
}

pub struct HashTable {
    /// Decoupled Directory Array (size is always a power of 2)
    pub directory: Vec<DirectorySlot>,
    pub directory_mask: u64,
    pub hashes: Vec<u64>,
    pub pointers: Vec<(usize, usize)>,
    /// Original Build RecordBatches pinned in-memory
    pub build_batches: Vec<RecordBatch>,
}

/// A temporary tuple representing a build-side row during the building phase
#[derive(Clone, Copy)]
struct BuildTuple {
    hash: u64,
    batch_idx: usize,
    row_idx: usize,
}

impl HashTable {
    fn hash_string_to_i64(s: &str) -> i64 {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in s.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3u64);
        }
        hash as i64
    }

    pub fn extract_key_to_i64(array: &dyn Array, row_idx: usize) -> Option<i64> {
        if array.is_null(row_idx) {
            return None; // Standard SQL Join Semantics: Skip NULLs!
        }

        match array.data_type() {
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();
                Some(arr.value(row_idx) as i64)
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i16>>()
                    .unwrap();
                Some(arr.value(row_idx) as i64)
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();
                Some(arr.value(row_idx) as i64)
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                Some(arr.value(row_idx))
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f64>>()
                    .unwrap();
                Some(arr.value(row_idx).to_bits() as i64)
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                let val = arr.value(row_idx);
                // Hash the string slice into a high-entropy i64
                Some(Self::hash_string_to_i64(val))
            }
            _ => None,
        }
    }

    fn hash_row_keys(batch: &RecordBatch, col_indexes: &[usize], row_idx: usize) -> Option<u64> {
        let mut combined_hash = 0u64;
        for &col_idx in col_indexes {
            let col = batch.column(col_idx);
            let cell_key = Self::extract_key_to_i64(col.as_ref(), row_idx)?;
            combined_hash = hash_combine(combined_hash, hash64(cell_key as u64));
        }

        Some(combined_hash)
    }

    /// Compares all join keys between a probe row and a build row.
    /// Returns true only if every corresponding column matches perfectly!
    pub fn keys_match(
        &self,
        build_batch_idx: usize,
        build_row_idx: usize,
        probe_batch: &RecordBatch,
        probe_col_indexes: &[usize],
        probe_row_idx: usize,
        build_col_indexes: &[usize],
    ) -> bool {
        let build_batch = &self.build_batches[build_batch_idx];

        for i in 0..probe_col_indexes.len() {
            let p_col = probe_batch.column(probe_col_indexes[i]);
            let b_col = build_batch.column(build_col_indexes[i]);

            let p_val = Self::extract_key_to_i64(p_col.as_ref(), probe_row_idx);
            let b_val = Self::extract_key_to_i64(b_col.as_ref(), build_row_idx);

            match (p_val, b_val) {
                (Some(p), Some(b)) if p == b => {}
                _ => return false,
            }
        }

        true
    }

    /// Compiles the complete unchained adjacency-list hash table
    /// By sorting the tuples by their hashes, colliding elements are placed
    /// physically next to each other, allowing collision resolution via
    /// the contiguous memory scanning
    pub fn try_build(batches: Vec<RecordBatch>, col_indexes: &[usize]) -> Result<Self> {
        let mut tuples = vec![];
        for (idx, batch) in batches.iter().enumerate() {
            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                if let Some(hash) = Self::hash_row_keys(batch, col_indexes, row_idx) {
                    tuples.push(BuildTuple {
                        hash,
                        batch_idx: idx,
                        row_idx,
                    });
                }
            }
        }

        let num_tuples = tuples.len();
        if num_tuples == 0 {
            return Ok(Self {
                directory: Vec::new(),
                directory_mask: 0,
                hashes: Vec::new(),
                pointers: Vec::new(),
                build_batches: batches,
            });
        }

        // guarantees all colliding keys are physically placed adjacent to each other
        tuples.sort_by_key(|t| t.hash);

        // build directory
        let dir_size = num_tuples.next_power_of_two();
        let dir_mask = (dir_size - 1) as u64;
        let mut directory = vec![DirectorySlot(0); dir_size];
        let mut hashes = Vec::with_capacity(num_tuples);
        let mut pointers = Vec::with_capacity(num_tuples);
        for (offset, tuple) in tuples.into_iter().enumerate() {
            hashes.push(tuple.hash);
            pointers.push((tuple.batch_idx, tuple.row_idx));

            let bucket_idx = (tuple.hash & dir_mask) as usize;
            let bloom_tag = bloom_tag(tuple.hash);
            if directory[bucket_idx].is_empty() {
                directory[bucket_idx] = DirectorySlot::new(offset, bloom_tag);
            } else {
                directory[bucket_idx].union_bloom_filter(bloom_tag);
            }
        }

        Ok(Self {
            directory,
            directory_mask: dir_mask,
            hashes,
            pointers,
            build_batches: batches,
        })
    }

    pub fn try_probe(
        &self,
        probe_batch: &RecordBatch,
        probe_col_indexes: &[usize],
        probe_row_idx: usize,
        build_col_indexes: &[usize],
        mut on_match: impl FnMut(usize, usize),
    ) {
        if self.directory.is_empty() {
            return;
        }

        let mut probe_hash = 0u64;
        for &col_idx in probe_col_indexes {
            let col = probe_batch.column(col_idx);
            let cell_key = match Self::extract_key_to_i64(col.as_ref(), probe_row_idx) {
                Some(k) => k,
                None => return,
            };
            probe_hash = hash_combine(probe_hash, hash64(cell_key as u64));
        }

        let bucket_idx = (probe_hash & self.directory_mask) as usize;
        let slot = self.directory[bucket_idx];
        if slot.is_empty() {
            return;
        }

        if !could_contain(slot.bloom_filter(), probe_hash) {
            return;
        }

        // Sequential Scan: trace adjacent colliding keys
        let mut offset = slot.offset();
        let limit = self.hashes.len();
        while offset < limit {
            let build_hash = self.hashes[offset];
            if build_hash & self.directory_mask != bucket_idx as u64 {
                // we hit at the end of the bucket
                break;
            }

            if build_hash == probe_hash {
                let (b_batch_idx, b_row_idx) = self.pointers[offset];
                if self.keys_match(
                    b_batch_idx,
                    b_row_idx,
                    probe_batch,
                    probe_col_indexes,
                    probe_row_idx,
                    build_col_indexes,
                ) {
                    on_match(b_batch_idx, b_row_idx);
                }
            }
            offset += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{ArrayRef, Field, Schema, array::PrimitiveArray, array::StringArray};
    use std::sync::Arc;

    #[test]
    fn test_hash_table_build_and_probe_single_column() {
        let schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]));

        let id_col: ArrayRef = Arc::new(PrimitiveArray::from(vec![10i32, 20, 20, 40]));
        let name_col: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Bobby"),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![id_col, name_col]).unwrap();

        // 1. Build our perfect-fit TUM-style HashTable on column index 0 (id)!
        let table = HashTable::try_build(vec![batch.clone()], &[0]).unwrap();

        assert!(!table.directory.is_empty());
        assert_eq!(table.hashes.len(), 4);

        // 2. Probe Row index 1 (which holds ID 20). Should yield TWO matching records: "Bob" and "Bobby"!
        let mut matches = Vec::new();
        table.try_probe(&batch, &[0], 1, &[0], |batch_idx, row_idx| {
            matches.push((batch_idx, row_idx));
        });

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0], (0, 1)); // Bob
        assert_eq!(matches[1], (0, 2)); // Bobby

        // 3. Probe Non-Existent Key (Row index 3 with ID 40 is probed but let's build a mock batch of length 1 for 99!)
        let mock_schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));
        let mock_id: ArrayRef = Arc::new(PrimitiveArray::from(vec![99i32]));
        let mock_batch = RecordBatch::try_new(mock_schema, vec![mock_id]).unwrap();

        let mut misses = Vec::new();
        table.try_probe(&mock_batch, &[0], 0, &[0], |batch_idx, row_idx| {
            misses.push((batch_idx, row_idx));
        });

        assert!(misses.is_empty());
    }

    #[test]
    fn test_hash_table_multi_column_build_and_probe() {
        let schema = Arc::new(Schema::new(vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "country".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ]));

        let id_col: ArrayRef = Arc::new(PrimitiveArray::from(vec![10i32, 20, 20, 10]));
        let country_col: ArrayRef = Arc::new(StringArray::from(vec![
            Some("US"),
            Some("US"),
            Some("DE"),
            Some("US"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![id_col, country_col]).unwrap();

        // Build HashTable on BOTH columns: id (index 0) and country (index 1)
        let table = HashTable::try_build(vec![batch.clone()], &[0, 1]).unwrap();

        assert!(!table.directory.is_empty());

        // Probe 1: (10, "US") at Row index 0 -> Should match row 0 and row 3!
        let mut matches1 = Vec::new();
        table.try_probe(&batch, &[0, 1], 0, &[0, 1], |b_idx, r_idx| {
            matches1.push((b_idx, r_idx));
        });
        assert_eq!(matches1.len(), 2);
        assert_eq!(matches1[0], (0, 0));
        assert_eq!(matches1[1], (0, 3));

        // Probe 2: (20, "DE") at Row index 2 -> Should match row 2! (Does NOT match row 1 because country is different!)
        let mut matches2 = Vec::new();
        table.try_probe(&batch, &[0, 1], 2, &[0, 1], |b_idx, r_idx| {
            matches2.push((b_idx, r_idx));
        });
        assert_eq!(matches2.len(), 1);
        assert_eq!(matches2[0], (0, 2));
    }
}

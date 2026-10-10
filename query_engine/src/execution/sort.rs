use std::{
    cmp::{self, Ordering},
    collections::BinaryHeap,
    mem,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};

use crate::{
    arrow::{
        Array, ArrayRef, DataType, RecordBatch,
        array::{PrimitiveArray, StringArray},
        ord::SortOptions,
    },
    execution::{
        GermanString,
        hash_join::hash_table::HashTable,
        pipeline::{CombineResult, PhysicalSink, SinkContext, SinkResult},
    },
};

#[derive(Clone, Copy)]
pub struct SortColumn {
    pub col_idx: usize,
    pub opt: SortOptions,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SortValue {
    Int(i64),
    Float(u64),
    Str(GermanString),
}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SortValue::Int(x), SortValue::Int(y)) => x.cmp(y),
            (SortValue::Float(x), SortValue::Float(y)) => x.cmp(y),
            (SortValue::Str(x), SortValue::Str(y)) => x.cmp(y),
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

/// A cache-hot heap item (index only) representing an active row candidate
#[derive(Clone, PartialEq, Eq)]
struct Item {
    pub batch_idx: usize,
    pub row_idx: usize,
    pub sort_values: Vec<SortValue>,
    pub descending: Vec<bool>,
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> Ordering {
        for (i, (val_a, val_b)) in self
            .sort_values
            .iter()
            .zip(other.sort_values.iter())
            .enumerate()
        {
            let mut ord = val_a.cmp(val_b);
            if self.descending[i] {
                ord = ord.reverse();
            }

            if ord != Ordering::Equal {
                return ord;
            }
        }

        Ordering::Equal
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct LocalState {
    batches: Vec<RecordBatch>,
    heap: BinaryHeap<Item>,
}

pub struct PhysicalSortSink {
    sort_columns: Vec<SortColumn>,
    skip: usize,
    fetch: Option<usize>,
    total_cores: usize,
    partitions: Vec<Mutex<LocalState>>,
}

impl PhysicalSortSink {
    pub fn new(
        sort_columns: Vec<SortColumn>,
        skip: usize,
        fetch: Option<usize>,
        total_cores: usize,
    ) -> Self {
        let mut partitions = Vec::with_capacity(total_cores);
        for _ in 0..total_cores {
            partitions.push(Mutex::new(LocalState {
                batches: Vec::new(),
                heap: BinaryHeap::new(),
            }));
        }

        Self {
            sort_columns,
            skip,
            fetch,
            total_cores,
            partitions,
        }
    }

    fn merge_sorted_runs(
        &self,
        batches_1: Vec<RecordBatch>,
        items_1: Vec<Item>,
        batches_2: Vec<RecordBatch>,
        items_2: Vec<Item>,
    ) -> Result<(Vec<RecordBatch>, Vec<Item>)> {
        let len_1 = items_1.len();
        let len_2 = items_2.len();
        let cap = match self.fetch {
            Some(f) => cmp::min(self.skip + f, len_1 + len_2),
            None => len_1 + len_2,
        };

        let start_b = batches_1.len();
        let mut merged_batches = batches_1;
        merged_batches.extend(batches_2);

        let mut merged_items = Vec::with_capacity(cap);
        let mut p1 = 0;
        let mut p2 = 0;
        while p1 < len_1 && p2 < len_2 {
            // To think: Can we not clone here ?
            if items_1[p1] <= items_2[p2] {
                merged_items.push(items_1[p1].clone());
                p1 += 1;
            } else {
                let mut item_2 = items_2[p2].clone();
                item_2.batch_idx += start_b;
                merged_items.push(item_2);
                p2 += 1;
            }
            if merged_items.len() >= cap {
                break;
            }
        }

        while p1 < len_1 && merged_items.len() < cap {
            merged_items.push(items_1[p1].clone());
            p1 += 1;
        }

        while p2 < len_2 && merged_items.len() < cap {
            let mut item_2 = items_2[p2].clone();
            item_2.batch_idx += start_b;
            merged_items.push(item_2);
            p2 += 1;
        }

        Ok((merged_batches, merged_items))
    }

    fn merge(
        &self,
        runs: &mut [Option<(Vec<RecordBatch>, Vec<Item>)>],
    ) -> Result<(Vec<RecordBatch>, Vec<Item>)> {
        let len = runs.len();
        match len {
            0 => Err(anyhow!("Cannot merge 0 runs")),
            1 => Ok(runs[0].take().unwrap()),
            2 => {
                let (batches_1, items_1) = runs[0].take().unwrap();
                let (batches_2, items_2) = runs[1].take().unwrap();
                self.merge_sorted_runs(batches_1, items_1, batches_2, items_2)
            }
            _ => {
                let mid = len >> 1;
                let (left, right) = runs.split_at_mut(mid);
                let (l_batches, l_items) = self.merge(left)?;
                let (r_batches, r_items) = self.merge(right)?;
                self.merge_sorted_runs(l_batches, l_items, r_batches, r_items)
            }
        }
    }
}

impl PhysicalSink for PhysicalSortSink {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn combine(&self) -> Result<CombineResult> {
        let mut sorted_runs: Vec<Option<(Vec<RecordBatch>, Vec<Item>)>> =
            Vec::with_capacity(self.total_cores);

        for i in 0..self.total_cores {
            let mut guard = self.partitions[i].lock().unwrap();
            let state = mem::replace(
                &mut *guard,
                LocalState {
                    batches: Vec::new(),
                    heap: BinaryHeap::new(),
                },
            );
            if !state.heap.is_empty() {
                sorted_runs.push(Some((state.batches, state.heap.into_sorted_vec())))
            }
        }

        if sorted_runs.is_empty() {
            return Ok(CombineResult::Empty);
        }

        let (batches, items) = self.merge(&mut sorted_runs)?;
        let row_count = items.len();
        if row_count <= self.skip {
            return Ok(CombineResult::Empty);
        }

        let start = self.skip;
        let take = match self.fetch {
            Some(f) => cmp::min(f, row_count - start),
            None => row_count - start,
        };

        let taken_items = &items[start..start + take];
        let schema = batches[0].schema().clone();
        let col_count = schema.fields().len();
        let mut final_columns = Vec::with_capacity(col_count);
        for c_idx in 0..col_count {
            match schema.fields()[c_idx].data_type {
                DataType::Int8 => {
                    let mut b = Vec::with_capacity(taken_items.len());
                    for item in taken_items {
                        let arr = batches[item.batch_idx]
                            .column(c_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i8>>()
                            .unwrap();
                        b.push(if arr.is_null(item.row_idx) {
                            None
                        } else {
                            Some(arr.value(item.row_idx))
                        });
                    }
                    final_columns.push(Arc::new(PrimitiveArray::from(b)) as ArrayRef);
                }
                DataType::Int16 => {
                    let mut b = Vec::with_capacity(taken_items.len());
                    for item in taken_items {
                        let arr = batches[item.batch_idx]
                            .column(c_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i16>>()
                            .unwrap();
                        b.push(if arr.is_null(item.row_idx) {
                            None
                        } else {
                            Some(arr.value(item.row_idx))
                        });
                    }
                    final_columns.push(Arc::new(PrimitiveArray::from(b)) as ArrayRef);
                }
                DataType::Int32 => {
                    let mut b = Vec::with_capacity(taken_items.len());
                    for item in taken_items {
                        let arr = batches[item.batch_idx]
                            .column(c_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i32>>()
                            .unwrap();
                        b.push(if arr.is_null(item.row_idx) {
                            None
                        } else {
                            Some(arr.value(item.row_idx))
                        });
                    }
                    final_columns.push(Arc::new(PrimitiveArray::from(b)) as ArrayRef);
                }
                DataType::Int64 => {
                    let mut b = Vec::with_capacity(taken_items.len());
                    for item in taken_items {
                        let arr = batches[item.batch_idx]
                            .column(c_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i64>>()
                            .unwrap();
                        b.push(if arr.is_null(item.row_idx) {
                            None
                        } else {
                            Some(arr.value(item.row_idx))
                        });
                    }
                    final_columns.push(Arc::new(PrimitiveArray::from(b)) as ArrayRef);
                }
                DataType::Float32 => {
                    let mut b = Vec::with_capacity(taken_items.len());
                    for item in taken_items {
                        let arr = batches[item.batch_idx]
                            .column(c_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<f32>>()
                            .unwrap();
                        b.push(if arr.is_null(item.row_idx) {
                            None
                        } else {
                            Some(arr.value(item.row_idx))
                        });
                    }
                    final_columns.push(Arc::new(PrimitiveArray::from(b)) as ArrayRef);
                }
                DataType::Float64 => {
                    let mut b = Vec::with_capacity(taken_items.len());
                    for item in taken_items {
                        let arr = batches[item.batch_idx]
                            .column(c_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<f64>>()
                            .unwrap();
                        b.push(if arr.is_null(item.row_idx) {
                            None
                        } else {
                            Some(arr.value(item.row_idx))
                        });
                    }
                    final_columns.push(Arc::new(PrimitiveArray::from(b)) as ArrayRef);
                }
                DataType::Utf8 => {
                    let mut b = Vec::with_capacity(taken_items.len());
                    for item in taken_items {
                        let arr = batches[item.batch_idx]
                            .column(c_idx)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        b.push(if arr.is_null(item.row_idx) {
                            None
                        } else {
                            Some(arr.value(item.row_idx))
                        });
                    }
                    final_columns.push(Arc::new(StringArray::from(b)) as ArrayRef);
                }
                dt => {
                    return Err(anyhow!(
                        "DataType {:?} not supported for sort materialization",
                        dt
                    ));
                }
            }
        }

        Ok(CombineResult::Materialised(RecordBatch::try_new(
            schema,
            final_columns,
        )?))
    }

    fn sink(&self, ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
        let mut local_state = self.partitions[ctx.core_id % self.total_cores]
            .lock()
            .unwrap();
        let batch_idx = local_state.batches.len();
        let num_rows = input.num_rows();

        if num_rows == 0 {
            return Ok(SinkResult::NeedMoreInput);
        }

        let cap = match self.fetch {
            Some(f) => f + self.skip,
            None => usize::MAX,
        };

        let descending: Vec<bool> = self.sort_columns.iter().map(|c| c.opt.descending).collect();

        for r_idx in 0..num_rows {
            let mut sort_values = Vec::with_capacity(self.sort_columns.len());
            for sort_col in &self.sort_columns {
                let col = input.column(sort_col.col_idx);
                let val = match col.data_type() {
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                        let v = HashTable::extract_key_to_i64(col.as_ref(), r_idx).unwrap_or(0);
                        SortValue::Int(v)
                    }
                    DataType::Float32 => {
                        let f = col
                            .as_any()
                            .downcast_ref::<PrimitiveArray<f32>>()
                            .unwrap()
                            .value(r_idx);
                        let bits = f.to_bits();
                        let order_bits = if f >= 0.0 { bits ^ (1 << 31) } else { !bits };
                        SortValue::Float(order_bits as u64)
                    }
                    DataType::Float64 => {
                        let f = col
                            .as_any()
                            .downcast_ref::<PrimitiveArray<f64>>()
                            .unwrap()
                            .value(r_idx);
                        let bits = f.to_bits();
                        let order_bits = if f >= 0.0 { bits ^ (1 << 63) } else { !bits };
                        SortValue::Float(order_bits)
                    }
                    DataType::Utf8 => {
                        let s = col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(r_idx);
                        SortValue::Str(GermanString::from_str(s))
                    }
                    _ => SortValue::Int(0),
                };
                sort_values.push(val)
            }

            let item = Item {
                batch_idx,
                row_idx: r_idx,
                sort_values,
                descending: descending.clone(),
            };

            if local_state.heap.len() < cap {
                local_state.heap.push(item);
            } else {
                if let Some(root) = local_state.heap.peek() {
                    if item < *root {
                        if let Some(mut root) = local_state.heap.peek_mut() {
                            *root = item;
                        }
                    } // `root` drops here -> execute shift_down(0) operator
                }
            }
        }

        local_state.batches.push(input);
        Ok(SinkResult::NeedMoreInput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_sort_sink_single_column_asc_limit() {
        // Query: SELECT id ORDER BY id ASC LIMIT 2 OFFSET 1
        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        let sort_sink = PhysicalSortSink::new(
            vec![SortColumn {
                col_idx: 0,
                opt: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            }],
            1,       // skip
            Some(2), // fetch
            1,       // total_cores
        );

        let col: ArrayRef = Arc::new(PrimitiveArray::from(vec![40i32, 10, 30, 20, 50]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let mut ctx = SinkContext {
            core_id: 0,
            pipeline_id: 10,
        };
        sort_sink.sink(&mut ctx, batch).unwrap();

        let res = sort_sink.combine().unwrap();
        match res {
            CombineResult::Materialised(out_batch) => {
                assert_eq!(out_batch.num_rows(), 2);
                let out_id = out_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();
                // Sorted: 10, 20, 30, 40, 50
                // Skip 1 (10), take 2: 20, 30
                assert_eq!(out_id.value(0), 20);
                assert_eq!(out_id.value(1), 30);
            }
            _ => panic!("Expected Materialised result from SortSink"),
        }
    }

    #[test]
    fn test_sort_sink_multi_column_with_german_string() {
        // Schema: dept (Utf8), salary (Int32)
        // Query: ORDER BY dept ASC, salary DESC LIMIT 3
        let schema = Arc::new(Schema::new(vec![
            Field {
                name: "dept".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
            Field {
                name: "salary".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));

        let sort_sink = PhysicalSortSink::new(
            vec![
                SortColumn {
                    col_idx: 0,
                    opt: SortOptions {
                        descending: false, // dept ASC
                        nulls_first: false,
                    },
                },
                SortColumn {
                    col_idx: 1,
                    opt: SortOptions {
                        descending: true, // salary DESC
                        nulls_first: false,
                    },
                },
            ],
            0,       // skip
            Some(3), // fetch
            2,       // total_cores
        );

        // Batch 1 (from Core 0):
        // Engineering: 100
        // Sales: 300
        let col_dept1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Engineering"), Some("Sales")]));
        let col_sal1: ArrayRef = Arc::new(PrimitiveArray::from(vec![100i32, 300]));
        let batch1 = RecordBatch::try_new(schema.clone(), vec![col_dept1, col_sal1]).unwrap();

        // Batch 2 (from Core 1):
        // Engineering: 500
        // Sales: 200
        let col_dept2: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Engineering"), Some("Sales")]));
        let col_sal2: ArrayRef = Arc::new(PrimitiveArray::from(vec![500i32, 200]));
        let batch2 = RecordBatch::try_new(schema.clone(), vec![col_dept2, col_sal2]).unwrap();

        let mut ctx0 = SinkContext {
            core_id: 0,
            pipeline_id: 10,
        };
        let mut ctx1 = SinkContext {
            core_id: 1,
            pipeline_id: 10,
        };
        sort_sink.sink(&mut ctx0, batch1).unwrap();
        sort_sink.sink(&mut ctx1, batch2).unwrap();

        let res = sort_sink.combine().unwrap();
        match res {
            CombineResult::Materialised(out_batch) => {
                assert_eq!(out_batch.num_rows(), 3);
                let out_dept = out_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let out_sal = out_batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();

                // Expected order:
                // 1. Engineering 500
                // 2. Engineering 100
                // 3. Sales 300
                assert_eq!(out_dept.value(0), "Engineering");
                assert_eq!(out_sal.value(0), 500);

                assert_eq!(out_dept.value(1), "Engineering");
                assert_eq!(out_sal.value(1), 100);

                assert_eq!(out_dept.value(2), "Sales");
                assert_eq!(out_sal.value(2), 300);
            }
            _ => panic!("Expected Materialised result from SortSink"),
        }
    }

    #[test]
    fn test_sort_sink_dfs_binary_tree_merge() {
        // Test DFS tree merge with 4 cores: [4], [3], [2], [1] -> sorted [1, 2, 3, 4]
        let schema = Arc::new(Schema::new(vec![Field {
            name: "val".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        let sort_sink = PhysicalSortSink::new(
            vec![SortColumn {
                col_idx: 0,
                opt: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            }],
            0,
            None, // Full sort
            4,    // 4 cores
        );

        let vals = vec![40i32, 30, 20, 10];
        for (core_id, val) in vals.into_iter().enumerate() {
            let col: ArrayRef = Arc::new(PrimitiveArray::from(vec![val]));
            let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
            let mut ctx = SinkContext {
                core_id,
                pipeline_id: 10,
            };
            sort_sink.sink(&mut ctx, batch).unwrap();
        }

        let res = sort_sink.combine().unwrap();
        match res {
            CombineResult::Materialised(out_batch) => {
                assert_eq!(out_batch.num_rows(), 4);
                let out_val = out_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();
                assert_eq!(out_val.value(0), 10);
                assert_eq!(out_val.value(1), 20);
                assert_eq!(out_val.value(2), 30);
                assert_eq!(out_val.value(3), 40);
            }
            _ => panic!("Expected Materialised result from SortSink"),
        }
    }
}

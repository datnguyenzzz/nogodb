use std::{
    cmp,
    collections::HashMap,
    mem,
    sync::{Arc, Mutex},
};

use anyhow::Result;

use crate::{
    arrow::{
        ArrayRef, DataType, Field, RecordBatch, Schema,
        array::{PrimitiveArray, select},
    },
    execution::{
        hash_join::{hash_combine, hash_table::HashTable, hash64},
        pipeline::{
            CombineResult::{self, Materialised},
            PhysicalSink, SinkContext, SinkResult,
        },
        scheduler::{InterCoreMessage, MailBoxSender},
    },
    planner::AggregationFn,
};

pub struct AggState {
    pub group_keys: Vec<i64>,
    pub accumulators: Vec<i64>,
    pub avg_counts: Vec<i64>,
}

pub struct AggregateDeclaration {
    pub col_idx: usize,
    pub op: AggregationFn,
    pub data_type: DataType,
}

pub struct PhysicalAggregateSink {
    total_cores: usize,
    group_col_indexes: Vec<usize>,
    mailboxes: Arc<Vec<MailBoxSender>>,
    partitions: Vec<Mutex<HashMap<u64, Vec<AggState>>>>,
    agg_decls: Vec<AggregateDeclaration>,
}

impl PhysicalAggregateSink {
    pub fn new(
        total_cores: usize,
        group_col_indexes: Vec<usize>,
        mailboxes: Arc<Vec<MailBoxSender>>,
        agg_decls: Vec<AggregateDeclaration>,
    ) -> Self {
        let mut partitions = Vec::with_capacity(total_cores);
        for _ in 0..total_cores {
            partitions.push(Mutex::new(HashMap::new()));
        }
        Self {
            total_cores,
            group_col_indexes,
            mailboxes,
            partitions,
            agg_decls,
        }
    }

    pub fn accumulate_local(&self, core_id: usize, batch: RecordBatch) -> Result<()> {
        let mut local_map = self.partitions[core_id].lock().unwrap();
        let len = batch.num_rows();
        for i in 0..len {
            let mut hash = 0u64;
            let mut group_keys = Vec::with_capacity(self.group_col_indexes.len());
            let mut is_null_group = false;

            for &col_idx in &self.group_col_indexes {
                let col = batch.column(col_idx);
                if let Some(key) = HashTable::extract_key_to_i64(col.as_ref(), i) {
                    hash = hash_combine(hash, hash64(key as u64));
                    group_keys.push(key)
                } else {
                    is_null_group = true;
                    break;
                }
            }

            if is_null_group {
                continue;
            }

            let mut agg_values = Vec::with_capacity(self.agg_decls.len());
            let mut skip_row = false;

            for decl in &self.agg_decls {
                let col = batch.column(decl.col_idx);
                let val = match col.data_type() {
                    DataType::Utf8 => {
                        if decl.op == AggregationFn::Count {
                            if col.is_null(i) { None } else { Some(1) }
                        } else {
                            None
                        }
                    }
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean => HashTable::extract_key_to_i64(col.as_ref(), i),
                };

                if let Some(v) = val {
                    agg_values.push(v)
                } else {
                    skip_row = true;
                    break;
                }
            }

            if skip_row {
                continue;
            }

            let bucket = local_map.entry(hash).or_insert_with(Vec::new);
            let mut found = false;
            for state in bucket.iter_mut() {
                if state.group_keys == group_keys {
                    // existing entry
                    for (i, decl) in self.agg_decls.iter().enumerate() {
                        let current = state.accumulators[i];
                        let val = agg_values[i];
                        match batch.column(decl.col_idx).data_type() {
                            DataType::Float32 => {
                                let curr_f = f32::from_bits(current as u32);
                                let val_f = f32::from_bits(val as u32);
                                state.accumulators[i] = match decl.op {
                                    AggregationFn::Sum => (curr_f + val_f).to_bits() as i64,
                                    AggregationFn::Count => current + 1,
                                    AggregationFn::Min => curr_f.min(val_f).to_bits() as i64,
                                    AggregationFn::Max => curr_f.max(val_f).to_bits() as i64,
                                    AggregationFn::Avg => {
                                        state.avg_counts[i] += 1;
                                        (curr_f + val_f).to_bits() as i64
                                    }
                                }
                            }
                            DataType::Float64 => {
                                let curr_f = f64::from_bits(current as u64);
                                let val_f = f64::from_bits(val as u64);
                                state.accumulators[i] = match decl.op {
                                    AggregationFn::Sum => (curr_f + val_f).to_bits() as i64,
                                    AggregationFn::Count => current + 1,
                                    AggregationFn::Min => curr_f.min(val_f).to_bits() as i64,
                                    AggregationFn::Max => curr_f.max(val_f).to_bits() as i64,
                                    AggregationFn::Avg => {
                                        state.avg_counts[i] += 1;
                                        (curr_f + val_f).to_bits() as i64
                                    }
                                }
                            }
                            DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Boolean => {
                                state.accumulators[i] = match decl.op {
                                    AggregationFn::Sum => current + val,
                                    AggregationFn::Count => current + 1,
                                    AggregationFn::Min => cmp::min(current, val),
                                    AggregationFn::Max => cmp::max(current, val),
                                    AggregationFn::Avg => {
                                        state.avg_counts[i] += 1;
                                        current + val
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    found = true;
                    break;
                }
            }

            if !found {
                // new entry
                let mut accumulators = Vec::with_capacity(self.agg_decls.len());
                for (i, decl) in self.agg_decls.iter().enumerate() {
                    let val = agg_values[i];
                    accumulators.push(match decl.op {
                        AggregationFn::Count => 1,
                        _ => val,
                    })
                }

                bucket.push(AggState {
                    group_keys,
                    accumulators,
                    avg_counts: vec![1i64; self.agg_decls.len()],
                })
            }
        }

        Ok(())
    }
}

impl PhysicalSink for PhysicalAggregateSink {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn sink(&self, ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
        let len = input.num_rows();
        let mut buckets: Vec<Vec<i32>> = (0..self.total_cores)
            .map(|_| Vec::with_capacity((len + self.total_cores - 1) / self.total_cores))
            .collect();

        for i in 0..len {
            let mut hash = 0u64;
            for &col_idx in &self.group_col_indexes {
                let col = input.column(col_idx);
                if let Some(key) = HashTable::extract_key_to_i64(col.as_ref(), i) {
                    hash = hash_combine(hash, hash64(key as u64));
                }
            }
            let cid = (hash as usize) % self.total_cores;
            buckets[cid].push(i as i32);
        }

        for (cid, row_ids) in buckets.into_iter().enumerate() {
            if row_ids.is_empty() {
                continue;
            }

            let index_array = PrimitiveArray::from(row_ids);
            let projected: Vec<ArrayRef> = input
                .columns()
                .iter()
                .map(|col| select::take(col.as_ref(), &index_array).unwrap())
                .collect();
            let batch = RecordBatch::try_new(input.schema().clone(), projected)?;

            if cid == ctx.core_id {
                self.accumulate_local(ctx.core_id, batch)?;
            } else {
                self.mailboxes[cid].submit(InterCoreMessage::AggregateShuffle {
                    pipeline_id: ctx.pipeline_id,
                    batch,
                })?;
            }
        }

        Ok(SinkResult::NeedMoreInput)
    }

    fn combine(&self) -> Result<CombineResult> {
        let mut total_group = 0;
        let mut partitions = Vec::with_capacity(self.total_cores);
        for i in 0..self.total_cores {
            let mut local_map = self.partitions[i].lock().unwrap();
            let partition = mem::take(&mut *local_map);
            for bucket in partition.values() {
                total_group += bucket.len();
            }
            partitions.push(partition)
        }

        if total_group == 0 {
            return Ok(CombineResult::Empty);
        }

        let mut group_keys = Vec::with_capacity(self.group_col_indexes.len());
        for _ in 0..self.group_col_indexes.len() {
            group_keys.push(Vec::with_capacity(total_group))
        }
        let mut metrics = Vec::with_capacity(self.agg_decls.len());
        for _ in 0..self.agg_decls.len() {
            metrics.push(Vec::with_capacity(total_group))
        }

        for partition in partitions {
            for bucket in partition.into_values() {
                for state in bucket {
                    for (i, key) in state.group_keys.into_iter().enumerate() {
                        group_keys[i].push(key)
                    }

                    for (i, decl) in self.agg_decls.iter().enumerate() {
                        let acc = state.accumulators[i];
                        if decl.op == AggregationFn::Avg {
                            let count = state.avg_counts[i] as f64;
                            let sum = if matches!(decl.data_type, DataType::Float64) {
                                f64::from_bits(acc as u64)
                            } else if matches!(decl.data_type, DataType::Float32) {
                                f32::from_bits(acc as u32) as f64
                            } else {
                                acc as f64
                            };
                            let avg = sum / count;
                            metrics[i].push(avg.to_bits() as i64);
                        } else {
                            metrics[i].push(acc)
                        }
                    }
                }
            }
        }

        let mut final_columns: Vec<ArrayRef> =
            Vec::with_capacity(self.group_col_indexes.len() + self.agg_decls.len());
        for keys in group_keys {
            final_columns.push(Arc::new(PrimitiveArray::from(keys)))
        }
        for (i, metric) in metrics.into_iter().enumerate() {
            let decl = &self.agg_decls[i];
            if decl.op == AggregationFn::Avg || matches!(decl.data_type, DataType::Float64) {
                let float_values: Vec<f64> = metric
                    .into_iter()
                    .map(|bits| f64::from_bits(bits as u64))
                    .collect();
                final_columns.push(Arc::new(PrimitiveArray::from(float_values)));
            } else {
                final_columns.push(Arc::new(PrimitiveArray::from(metric)));
            }
        }

        let mut fields = Vec::with_capacity(final_columns.len());
        for (i, _col_idx) in self.group_col_indexes.iter().enumerate() {
            fields.push(Field {
                name: format!("group_key_{}", i),
                data_type: DataType::Int64,
                nullable: true,
            });
        }

        for (i, decl) in self.agg_decls.iter().enumerate() {
            let data_type =
                if decl.op == AggregationFn::Avg || matches!(decl.data_type, DataType::Float64) {
                    DataType::Float64
                } else {
                    DataType::Int64
                };
            fields.push(Field {
                name: format!("agg_metric_{}", i),
                data_type,
                nullable: true,
            });
        }

        let final_schema = Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(final_schema), final_columns)?;
        Ok(Materialised(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{Field, Schema, array::PrimitiveArray, array::StringArray};
    use std::sync::Arc;
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn test_physical_aggregate_sink_comprehensive() {
        let (tx, _rx) = unbounded_channel();
        let mailbox = MailBoxSender { sender: tx };
        let mailboxes = Arc::new(vec![mailbox]);

        // Input schema: department (String), salary (Int32), rating (Float64)
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
            Field {
                name: "rating".to_string(),
                data_type: DataType::Float64,
                nullable: false,
            },
        ]));

        // Sample data:
        // Dept: "Engineering", "Sales", "Engineering", "Sales"
        // Salary: 100, 200, 300, 400
        // Rating: 4.5, 3.0, 5.5, 5.0
        let col_dept: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Engineering"),
            Some("Sales"),
            Some("Engineering"),
            Some("Sales"),
        ]));
        let col_salary: ArrayRef = Arc::new(PrimitiveArray::from(vec![100i32, 200, 300, 400]));
        let col_rating: ArrayRef = Arc::new(PrimitiveArray::from(vec![4.5f64, 3.0, 5.5, 5.0]));

        let batch = RecordBatch::try_new(schema, vec![col_dept, col_salary, col_rating]).unwrap();

        // Query: SELECT dept, COUNT(*), SUM(salary), MIN(salary), MAX(salary), AVG(rating) GROUP BY dept
        let agg_sink = PhysicalAggregateSink::new(
            1,       // total_cores
            vec![0], // group_by dept
            mailboxes,
            vec![
                AggregateDeclaration {
                    col_idx: 1,
                    op: AggregationFn::Count,
                    data_type: DataType::Int32,
                },
                AggregateDeclaration {
                    col_idx: 1,
                    op: AggregationFn::Sum,
                    data_type: DataType::Int32,
                },
                AggregateDeclaration {
                    col_idx: 1,
                    op: AggregationFn::Min,
                    data_type: DataType::Int32,
                },
                AggregateDeclaration {
                    col_idx: 1,
                    op: AggregationFn::Max,
                    data_type: DataType::Int32,
                },
                AggregateDeclaration {
                    col_idx: 2,
                    op: AggregationFn::Avg,
                    data_type: DataType::Float64,
                },
            ],
        );

        let mut sink_ctx = SinkContext {
            core_id: 0,
            pipeline_id: 10,
        };
        agg_sink.sink(&mut sink_ctx, batch).unwrap();

        // Combine and materialize!
        let combine_result = agg_sink.combine().unwrap();

        match combine_result {
            CombineResult::Materialised(result_batch) => {
                assert_eq!(result_batch.num_rows(), 2); // 2 departments: Engineering, Sales
                assert_eq!(result_batch.num_columns(), 6); // 1 group key + 5 metrics

                // Symmetrically verify values for each group!
                let out_count = result_batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                let out_sum = result_batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                let out_min = result_batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                let out_max = result_batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                let out_avg = result_batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f64>>()
                    .unwrap();

                // Because HashMap order is arbitrary, we test both rows:
                for row in 0..2 {
                    assert_eq!(out_count.value(row), 2); // 2 employees in each dept

                    let sum_val = out_sum.value(row);
                    if sum_val == 400 {
                        // Engineering (100 + 300 = 400)
                        assert_eq!(out_min.value(row), 100);
                        assert_eq!(out_max.value(row), 300);
                        let avg_rating = out_avg.value(row);
                        assert_eq!(avg_rating, 5.0); // (4.5 + 5.5) / 2 = 5.0!
                    } else if sum_val == 600 {
                        // Sales (200 + 400 = 600)
                        assert_eq!(out_min.value(row), 200);
                        assert_eq!(out_max.value(row), 400);
                        let avg_rating = out_avg.value(row);
                        assert_eq!(avg_rating, 4.0); // (3.0 + 5.0) / 2 = 4.0!
                    } else {
                        panic!("Unexpected sum value: {}", sum_val);
                    }
                }
            }
            _ => panic!("Expected Materialised RecordBatch from combine()"),
        }
    }
}

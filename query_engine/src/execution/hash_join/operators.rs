use std::{
    cmp, mem,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::{
    arrow::{
        Array, ArrayRef, DataType, RecordBatch, Schema, array::{BooleanArray, PrimitiveArray, StringArray, select},
    }, execution::{
        dispatcher::Dispatcher,
        hash_join::{hash_combine, hash_table::HashTable, hash64},
        pipeline::{OperatorContext, PhysicalOperator, PhysicalSink, SinkContext, SinkResult},
        scheduler::{InterCoreMessage, MailBoxSender},
    },
};

pub struct PhysicalBuildSink {
    join_id: usize,
    /// Build column indexes
    col_indexes: Vec<usize>,
    total_cores: usize,
    dispatcher: Arc<Dispatcher>,
    mailboxes: Arc<Vec<MailBoxSender>>,
    /// Accumulated build batches shuffled from other cores, owned exclusively by this core's local heap
    local_batches: Mutex<Vec<RecordBatch>>,
    /// Thread-local compiled known-size fit HashTable
    local_hash_table: Mutex<Option<HashTable>>,
}

impl PhysicalBuildSink {
    pub fn new(
        join_id: usize,
        col_indexes: Vec<usize>,
        total_cores: usize,
        dispatcher: Arc<Dispatcher>,
        mailboxes: Arc<Vec<MailBoxSender>>,
    ) -> Self {
        Self {
            join_id,
            col_indexes,
            total_cores,
            dispatcher,
            mailboxes,
            local_batches: Mutex::new(Vec::new()),
            local_hash_table: Mutex::new(None),
        }
    }

    pub fn push_batch_local(&self, batch: RecordBatch) {
        let mut local_batches = self.local_batches.lock().unwrap();
        local_batches.push(batch);
    }
}

impl PhysicalSink for PhysicalBuildSink {
    fn sink(&self, ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
        let len = input.num_rows();
        let mut buckets: Vec<Vec<i32>> = (0..self.total_cores)
            .map(|_| Vec::with_capacity((len + self.total_cores - 1) / self.total_cores))
            .collect();

        for i in 0..len {
            let hash = HashTable::hash_row_keys(&input, &self.col_indexes, i).unwrap_or_default();
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
                let mut local_batches = self.local_batches.lock().unwrap();
                local_batches.push(batch);
            } else {
                // Submit the shuffled batch to the corressponding remote core
                self.mailboxes[cid].submit(InterCoreMessage::JoinBuildShuffle {
                    pipeline_id: ctx.pipeline_id,
                    batch,
                })?;
            }
        }

        Ok(SinkResult::NeedMoreInput)
    }

    fn combine(&self) -> Result<()> {
        let mut local_batches = self.local_batches.lock().unwrap();
        let mut local_table = self.local_hash_table.lock().unwrap();

        let batches = mem::take(&mut *local_batches);
        let table = HashTable::try_build(batches, &self.col_indexes)?;
        // Publish boundaries globally to the dispatcher
        // for background I/O pruning numeric data type only
        if !table.hashes.is_empty() {
            for &idx in &self.col_indexes {
                let mut min = i64::MAX;
                let mut max = i64::MIN;
                let mut is_numeric = true;

                for batch in &table.build_batches {
                    let col = batch.column(idx);
                    match col.data_type() {
                        DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Float64 => {
                            for row_idx in 0..batch.num_rows() {
                                if let Some(key) =
                                    HashTable::extract_key_to_i64(col.as_ref(), row_idx)
                                {
                                    min = cmp::min(min, key);
                                    max = cmp::max(max, key);
                                }
                            }
                        }
                        _ => {
                            is_numeric = false;
                            break;
                        }
                    }
                }
                if is_numeric && min <= max {
                    self.dispatcher
                        .publish_join_bounds(self.join_id, idx, min, max);
                }
            }
        }

        *local_table = Some(table);
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct PhysicalProbeOperator {
    total_cores: usize,
    /// Probe column indexes
    probe_col_indexes: Vec<usize>,
    mailboxes: Arc<Vec<MailBoxSender>>,
    build_sink: Arc<PhysicalBuildSink>,
    build_col_indexes: Vec<usize>,
}

impl PhysicalProbeOperator {
    pub fn probe_and_respond_local(
        &self,
        probe_batch: &RecordBatch,
        response_tx: UnboundedSender<RecordBatch>,
    ) -> Result<()> {
        let table_guard = self.build_sink.local_hash_table.lock().unwrap();
        let table = table_guard
            .as_ref()
            .ok_or_else(|| anyhow!("Local hash table not compiled yet!"))?;

        if table.directory.is_empty() {
            return Ok(());
        }

        let mut matched_probe = Vec::new();
        let mut matched_build = Vec::new();
        for i in 0..probe_batch.num_rows() {
            table.try_probe(
                probe_batch,
                &self.probe_col_indexes,
                i,
                &self.build_col_indexes,
                |batch_idx, row_idx| {
                    matched_probe.push(i as i32);
                    matched_build.push((batch_idx, row_idx));
                },
            );
        }

        if !matched_probe.is_empty() {
            let mut joined = Vec::with_capacity(
                probe_batch.num_columns() + table.build_batches[0].num_columns(),
            );

            let probe_indexes = PrimitiveArray::from(matched_probe);
            for col in probe_batch.columns() {
                let taken = select::take(col.as_ref(), &probe_indexes)?;
                joined.push(taken);
            }

            let taken_build = self.materialize_build_columns(table, &matched_build)?;
            joined.extend(taken_build);

            let joined_schema =
                Schema::merge(probe_batch.schema(), &table.build_batches[0].schema());

            let joined_batch = RecordBatch::try_new(Arc::new(joined_schema), joined)?;

            let _ = response_tx.send(joined_batch);
        }

        Ok(())
    }

    fn materialize_build_columns(
        &self,
        table: &HashTable,
        matched_build_rows: &[(usize, usize)],
    ) -> Result<Vec<ArrayRef>> {
        let num_cols = table.build_batches[0].num_columns();
        let num_matches = matched_build_rows.len();

        let mut output_columns = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let dt = table.build_batches[0].column(col_idx).data_type();

            match dt {
                DataType::Int32 => {
                    let mut builder = Vec::with_capacity(num_matches);
                    for &(batch_idx, row_idx) in matched_build_rows {
                        let col = table.build_batches[batch_idx]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i32>>()
                            .unwrap();
                        if col.is_null(row_idx) {
                            builder.push(None);
                        } else {
                            builder.push(Some(col.value(row_idx)));
                        }
                    }
                    output_columns.push(Arc::new(PrimitiveArray::from(builder)) as ArrayRef);
                }
                DataType::Int64 => {
                    let mut builder = Vec::with_capacity(num_matches);
                    for &(batch_idx, row_idx) in matched_build_rows {
                        let col = table.build_batches[batch_idx]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i64>>()
                            .unwrap();
                        if col.is_null(row_idx) {
                            builder.push(None);
                        } else {
                            builder.push(Some(col.value(row_idx)));
                        }
                    }
                    output_columns.push(Arc::new(PrimitiveArray::from(builder)) as ArrayRef);
                }
                DataType::Float64 => {
                    let mut builder = Vec::with_capacity(num_matches);
                    for &(batch_idx, row_idx) in matched_build_rows {
                        let col = table.build_batches[batch_idx]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<f64>>()
                            .unwrap();
                        if col.is_null(row_idx) {
                            builder.push(None);
                        } else {
                            builder.push(Some(col.value(row_idx)));
                        }
                    }
                    output_columns.push(Arc::new(PrimitiveArray::from(builder)) as ArrayRef);
                }
                DataType::Boolean => {
                    let mut builder = Vec::with_capacity(num_matches);
                    for &(batch_idx, row_idx) in matched_build_rows {
                        let col = table.build_batches[batch_idx]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap();
                        if col.is_null(row_idx) {
                            builder.push(None);
                        } else {
                            builder.push(Some(col.value(row_idx)));
                        }
                    }
                    output_columns.push(
                        Arc::new(BooleanArray::from(builder)) as ArrayRef
                    );
                }
                DataType::Utf8 => {
                    let mut builder = Vec::with_capacity(num_matches);
                    for &(batch_idx, row_idx) in matched_build_rows {
                        let col = table.build_batches[batch_idx]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        if col.is_null(row_idx) {
                            builder.push(None);
                        } else {
                            builder.push(Some(col.value(row_idx)));
                        }
                    }
                    output_columns.push(Arc::new(StringArray::from(builder)) as ArrayRef);
                }
                _ => {
                    return Err(anyhow!(
                        "Vectorized build-side materialization not yet supported for DataType: {:?}",
                        dt
                    ));
                }
            }
        }

        Ok(output_columns)
    }
}

impl PhysicalOperator for PhysicalProbeOperator {
    fn execute(&self, ctx: &OperatorContext, input: &RecordBatch) -> Result<Option<RecordBatch>> {
        let len = input.num_rows();
        let mut buckets: Vec<Vec<i32>> = (0..self.total_cores)
            .map(|_| Vec::with_capacity((len + self.total_cores - 1) / self.total_cores))
            .collect();

        for i in 0..len {
            let mut hash = 0u64;
            let mut is_null = false;
            for &idx in &self.probe_col_indexes {
                let col = input.column(idx);
                if let Some(key) = HashTable::extract_key_to_i64(col.as_ref(), i) {
                    hash = hash_combine(hash, hash64(key as u64));
                } else {
                    is_null = true;
                    break;
                }
            }
            if !is_null {
                let cid = (hash as usize) % self.total_cores;
                buckets[cid].push(i as i32);
            }
        }

        let (tx, mut rx) = unbounded_channel::<RecordBatch>();
        let mut joined_batches = Vec::new();

        for (cid, row_indexes) in buckets.into_iter().enumerate() {
            if row_indexes.is_empty() {
                continue;
            }
            let index_arr = PrimitiveArray::from(row_indexes);
            let projected: Vec<ArrayRef> = input
                .columns()
                .iter()
                .map(|col| select::take(col.as_ref(), &index_arr).unwrap())
                .collect();
            let probe_batch = RecordBatch::try_new(input.schema().clone(), projected)?;
            if cid == ctx.core_id {
                self.probe_and_respond_local(&probe_batch, tx.clone())?;
            } else {
                // Submit the shuffled batch to the corressponding remote core
                self.mailboxes[cid].submit(InterCoreMessage::JoinProbeShuffle {
                    pipeline_id: ctx.pipeline_id,
                    batch: probe_batch,
                    response_tx: tx.clone(),
                })?;
            }
        }

        drop(tx);
        while let Ok(batch) = rx.try_recv() {
            joined_batches.push(batch)
        }

        if joined_batches.is_empty() {
            return Ok(None);
        }

        select::concat_batches(joined_batches[0].schema(), &joined_batches).map(Some)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{Field, Schema, array::StringArray};
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn test_physical_build_sink_and_probe_operator() {
        let (tx, _rx) = unbounded_channel();
        let mailbox = MailBoxSender { sender: tx };
        let mailboxes = Arc::new(vec![mailbox]);
        let dispatcher = Arc::new(Dispatcher::new(1));

        // 1. Build relation (id, name)
        let build_schema = Arc::new(Schema::new(vec![
            Field {
                name: "b_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "b_name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]));
        let b_id: ArrayRef = Arc::new(PrimitiveArray::from(vec![10i32, 20]));
        let b_name: ArrayRef = Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")]));
        let build_batch = RecordBatch::try_new(build_schema, vec![b_id, b_name]).unwrap();

        let build_sink = Arc::new(PhysicalBuildSink::new(
            100,     // join_id
            vec![0], // build_col_indexes
            1,       // total_cores
            dispatcher.clone(),
            mailboxes.clone(),
        ));

        let mut sink_ctx = SinkContext {
            core_id: 0,
            pipeline_id: 10,
        };
        build_sink.sink(&mut sink_ctx, build_batch).unwrap();
        build_sink.combine().unwrap();

        // 2. Probe relation (id, age)
        let probe_schema = Arc::new(Schema::new(vec![
            Field {
                name: "p_id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "p_age".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ]));
        let p_id: ArrayRef = Arc::new(PrimitiveArray::from(vec![20i32, 30]));
        let p_age: ArrayRef = Arc::new(PrimitiveArray::from(vec![45i32, 50]));
        let probe_batch = RecordBatch::try_new(probe_schema, vec![p_id, p_age]).unwrap();

        let probe_op = PhysicalProbeOperator {
            total_cores: 1,
            probe_col_indexes: vec![0],
            mailboxes,
            build_sink,
            build_col_indexes: vec![0],
        };

        let op_ctx = OperatorContext {
            core_id: 0,
            pipeline_id: 20,
        };

        // 3. Execute the vectorized physical probe!
        // Should join matching row (20, Bob) with (20, 45)!
        let joined_option = probe_op.execute(&op_ctx, &probe_batch).unwrap();
        assert!(joined_option.is_some());

        let joined_batch = joined_option.unwrap();
        assert_eq!(joined_batch.num_rows(), 1);
        assert_eq!(joined_batch.num_columns(), 4); // p_id, p_age, b_id, b_name

        let out_id = joined_batch
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();
        assert_eq!(out_id.value(0), 20);

        let out_age = joined_batch
            .column(1)
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap();
        assert_eq!(out_age.value(0), 45);

        let out_b_name = joined_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(out_b_name.value(0), "Bob");
    }
}

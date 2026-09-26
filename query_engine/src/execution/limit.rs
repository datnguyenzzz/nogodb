use std::{
    cmp, mem,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use anyhow::Result;

use crate::{
    arrow::{RecordBatch, array::select::concat_batches},
    execution::{
        dispatcher::Dispatcher,
        pipeline::{
            CombineResult, OperatorContext, PhysicalOperator, PhysicalSink, SinkContext, SinkResult,
        },
    },
};

pub struct PhysicalLimitOperator {
    pub skip: usize,
    pub fetch: usize,
    pub is_pre_sort: bool,
    pub global_counter: Arc<AtomicUsize>,
    /// Global early termination, stops all reactors from pulling further morsels
    pub is_finished: Arc<AtomicBool>,
    /// Per-core local counters
    pub local_counters: Vec<AtomicUsize>,
    pub dispatcher: Arc<Dispatcher>,
}

impl PhysicalLimitOperator {
    pub fn new(
        skip: usize,
        fetch: usize,
        is_pre_sort: bool,
        total_cores: usize,
        dispatcher: Arc<Dispatcher>,
    ) -> Self {
        let mut local_counters = Vec::with_capacity(total_cores);
        for _ in 0..total_cores {
            local_counters.push(AtomicUsize::new(0));
        }

        Self {
            skip,
            fetch,
            is_pre_sort,
            global_counter: Arc::new(AtomicUsize::new(0)),
            is_finished: Arc::new(AtomicBool::new(false)),
            local_counters,
            dispatcher,
        }
    }
}

impl PhysicalOperator for PhysicalLimitOperator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn execute(&self, ctx: &OperatorContext, input: &RecordBatch) -> Result<Option<RecordBatch>> {
        let len = input.num_rows();
        if len == 0 {
            return Ok(None);
        }

        let max_needed = self.fetch + self.skip;

        if self.is_pre_sort {
            // We need to sort (order by) afterward. Thus to make sure we would
            // have enough dataset, each worker caps at skip+fetch locally
            let local_counter = &self.local_counters[ctx.core_id % self.local_counters.len()];
            let curr = local_counter.load(Ordering::Relaxed);
            if curr >= max_needed {
                return Ok(None);
            }

            let needed = max_needed - curr;
            let to_take = cmp::min(needed, len);
            local_counter.fetch_add(to_take, Ordering::Relaxed);
            Ok(Some(input.slice(0, to_take)))
        } else {
            // Unordered query: cap at skip+fetch globally across all workers
            if self.is_finished.load(Ordering::Relaxed) {
                return Ok(None);
            }

            let start_offset = self.global_counter.fetch_add(len, Ordering::Relaxed);
            if start_offset >= max_needed {
                // Signal the Dispatcher to cancel remaining morsels
                self.is_finished.store(true, Ordering::Relaxed);
                let _ = self.dispatcher.cancel_pipeline(ctx.pipeline_id);
                return Ok(None);
            }

            let end_offset = start_offset + len;
            let to_take = if end_offset > max_needed {
                self.is_finished.store(true, Ordering::Relaxed);
                let _ = self.dispatcher.cancel_pipeline(ctx.pipeline_id);
                max_needed - start_offset
            } else {
                len
            };

            Ok(Some(input.slice(0, to_take)))
        }
    }
}

pub struct PhysicalLimitSink {
    pub skip: usize,
    pub fetch: usize,
    pub batches: Mutex<Vec<RecordBatch>>,
}

impl PhysicalLimitSink {
    pub fn new(skip: usize, fetch: usize) -> Self {
        Self {
            skip,
            fetch,
            batches: Mutex::new(Vec::new()),
        }
    }
}

impl PhysicalSink for PhysicalLimitSink {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn combine(&self) -> Result<CombineResult> {
        let mut lock = self.batches.lock().unwrap();
        let batches = mem::take(&mut *lock);
        if batches.is_empty() {
            return Ok(CombineResult::Empty);
        }

        let batch = concat_batches(batches[0].schema(), &batches)?;
        let len = batch.num_rows();
        if len <= self.skip {
            return Ok(CombineResult::Empty);
        }

        let start = self.skip;
        let to_take = cmp::min(self.fetch, len - start);

        if to_take == 0 {
            return Ok(CombineResult::Empty);
        }

        Ok(CombineResult::Materialised(batch.slice(start, to_take)))
    }

    fn sink(&self, _ctx: &mut SinkContext, input: RecordBatch) -> Result<SinkResult> {
        let mut l_batch = self.batches.lock().unwrap();
        l_batch.push(input);
        Ok(SinkResult::NeedMoreInput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{ArrayRef, DataType, Field, Schema, array::PrimitiveArray};
    use std::sync::Arc;

    #[test]
    fn test_physical_limit_operator_global_unordered() {
        let dispatcher = Arc::new(Dispatcher::new(1));
        let limit_op = PhysicalLimitOperator::new(2, 3, false, 1, dispatcher);

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        let ctx = OperatorContext {
            core_id: 0,
            pipeline_id: 10,
        };

        // Batch 1: 4 rows [0, 1, 2, 3]
        let col1: ArrayRef = Arc::new(PrimitiveArray::from(vec![0i32, 1, 2, 3]));
        let batch1 = RecordBatch::try_new(schema.clone(), vec![col1]).unwrap();
        let res1 = limit_op.execute(&ctx, &batch1).unwrap().unwrap();
        assert_eq!(res1.num_rows(), 4); // All 4 emitted because end_offset (4) <= 5

        // Batch 2: 4 rows [4, 5, 6, 7]
        let col2: ArrayRef = Arc::new(PrimitiveArray::from(vec![4i32, 5, 6, 7]));
        let batch2 = RecordBatch::try_new(schema.clone(), vec![col2]).unwrap();
        let res2 = limit_op.execute(&ctx, &batch2).unwrap().unwrap();
        assert_eq!(res2.num_rows(), 1); // Only 1 row emitted to hit max_needed (5)!
        assert!(limit_op.is_finished.load(Ordering::Relaxed));

        // Batch 3: Subsequent batch should be immediately dropped!
        let res3 = limit_op.execute(&ctx, &batch2).unwrap();
        assert!(res3.is_none());
    }

    #[test]
    fn test_physical_limit_operator_local_pre_sort() {
        let dispatcher = Arc::new(Dispatcher::new(2));
        // Target: LIMIT 2 OFFSET 1 -> max_needed = 3 per worker
        let limit_op = PhysicalLimitOperator::new(1, 2, true, 2, dispatcher);

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        let col: ArrayRef = Arc::new(PrimitiveArray::from(vec![10i32, 20, 30, 40, 50]));
        let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();

        // Core 0 processes batch: caps at 3!
        let ctx0 = OperatorContext {
            core_id: 0,
            pipeline_id: 10,
        };
        let res0 = limit_op.execute(&ctx0, &batch).unwrap().unwrap();
        assert_eq!(res0.num_rows(), 3);

        // Core 0 tries again: should be dropped!
        let res0_next = limit_op.execute(&ctx0, &batch).unwrap();
        assert!(res0_next.is_none());

        // Core 1 processes batch: also independently caps at 3!
        let ctx1 = OperatorContext {
            core_id: 1,
            pipeline_id: 10,
        };
        let res1 = limit_op.execute(&ctx1, &batch).unwrap().unwrap();
        assert_eq!(res1.num_rows(), 3);
    }

    #[test]
    fn test_physical_limit_sink_slicing() {
        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        // Sink configured for: LIMIT 2 OFFSET 1
        let sink = PhysicalLimitSink::new(1, 2);
        let mut sink_ctx = SinkContext {
            core_id: 0,
            pipeline_id: 10,
        };

        let col: ArrayRef = Arc::new(PrimitiveArray::from(vec![10i32, 20, 30, 40]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
        sink.sink(&mut sink_ctx, batch).unwrap();

        let combine_res = sink.combine().unwrap();
        match combine_res {
            CombineResult::Materialised(out_batch) => {
                assert_eq!(out_batch.num_rows(), 2);
                let out_id = out_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();
                assert_eq!(out_id.value(0), 20); // Skipped 10
                assert_eq!(out_id.value(1), 30);
            }
            _ => panic!("Expected Materialised result from LimitSink"),
        }
    }
}

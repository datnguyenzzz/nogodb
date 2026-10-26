use std::{cmp, sync::Arc};

use anyhow::Result;
use futures::executor::block_on;

use crate::{
    arrow::{RecordBatch, SchemaRef, ipc::record_batch::Reader},
    execution::{
        Morsel,
        dispatcher::Dispatcher,
        pipeline::{PipelineID, ScanMessage},
    },
    planner::DynamicJoinPruner,
    storage::{ChunkMetaData, DataStorage},
};

pub const VECTOR_SIZE: usize = 2048;

pub struct ScanSource {
    pub table_name: String,
    pub schema: SchemaRef,
    pub storage: Arc<dyn DataStorage>,
    pub pruners: Vec<DynamicJoinPruner>,
    pub projections: Option<Vec<usize>>,
}

impl ScanSource {
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        storage: Arc<dyn DataStorage>,
        pruners: Vec<DynamicJoinPruner>,
        projections: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table_name,
            schema,
            storage,
            pruners,
            projections,
        }
    }

    pub fn execute(&self, pid: PipelineID, dispatcher: Arc<Dispatcher>) -> Result<()> {
        let iterator_fut = self.storage.new_iterator(&self.table_name);
        let pipeline = dispatcher.get_pipeline(pid);
        let mut iterator = block_on(iterator_fut)?;
        let mut pushed_morsels = 0;
        let mut global_row_offset = 0;
        let mut chunk_idx = 0;
        while let Some(meta) = block_on(iterator.next_chunk_meta())? {
            // dynamic pruning
            let mut is_disjoint = false;
            let chunk_metadata = ChunkMetaData::try_new(meta)?;
            for pruner in &self.pruners {
                if let Some(col_idx) = self.schema.fields().iter().position(|f| {
                    f.name == pruner.probe_col
                        || f.name.ends_with(&format!(".{}", pruner.probe_col))
                }) {
                    if let Some(chunk_stat) = chunk_metadata.get_column_stat(col_idx) {
                        if let Some(join_stat) = dispatcher.get_join_bounds(pruner.join_id, col_idx)
                        {
                            if chunk_stat.is_disjoint(&join_stat) {
                                is_disjoint = true;
                                break;
                            }
                        }
                    };
                }
            }

            if is_disjoint {
                iterator.skip_current_chunk()?;
                global_row_offset += chunk_metadata.num_rows;
                chunk_idx += 1;
                continue;
            }

            let data = block_on(iterator.read_current_chunk_data())?;
            let mut reader =
                Reader::try_new_with_schema(&data[..], pipeline.schema.clone()).unwrap();
            if let Some(proj) = self.projections.clone() {
                reader = reader.with_projection(proj);
            }
            let batches: Result<Vec<RecordBatch>> = reader.collect();
            let batches = batches.unwrap();

            for batch in batches {
                let mut offset: usize = 0;
                while offset < batch.num_rows() {
                    let page_rows = cmp::min(batch.num_rows() - offset, VECTOR_SIZE);
                    let numa_node = chunk_idx % dispatcher.numa_nodes;

                    let morsel = Morsel {
                        start_row: global_row_offset,
                        num_rows: page_rows,
                        numa_node,
                    };

                    dispatcher.push_scan_message(
                        pid,
                        numa_node,
                        morsel,
                        ScanMessage::Batch(batch.slice(offset, page_rows)),
                    )?;
                    pushed_morsels += 1;
                    global_row_offset += page_rows;
                    chunk_idx += 1;
                    offset += page_rows;
                }
            }
        }

        dispatcher.finish_pushing(pid, pushed_morsels)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ipc::record_batch::Writer;
    use crate::arrow::{DataType, Field, RecordBatch, Schema, array::PrimitiveArray};
    use crate::storage::embedded::tests::MockDataStorage;

    #[test]
    fn test_scan_source_basic_execution() {
        let storage = Arc::new(MockDataStorage::new());

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        let col = Arc::new(PrimitiveArray::from(vec![1i32, 2]));
        let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();

        // Create 2 mock pages
        for _ in 0..2 {
            let mut page = Vec::new();
            let mut writer = Writer::new(&mut page);
            writer.write(&batch).unwrap();
            storage.add_page("orders", page);
        }

        let scan_source = ScanSource {
            table_name: "orders".to_string(),
            schema: schema.clone(),
            storage,
            pruners: vec![],
            projections: None,
        };

        let dispatcher = Arc::new(Dispatcher::new(2));
        let (tx, _rx) = std::sync::mpsc::channel();
        let pipeline = Arc::new(crate::execution::pipeline::Pipeline {
            id: 1,
            source: None,
            downstream_id: None,
            operators: vec![],
            sink: Box::new(crate::execution::limit::PhysicalLimitSink::new(0, 1000)),
            dependencies: vec![],
            partitions: 1,
            schema,
        });

        dispatcher.register(1, vec![], pipeline, tx).unwrap();

        // Execute scan!
        scan_source.execute(1, dispatcher.clone()).unwrap();

        // Verify that 2 morsels were pushed into Dispatcher queues
        let state = dispatcher.state.lock().unwrap();
        let queue = state.work_queues.get(&1).unwrap();
        assert_eq!(queue.total_morsels, Some(2));
        let count_pushed = queue.numa_queues.iter().map(|q| q.len()).sum::<usize>();
        assert_eq!(count_pushed, 2);
    }

    #[test]
    fn test_scan_source_dynamic_join_pruning() {
        use crate::storage::ColumnStat;
        let storage = Arc::new(MockDataStorage::new());

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        // Page 0: id in [10, 20] (Disjoint from join range [150, 180])
        let col0 = Arc::new(PrimitiveArray::from(vec![10i32, 20]));
        let batch0 = RecordBatch::try_new(schema.clone(), vec![col0]).unwrap();
        let mut page0 = Vec::new();
        let mut writer0 = Writer::new(&mut page0);
        writer0.write(&batch0).unwrap();
        storage.add_page("orders", page0);

        // Page 1: id in [100, 200] (Overlaps join range [150, 180])
        let col1 = Arc::new(PrimitiveArray::from(vec![100i32, 200]));
        let batch1 = RecordBatch::try_new(schema.clone(), vec![col1]).unwrap();
        let mut page1 = Vec::new();
        let mut writer1 = Writer::new(&mut page1);
        writer1.write(&batch1).unwrap();
        storage.add_page("orders", page1);

        let pruner = DynamicJoinPruner {
            join_id: 42,
            build_col: "user_id".to_string(),
            probe_col: "id".to_string(),
        };

        let scan_source = ScanSource {
            table_name: "orders".to_string(),
            schema: schema.clone(),
            storage,
            pruners: vec![pruner],
            projections: None,
        };

        let dispatcher = Arc::new(Dispatcher::new(1));
        // Publish active join bounds for join_id 42, column 0: [150, 180]
        dispatcher.publish_join_bounds(42, 0, ColumnStat::Int64 { min: 150, max: 180 });

        let (tx, _rx) = std::sync::mpsc::channel();
        let pipeline = Arc::new(crate::execution::pipeline::Pipeline {
            id: 2,
            source: None,
            downstream_id: None,
            operators: vec![],
            sink: Box::new(crate::execution::limit::PhysicalLimitSink::new(0, 1000)),
            dependencies: vec![],
            partitions: 1,
            schema,
        });

        dispatcher.register(2, vec![], pipeline, tx).unwrap();

        // Execute scan!
        scan_source.execute(2, dispatcher.clone()).unwrap();

        // Verify: Page 0 was PRUNED! Only Page 1 was pushed!
        let state = dispatcher.state.lock().unwrap();
        let queue = state.work_queues.get(&2).unwrap();
        assert_eq!(queue.total_morsels, Some(1)); // Only 1 pushed morsel!
        assert_eq!(queue.numa_queues[0].len(), 1);
    }

    #[test]
    fn test_scan_source_splits_large_batch_into_vector_size_morsels() {
        let storage = Arc::new(MockDataStorage::new());

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

        // Create a large batch with 5,000 rows (> 2 * VECTOR_SIZE = 4096)
        let total_rows = 5000;
        let id_vals: Vec<i32> = (0..total_rows as i32).collect();
        let col = Arc::new(PrimitiveArray::from(id_vals));
        let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();

        let mut page = Vec::new();
        let mut writer = Writer::new(&mut page);
        writer.write(&batch).unwrap();
        storage.add_page("large_table", page);

        let scan_source = ScanSource {
            table_name: "large_table".to_string(),
            schema: schema.clone(),
            storage,
            pruners: vec![],
            projections: None,
        };

        let dispatcher = Arc::new(Dispatcher::new(1));
        let (tx, _rx) = std::sync::mpsc::channel();
        let pipeline = Arc::new(crate::execution::pipeline::Pipeline {
            id: 10,
            source: None,
            downstream_id: None,
            operators: vec![],
            sink: Box::new(crate::execution::limit::PhysicalLimitSink::new(0, 10000)),
            dependencies: vec![],
            partitions: 1,
            schema,
        });

        dispatcher.register(10, vec![], pipeline, tx).unwrap();

        // Execute scan!
        scan_source.execute(10, dispatcher.clone()).unwrap();

        // Expected splitting of 5,000 rows with VECTOR_SIZE = 2048:
        // Morsel 0: start_row = 0, num_rows = 2048
        // Morsel 1: start_row = 2048, num_rows = 2048
        // Morsel 2: start_row = 4096, num_rows = 904 (5000 - 4096)
        let state = dispatcher.state.lock().unwrap();
        let queue = state.work_queues.get(&10).unwrap();
        assert_eq!(queue.total_morsels, Some(3));
        assert_eq!(queue.numa_queues[0].len(), 3);

        let (m0, msg0) = &queue.numa_queues[0][0];
        assert_eq!(m0.start_row, 0);
        assert_eq!(m0.num_rows, 2048);
        let ScanMessage::Batch(b0) = msg0;
        assert_eq!(b0.num_rows(), 2048);

        let (m1, msg1) = &queue.numa_queues[0][1];
        assert_eq!(m1.start_row, 2048);
        assert_eq!(m1.num_rows, 2048);
        let ScanMessage::Batch(b1) = msg1;
        assert_eq!(b1.num_rows(), 2048);

        let (m2, msg2) = &queue.numa_queues[0][2];
        assert_eq!(m2.start_row, 4096);
        assert_eq!(m2.num_rows, 904);
        let ScanMessage::Batch(b2) = msg2;
        assert_eq!(b2.num_rows(), 904);
    }
}

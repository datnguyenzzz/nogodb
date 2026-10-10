use std::sync::Arc;

use anyhow::Result;

use crate::{
    arrow::SchemaRef,
    execution::{
        Morsel,
        dispatcher::Dispatcher,
        pipeline::{PipelineID, ScanMessage::CompressedPage},
    },
    planner::DynamicJoinPruner,
    storage::DataStorage,
};

pub const VECTOR_SIZE: usize = 2048;

pub struct ScanSource {
    pub table_name: String,
    pub schema: SchemaRef,
    pub storage: Arc<dyn DataStorage>,
    pub pruners: Vec<DynamicJoinPruner>,
    pub projections: Option<Vec<String>>,
}

impl ScanSource {
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        storage: Arc<dyn DataStorage>,
        pruners: Vec<DynamicJoinPruner>,
        projections: Option<Vec<String>>,
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
        let total_pages = self.storage.get_page_count(&self.table_name)?;
        let mut pushed_morsels = 0;
        let mut global_row_offset = 0;
        for page_id in 0..total_pages {
            let page = match self.storage.read_page(&self.table_name, page_id)? {
                Some(p) => p,
                None => continue,
            };

            // dynamic pruning
            let mut is_disjoint = false;
            for pruner in &self.pruners {
                if let Some(col_idx) = self.schema.fields().iter().position(|f| {
                    f.name == pruner.probe_col
                        || f.name.ends_with(&format!(".{}", pruner.probe_col))
                }) {
                    let (p_min, p_max) = match page.metadata.column_stats.get(&col_idx) {
                        Some(stats) => stats,
                        None => continue, // No min/max stats available for this column; skip pruning
                    };
                    if let Some((j_min, j_max)) =
                        dispatcher.get_join_bounds(pruner.join_id, col_idx)
                    {
                        if *p_max < j_min || *p_min > j_max {
                            is_disjoint = true;
                            break;
                        }
                    }
                }
            }

            if is_disjoint {
                global_row_offset += page.metadata.num_rows;
                continue;
            }

            let page_rows = page.metadata.num_rows;
            let numa_node = page_id % dispatcher.numa_nodes;

            let morsel = Morsel {
                start_row: global_row_offset,
                num_rows: page_rows,
                numa_node,
            };

            let msg = CompressedPage {
                data: page.data,
                meta: page.metadata.header,
            };
            dispatcher.push_scan_message(pid, numa_node, morsel, msg)?;
            pushed_morsels += 1;
            global_row_offset += page_rows;
        }

        dispatcher.finish_pushing(pid, pushed_morsels)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{Buffer, DataType, Field, Schema};
    use crate::storage::{MockDataStorage, PageMetaData, StoragePage};
    use std::collections::HashMap;

    #[test]
    fn test_scan_source_basic_execution() {
        let storage = Arc::new(MockDataStorage::new());

        // Create 2 mock pages of 100 rows each
        for _page_id in 0..2 {
            let metadata = PageMetaData {
                num_rows: 100,
                column_stats: HashMap::new(),
                header: Buffer::from(vec![1, 2, 3, 4]),
            };
            let page = StoragePage {
                metadata,
                data: Buffer::from(vec![10, 20, 30, 40]),
            };
            storage.add_page("orders", page);
        }

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

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
        let storage = Arc::new(MockDataStorage::new());

        // Page 0: id in [10, 20] (Disjoint from join range [150, 180])
        let mut stats0 = HashMap::new();
        stats0.insert(0, (10i64, 20i64));
        storage.add_page(
            "orders",
            StoragePage {
                metadata: PageMetaData {
                    num_rows: 100,
                    column_stats: stats0,
                    header: Buffer::from(vec![1]),
                },
                data: Buffer::from(vec![10]),
            },
        );

        // Page 1: id in [100, 200] (Overlaps join range [150, 180])
        let mut stats1 = HashMap::new();
        stats1.insert(0, (100i64, 200i64));
        storage.add_page(
            "orders",
            StoragePage {
                metadata: PageMetaData {
                    num_rows: 100,
                    column_stats: stats1,
                    header: Buffer::from(vec![2]),
                },
                data: Buffer::from(vec![20]),
            },
        );

        let schema = Arc::new(Schema::new(vec![Field {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]));

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
        dispatcher.publish_join_bounds(42, 0, 150, 180);

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
}

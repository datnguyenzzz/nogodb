use std::sync::Arc;

use anyhow::Result;

use crate::{
    db::Database,
    execution::plan_builder::builder::{PhysicalPlanGenerator, PlanBuilder},
    optimiser::Optimiser,
    planner::planner::Planner,
    sql_parser::Parser,
};

pub struct Client {
    db: Arc<Database>,
    parser: Parser,
    planner: Planner,
    optimiser: Optimiser,
    physical_generator: PhysicalPlanGenerator,
}

impl Client {
    pub fn new(db: Arc<Database>) -> Self {
        let catalog = db.engine.catalog.clone();
        let total_cores = db.scheduler.numa_nodes * db.scheduler.cores_per_node;
        let engine = db.engine.clone();
        Self {
            db,
            parser: Parser::default(),
            planner: Planner::new(catalog),
            optimiser: Optimiser::default(),
            physical_generator: PhysicalPlanGenerator::new(engine.storage.clone(), total_cores),
        }
    }

    pub async fn execute(&mut self, statement: &str) -> Result<()> {
        let statements = self.parser.parse_sql(statement)?;

        for statement in statements {
            let plan = self.planner.plan_statment(statement).await?;

            match plan {
                crate::planner::LogicalPlan::Insert {
                    table_name,
                    columns: _,
                    input,
                } => {
                    let coordinator = crate::execution::write_coordinator::WriteCoordinator::new(
                        self.db.engine.storage.clone(),
                    );

                    if let crate::planner::LogicalPlan::Values { schema, values } = *input {
                        let batch = crate::execution::write_coordinator::values_to_record_batch(
                            schema, &values,
                        )?;
                        coordinator.handle_insert(&table_name, batch).await?;
                    } else {
                        // INSERT INTO ... SELECT query
                        let optimised_plan = self.optimiser.optimise(*input)?;
                        let physical_node = self.physical_generator.create_plan(&optimised_plan)?;
                        let mut builder = PlanBuilder::new(
                            self.db.engine.storage.clone(),
                            self.db.scheduler.dispatcher.clone(),
                            self.db.scheduler.mailboxes.clone(),
                            self.db.scheduler.cores_per_node,
                        );
                        physical_node.build(&mut builder, None)?;
                        let results = self.db.scheduler.execute_job(builder.pipelines)?;
                        for batch in results {
                            coordinator.handle_insert(&table_name, batch).await?;
                        }
                    }
                }
                _ => {
                    let optimised_plan = self.optimiser.optimise(plan)?;
                    let physical_node = self.physical_generator.create_plan(&optimised_plan)?;
                    let mut builder = PlanBuilder::new(
                        self.db.engine.storage.clone(),
                        self.db.scheduler.dispatcher.clone(),
                        self.db.scheduler.mailboxes.clone(),
                        self.db.scheduler.cores_per_node,
                    );
                    physical_node.build(&mut builder, None)?;
                    self.db.scheduler.execute_job(builder.pipelines)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::DataType;
    use crate::db::StorageEngine;
    use crate::execution::scheduler::Scheduler;
    use crate::storage::embed::tests::{MockCatalog, MockDataStorage};
    use crate::storage::{ChunkMetaData, ColumnStat, DataStorage};

    #[tokio::test]
    async fn test_client_execute_insert_statement() {
        let storage = Arc::new(MockDataStorage::new());
        let catalog = Arc::new(MockCatalog::new());
        catalog.add_table(
            "users",
            vec![("id", DataType::Int32), ("name", DataType::Utf8)],
        );
        let scheduler = Arc::new(Scheduler::new());
        let engine = Arc::new(StorageEngine {
            catalog,
            storage: storage.clone(),
        });

        let db = Arc::new(Database {
            scheduler,
            engine: engine.clone(),
        });

        let mut client = Client::new(db);

        // Execute SQL INSERT!
        let sql = "INSERT INTO users (id, name) VALUES (42, 'Charlie');";
        client.execute(sql).await.unwrap();

        // Verify that the row was written into storage via WriteCoordinator::handle_insert!
        assert_eq!(storage.get_chunk_count("users").await.unwrap(), 1);
        let mut it = storage.new_iterator("users", &[]).await.unwrap();
        let meta_bytes = it.next_chunk_meta().await.unwrap().unwrap();
        let meta = ChunkMetaData::try_new(meta_bytes).unwrap();
        assert_eq!(meta.num_rows, 1);
        match meta.get_column_stat(0).unwrap() {
            ColumnStat::Int64 { min, max } => {
                assert_eq!(*min, 42i64);
                assert_eq!(*max, 42i64);
            }
            _ => panic!("Expected Int64 stat"),
        }
        match meta.get_column_stat(1).unwrap() {
            ColumnStat::Utf8 { min, max } => {
                assert_eq!(min.as_str(), "Charlie");
                assert_eq!(max.as_str(), "Charlif");
            }
            _ => panic!("Expected Utf8 stat"),
        }
    }
}

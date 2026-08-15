pub mod embedded;
pub mod external;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::{arrow::RecordBatch, catalog::TableMetadata};

/// Trait governing all Metadata Catalog operations (fetching, writing, dropping schemas).
#[async_trait]
pub trait CatalogStorage: Send + Sync {
    /// Retrieves table schema metadata from the storage catalog on-demand.
    async fn fetch_table_meta(&self, table_name: &str) -> Result<TableMetadata>;
    /// Registers a new table schema inside the storage catalog (for CREATE TABLE).
    async fn register_table_meta(&self, table_name: &str, metadata: TableMetadata) -> Result<()>;
    /// Removes a table schema from the storage catalog (for DROP TABLE).
    async fn drop_table_meta(&self, table_name: &str) -> Result<()>;
}

/// Trait governing all Columnar Data operations (scanning, writing, appending batches).
#[async_trait]
pub trait DataStorage: Send + Sync {
    /// Initiates a partitioned table scan, returning a streaming iterator of RecordBatches.
    /// Supports "Projection Pruning" directly inside the storage client to minimize I/O transfer.
    async fn scan_table(
        &self,
        table_name: &str,
        projection: Option<&[String]>,
        partition_id: usize,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>>;

    /// Writes/appends a stream of RecordBatches into a physical partition (for INSERT / DML).
    async fn write_table_partition(
        &self,
        table_name: &str,
        partition_id: usize,
        batches: Vec<RecordBatch>,
    ) -> Result<()>;
}

pub struct StorageEngine {
    pub catalog: Arc<dyn CatalogStorage>,
    pub storage: Arc<dyn DataStorage>,
}

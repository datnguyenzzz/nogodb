pub mod embedded;
pub mod external;

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use crate::{arrow::Buffer, catalog::TableMetadata};

/// Trait governing all Metadata Catalog operations (fetching, writing, dropping schemas).
#[async_trait]
pub trait CatalogStorage: Send + Sync {
    async fn fetch_table_meta(&self, table_name: &str) -> Result<TableMetadata>;
    async fn register_table_meta(&self, table_name: &str, metadata: TableMetadata) -> Result<()>;
    async fn drop_table_meta(&self, table_name: &str) -> Result<()>;
}

/// Trait governing all Columnar Data operations (scanning, writing, appending batches).
pub trait DataStorage: Send + Sync {
    fn read_page(&self, table_name: &str, page_id: usize) -> Result<Option<StoragePage>>;
    fn get_page_count(&self, table_name: &str) -> Result<usize>;
    fn write_page(&self, table_name: &str, page_id: usize, page: StoragePage) -> Result<()>;
}

pub struct StorageEngine {
    pub catalog: Arc<dyn CatalogStorage>,
    pub storage: Arc<dyn DataStorage>,
}

#[derive(Clone)]
pub struct PageMetaData {
    pub num_rows: usize,
    /// Column-wise min/max statistics for pruning: col_idx -> (min_i64, max_i64)
    pub column_stats: HashMap<usize, (i64, i64)>,
    pub header: Buffer,
}

#[derive(Clone)]
pub struct StoragePage {
    pub metadata: PageMetaData,
    /// Raw compressed columnar data bytes
    pub data: Buffer,
}

#[derive(Default)]
pub struct MockDataStorage {
    pub tables: std::sync::Mutex<HashMap<String, Vec<StoragePage>>>,
}

impl MockDataStorage {
    pub fn new() -> Self {
        Self {
            tables: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn add_page(&self, table_name: &str, page: StoragePage) {
        let mut guard = self.tables.lock().unwrap();
        guard.entry(table_name.to_string()).or_default().push(page);
    }
}

impl DataStorage for MockDataStorage {
    fn read_page(&self, table_name: &str, page_id: usize) -> Result<Option<StoragePage>> {
        let guard = self.tables.lock().unwrap();
        Ok(guard
            .get(table_name)
            .and_then(|pages| pages.get(page_id).cloned()))
    }

    fn get_page_count(&self, table_name: &str) -> Result<usize> {
        let guard = self.tables.lock().unwrap();
        Ok(guard.get(table_name).map(|pages| pages.len()).unwrap_or(0))
    }

    fn write_page(&self, table_name: &str, _page_id: usize, page: StoragePage) -> Result<()> {
        self.add_page(table_name, page);
        Ok(())
    }
}

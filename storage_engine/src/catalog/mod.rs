use crate::{CatalogStorage, TableMetadata};
use anyhow::Result;
use async_trait::async_trait;

pub struct Catalog {}

#[async_trait]
impl CatalogStorage for Catalog {
    async fn fetch_table_meta(&self, _table_name: &str) -> Result<TableMetadata> {
        todo!()
    }
    async fn register_table_meta(&self, _table_name: &str, _metadata: TableMetadata) -> Result<()> {
        todo!()
    }
    async fn drop_table_meta(&self, _table_name: &str) -> Result<()> {
        todo!()
    }
}

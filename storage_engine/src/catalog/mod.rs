use crate::{CatalogStorage, StorageError, TableMetadata};
use async_trait::async_trait;

pub struct Catalog {}

#[async_trait]
impl CatalogStorage for Catalog {
    async fn fetch_table_meta(&self, _table_name: &str) -> Result<TableMetadata, StorageError> {
        todo!()
    }
    async fn register_table_meta(
        &self,
        _table_name: &str,
        _metadata: TableMetadata,
    ) -> Result<(), StorageError> {
        todo!()
    }
    async fn drop_table_meta(&self, _table_name: &str) -> Result<(), StorageError> {
        todo!()
    }
}

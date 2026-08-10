use anyhow::Result;
use async_trait::async_trait;

use crate::arrow::DataType;

pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
}

// Learning: https://google.github.io/comprehensive-rust/concurrency/async-pitfalls/async-traits.html
#[async_trait]
pub trait CatalogClient {
    async fn fetch_table_meta(&self, name: &str) -> Result<TableMetadata>;
}

use anyhow::Result;
use arrow::datatypes::DataType;
use async_trait::async_trait;

pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<(String, DataType)>,
}

// Learning: https://google.github.io/comprehensive-rust/concurrency/async-pitfalls/async-traits.html
#[async_trait]
pub trait CatalogClient: Send + Sync {
    async fn fetch_table_meta(&self, name: &str) -> Result<TableMetadata>;
}

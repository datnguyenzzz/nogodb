use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::{CatalogClient, catalog::TableMetadata};

/// Concrete implementation of `catalog::CatalogClient` that communicates with the
/// object storage/metadata layer via gRPC.
pub struct GrpcCatalogClient {
    server_address: String,
}

impl GrpcCatalogClient {
    pub fn new(server_address: String) -> Self {
        GrpcCatalogClient { server_address }
    }
}

#[async_trait]
impl CatalogClient for GrpcCatalogClient {
    async fn fetch_table_meta(&self, name: &str) -> Result<TableMetadata> {
        todo!("implement me")
    }
}

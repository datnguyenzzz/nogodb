use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;

use crate::catalog::catalog::{CatalogClient, TableMetadata};

pub struct CatalogProvider {
    /// Remote API client to fetch metadata
    client: Arc<dyn CatalogClient>,
    /// Thread-safe in-memory cache to store metadata once fetched
    cache: RwLock<HashMap<String, TableMetadata>>,
}

impl CatalogProvider {
    pub fn new(client: Arc<dyn CatalogClient>) -> Self {
        CatalogProvider {
            client,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Primary entry point. Returns cached metadata or initiates an API call to load it.
    pub async fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata> {
        todo!("implement me!")
    }
}

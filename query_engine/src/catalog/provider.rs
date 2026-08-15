use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;

use crate::{catalog::TableMetadata, storage::CatalogStorage};

pub struct CatalogProvider {
    client: Arc<dyn CatalogStorage>,
    cache: RwLock<HashMap<String, TableMetadata>>,
}

impl CatalogProvider {
    pub fn new(client: Arc<dyn CatalogStorage>) -> Self {
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

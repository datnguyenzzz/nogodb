use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;

use crate::{catalog::TableMetadata, storage::CatalogStorage};

pub struct CatalogProvider {
    pub client: Arc<dyn CatalogStorage>,
    pub cache: RwLock<HashMap<String, TableMetadata>>,
}

impl CatalogProvider {
    pub fn new(client: Arc<dyn CatalogStorage>) -> Self {
        CatalogProvider {
            client,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata> {
        {
            let cache_read = self.cache.read().unwrap();
            if let Some(meta) = cache_read.get(table_name) {
                return Ok(meta.clone());
            }
        }
        let meta = self.client.fetch_table_meta(table_name).await?;
        {
            let mut cache_write = self.cache.write().unwrap();
            cache_write.insert(table_name.to_string(), meta.clone());
        }

        Ok(meta)
    }
}

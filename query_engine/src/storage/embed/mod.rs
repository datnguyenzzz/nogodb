use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use storage_engine::{
    CatalogStorage as EmbeddedCatalogStorage, DataStorage as EmbeddedDataStorage,
    StorageIterator as EmbeddedStorageIterator,
};

use crate::storage::{CatalogStorage, DataStorage, StorageIterator, TableMetadata};

pub struct WrappedStorageIterator {
    pub inner: Box<dyn EmbeddedStorageIterator>,
}

#[async_trait]
impl StorageIterator for WrappedStorageIterator {
    async fn next_chunk_meta(&mut self) -> Result<Option<Vec<u8>>> {
        self.inner.next_chunk_meta().await
    }

    fn skip_current_chunk(&mut self) -> Result<()> {
        self.inner.skip_current_chunk()
    }

    async fn read_current_chunk_data(&mut self) -> Result<Vec<u8>> {
        self.inner.read_current_chunk_data().await
    }
}

pub struct WrappedStorage {
    pub storage: Arc<dyn EmbeddedDataStorage>,
}

impl WrappedStorage {
    pub fn new(storage: Arc<dyn EmbeddedDataStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl DataStorage for WrappedStorage {
    async fn new_iterator(
        &self,
        table_name: &str,
        projection: &[usize],
    ) -> Result<Box<dyn StorageIterator>> {
        let it = self.storage.new_iterator(table_name, projection).await?;
        Ok(Box::new(WrappedStorageIterator { inner: it }))
    }

    async fn flush(&self, table_name: &str) -> Result<()> {
        self.storage.flush(table_name).await
    }

    async fn append_chunk(&self, table_name: &str, chunk: Vec<u8>) -> Result<()> {
        self.storage.append_chunk(table_name, chunk).await
    }

    async fn get_chunk_count(&self, table_name: &str) -> Result<usize> {
        self.storage.get_chunk_count(table_name).await
    }
}

pub struct WrappedCatalog {
    pub catalog: Arc<dyn EmbeddedCatalogStorage>,
}

impl WrappedCatalog {
    pub fn new(catalog: Arc<dyn EmbeddedCatalogStorage>) -> Self {
        Self { catalog }
    }
}

impl From<storage_engine::TableMetadata> for TableMetadata {
    fn from(m: storage_engine::TableMetadata) -> Self {
        TableMetadata {
            name: m.name,
            columns: m.columns,
        }
    }
}

impl From<TableMetadata> for storage_engine::TableMetadata {
    fn from(m: TableMetadata) -> Self {
        storage_engine::TableMetadata {
            name: m.name,
            columns: m.columns,
        }
    }
}

#[async_trait]
impl CatalogStorage for WrappedCatalog {
    async fn fetch_table_meta(&self, table_name: &str) -> Result<TableMetadata> {
        let meta = self.catalog.fetch_table_meta(table_name).await?;
        Ok(meta.into())
    }

    async fn register_table_meta(&self, table_name: &str, metadata: TableMetadata) -> Result<()> {
        self.catalog
            .register_table_meta(table_name, metadata.into())
            .await
    }

    async fn drop_table_meta(&self, table_name: &str) -> Result<()> {
        self.catalog.drop_table_meta(table_name).await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use rs_arrow::DataType;
    use std::{collections::HashMap, sync::Mutex};

    use crate::storage::{CatalogStorage, DataStorage, StorageIterator, TableMetadata};

    pub struct MockStorageIterator {
        pub chunks: Vec<Vec<u8>>,
        pub current_idx: usize,
    }

    #[async_trait]
    impl StorageIterator for MockStorageIterator {
        async fn next_chunk_meta(&mut self) -> Result<Option<Vec<u8>>> {
            if self.current_idx < self.chunks.len() {
                Ok(Some(self.chunks[self.current_idx].clone()))
            } else {
                Ok(None)
            }
        }

        fn skip_current_chunk(&mut self) -> Result<()> {
            if self.current_idx < self.chunks.len() {
                self.current_idx += 1;
            }
            Ok(())
        }

        async fn read_current_chunk_data(&mut self) -> Result<Vec<u8>> {
            if self.current_idx < self.chunks.len() {
                let data = self.chunks[self.current_idx].clone();
                self.current_idx += 1;
                Ok(data)
            } else {
                Err(anyhow::anyhow!("Cursor out of bounds"))
            }
        }
    }

    #[derive(Default)]
    pub struct MockDataStorage {
        pub tables: std::sync::Mutex<HashMap<String, Vec<Vec<u8>>>>,
    }

    impl MockDataStorage {
        pub fn new() -> Self {
            Self {
                tables: std::sync::Mutex::new(HashMap::new()),
            }
        }

        pub fn add_page(&self, table_name: &str, page: Vec<u8>) {
            let mut guard = self.tables.lock().unwrap();
            guard.entry(table_name.to_string()).or_default().push(page);
        }
    }

    #[async_trait]
    impl DataStorage for MockDataStorage {
        async fn append_chunk(&self, table_name: &str, chunk: Vec<u8>) -> Result<()> {
            self.add_page(table_name, chunk);
            Ok(())
        }

        async fn new_iterator(
            &self,
            table_name: &str,
            _projection: &[usize],
        ) -> Result<Box<dyn StorageIterator>> {
            let guard = self.tables.lock().unwrap();
            let chunks = guard.get(table_name).cloned().unwrap_or_default();
            Ok(Box::new(MockStorageIterator {
                chunks,
                current_idx: 0,
            }))
        }

        async fn flush(&self, _table_name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_chunk_count(&self, table_name: &str) -> Result<usize> {
            let guard = self.tables.lock().unwrap();
            Ok(guard.get(table_name).map(|pages| pages.len()).unwrap_or(0))
        }
    }

    pub struct MockCatalog {
        tables: Mutex<HashMap<String, TableMetadata>>,
    }

    impl MockCatalog {
        pub fn new() -> Self {
            Self {
                tables: Mutex::new(HashMap::new()),
            }
        }

        pub fn add_table(&self, table_name: &str, columns: Vec<(&str, DataType)>) {
            let cols = columns
                .into_iter()
                .map(|(n, dt)| (n.to_string(), dt))
                .collect();
            let meta = TableMetadata {
                name: table_name.to_string(),
                columns: cols,
            };
            self.tables
                .lock()
                .unwrap()
                .insert(table_name.to_string(), meta);
        }
    }

    #[async_trait]
    impl CatalogStorage for MockCatalog {
        async fn fetch_table_meta(&self, table_name: &str) -> Result<TableMetadata> {
            self.tables
                .lock()
                .unwrap()
                .get(table_name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", table_name))
        }

        async fn register_table_meta(
            &self,
            _table_name: &str,
            _metadata: TableMetadata,
        ) -> Result<()> {
            Ok(())
        }

        async fn drop_table_meta(&self, _table_name: &str) -> Result<()> {
            Ok(())
        }
    }

    struct MockEmbeddedDataStorage {
        chunks: Mutex<Vec<Vec<u8>>>,
    }

    struct MockEmbeddedStorageIterator {
        chunks: Vec<Vec<u8>>,
        cursor: usize,
    }

    #[async_trait]
    impl storage_engine::StorageIterator for MockEmbeddedStorageIterator {
        async fn next_chunk_meta(&mut self) -> Result<Option<Vec<u8>>> {
            if self.cursor < self.chunks.len() {
                Ok(Some(vec![1, 2, 3]))
            } else {
                Ok(None)
            }
        }

        fn skip_current_chunk(&mut self) -> Result<()> {
            self.cursor += 1;
            Ok(())
        }

        async fn read_current_chunk_data(&mut self) -> Result<Vec<u8>> {
            let data = self.chunks[self.cursor].clone();
            self.cursor += 1;
            Ok(data)
        }
    }

    #[async_trait]
    impl storage_engine::DataStorage for MockEmbeddedDataStorage {
        async fn new_iterator(
            &self,
            _table_name: &str,
            _projection: &[usize],
        ) -> Result<Box<dyn storage_engine::StorageIterator>> {
            let chunks = self.chunks.lock().unwrap().clone();
            Ok(Box::new(MockEmbeddedStorageIterator { chunks, cursor: 0 }))
        }

        async fn flush(&self, _table_name: &str) -> Result<()> {
            Ok(())
        }

        async fn append_chunk(&self, _table_name: &str, chunk: Vec<u8>) -> Result<()> {
            self.chunks.lock().unwrap().push(chunk);
            Ok(())
        }

        async fn get_chunk_count(&self, _table_name: &str) -> Result<usize> {
            Ok(self.chunks.lock().unwrap().len())
        }
    }

    struct MockEmbeddedCatalog {
        meta: Mutex<Option<storage_engine::TableMetadata>>,
    }

    #[async_trait]
    impl storage_engine::CatalogStorage for MockEmbeddedCatalog {
        async fn fetch_table_meta(
            &self,
            _table_name: &str,
        ) -> Result<storage_engine::TableMetadata> {
            self.meta
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Not found"))
        }

        async fn register_table_meta(
            &self,
            _table_name: &str,
            metadata: storage_engine::TableMetadata,
        ) -> Result<()> {
            *self.meta.lock().unwrap() = Some(metadata);
            Ok(())
        }

        async fn drop_table_meta(&self, _table_name: &str) -> Result<()> {
            *self.meta.lock().unwrap() = None;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_wrapped_storage_and_catalog_delegation() {
        let embedded_storage = std::sync::Arc::new(MockEmbeddedDataStorage {
            chunks: Mutex::new(Vec::new()),
        });
        let wrapped_storage = super::WrappedStorage::new(embedded_storage);

        wrapped_storage
            .append_chunk("test", vec![10, 20])
            .await
            .unwrap();
        wrapped_storage.flush("test").await.unwrap();
        assert_eq!(wrapped_storage.get_chunk_count("test").await.unwrap(), 1);

        let mut it = wrapped_storage.new_iterator("test", &[0, 0]).await.unwrap();
        let meta = it.next_chunk_meta().await.unwrap();
        assert_eq!(meta, Some(vec![1, 2, 3]));
        let data = it.read_current_chunk_data().await.unwrap();
        assert_eq!(data, vec![10, 20]);
        assert_eq!(it.next_chunk_meta().await.unwrap(), None);

        let embedded_catalog = std::sync::Arc::new(MockEmbeddedCatalog {
            meta: Mutex::new(None),
        });
        let wrapped_catalog = super::WrappedCatalog::new(embedded_catalog);

        let meta = TableMetadata {
            name: "test".to_string(),
            columns: vec![("a".to_string(), DataType::Int32)],
        };
        wrapped_catalog
            .register_table_meta("test", meta.clone())
            .await
            .unwrap();
        let fetched = wrapped_catalog.fetch_table_meta("test").await.unwrap();
        assert_eq!(fetched, meta);
        wrapped_catalog.drop_table_meta("test").await.unwrap();
        assert!(wrapped_catalog.fetch_table_meta("test").await.is_err());
    }
}

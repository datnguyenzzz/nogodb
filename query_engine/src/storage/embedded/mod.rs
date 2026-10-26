#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use async_trait::async_trait;

    use crate::storage::{DataStorage, StorageIterator};

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

        async fn new_iterator(&self, table_name: &str) -> Result<Box<dyn StorageIterator>> {
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
}

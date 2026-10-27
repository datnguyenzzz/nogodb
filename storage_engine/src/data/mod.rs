use anyhow::Result;
use async_trait::async_trait;

use crate::{DataStorage, StorageIterator};

pub struct Data {}

#[async_trait]
impl DataStorage for Data {
    async fn new_iterator(
        &self,
        _table_name: &str,
        _projection: &[usize],
    ) -> Result<Box<dyn StorageIterator>> {
        todo!()
    }

    async fn flush(&self, _table_name: &str) -> Result<()> {
        todo!()
    }

    async fn append_chunk(&self, _table_name: &str, _chunk: Vec<u8>) -> Result<()> {
        todo!()
    }

    async fn get_chunk_count(&self, _table_name: &str) -> Result<usize> {
        todo!()
    }
}

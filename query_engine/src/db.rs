use std::sync::Arc;

use crate::{
    execution::scheduler::Scheduler,
    storage::{CatalogStorage, DataStorage, WrappedCatalog, WrappedStorage},
};

pub struct Database {
    pub scheduler: Arc<Scheduler>,
    pub engine: Arc<StorageEngine>,
}

pub struct StorageEngine {
    pub catalog: Arc<dyn CatalogStorage>,
    pub storage: Arc<dyn DataStorage>,
}

impl Database {
    pub fn new() -> Self {
        // Default to use an embedded storage engine instead of
        // a standalone storage engine
        let scheduler = Scheduler::new();
        let engine = StorageEngine {
            catalog: Arc::new(WrappedCatalog {
                catalog: Arc::new(storage_engine::Catalog {}),
            }),
            storage: Arc::new(WrappedStorage {
                storage: Arc::new(storage_engine::Data {}),
            }),
        };
        Self {
            scheduler: Arc::new(scheduler),
            engine: Arc::new(engine),
        }
    }
}
